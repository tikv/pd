// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

const (
	memberReadyCheckTotalTimeout   = 5 * time.Second
	memberReadyCheckAttemptTimeout = time.Second
)

type targetPDVersion struct {
	Version string `json:"version"`
}

// CheckMemberReadyForLeaderTransfer checks whether the target PD member can be promoted to leader.
func (s *Server) CheckMemberReadyForLeaderTransfer(ctx context.Context, memberID uint64) error {
	failpoint.InjectCall("checkMemberReadyForLeaderTransfer", memberID)

	checkCtx, cancel := context.WithTimeout(ctx, memberReadyCheckTotalTimeout)
	defer cancel()

	clientURLs, err := s.getMemberClientURLs(checkCtx, memberID)
	if err != nil {
		return err
	}

	triedURLs, ready, lastErr := s.checkMemberReadyURLs(checkCtx, memberID, clientURLs)
	if ready {
		return nil
	}

	reloadedClientURLs, err := s.getMemberClientURLs(checkCtx, memberID)
	if err != nil {
		lastErr = errors.Annotatef(err, "failed to reload target pd member %d client urls", memberID)
	} else if untriedClientURLs := excludeClientURLs(reloadedClientURLs, triedURLs); len(untriedClientURLs) > 0 {
		var reloadedTriedURLs []string
		reloadedTriedURLs, ready, lastErr = s.checkMemberReadyURLs(checkCtx, memberID, untriedClientURLs)
		triedURLs = append(triedURLs, reloadedTriedURLs...)
		if ready {
			return nil
		}
	}
	return errors.Errorf("target pd member %d is not ready for leader transfer, tried urls %v, last error: %v", memberID, triedURLs, lastErr)
}

func (s *Server) checkMemberReadyURLs(ctx context.Context, memberID uint64, clientURLs []string) ([]string, bool, error) {
	triedURLs := make([]string, 0, len(clientURLs))
	var lastErr error
	for _, clientURL := range clientURLs {
		triedURLs = append(triedURLs, clientURL)
		if err := s.checkMemberReadyURL(ctx, memberID, clientURL); err != nil {
			lastErr = err
			continue
		}
		return triedURLs, true, nil
	}
	return triedURLs, false, lastErr
}

func excludeClientURLs(clientURLs, excludedURLs []string) []string {
	excluded := make(map[string]struct{}, len(excludedURLs))
	for _, clientURL := range excludedURLs {
		excluded[clientURL] = struct{}{}
	}
	untried := make([]string, 0, len(clientURLs))
	for _, clientURL := range clientURLs {
		if _, ok := excluded[clientURL]; !ok {
			untried = append(untried, clientURL)
			excluded[clientURL] = struct{}{}
		}
	}
	return untried
}

func (s *Server) getMemberClientURLs(ctx context.Context, memberID uint64) ([]string, error) {
	client := s.GetClient()
	if client == nil {
		return nil, errs.ErrEtcdNotStarted
	}
	// Bypass the member cache so discovery shares the readiness deadline and
	// observes client URL updates immediately.
	members, err := etcdutil.ListEtcdMembers(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, member := range members.Members {
		if member.GetID() == memberID {
			if len(member.ClientURLs) == 0 {
				return nil, errors.Errorf("target pd member %d has no client url", memberID)
			}
			return member.ClientURLs, nil
		}
	}
	return nil, errors.Errorf("target pd member %d not found", memberID)
}

func (s *Server) checkMemberReadyURL(ctx context.Context, memberID uint64, clientURL string) error {
	checkCtx, cancel := context.WithTimeout(ctx, memberReadyCheckAttemptTimeout)
	defer cancel()

	version, err := s.getTargetPDVersion(checkCtx, clientURL)
	if err != nil {
		return errors.Annotatef(err, "failed to get target pd member %d version from %s", memberID, clientURL)
	}
	pdVersion, versionErr := versioninfo.ParseVersion(version)
	// An unparsable version, such as "None", does not prove whether /ready is
	// supported. Probe /ready so local and dev builds are still gated on their
	// actual readiness. Only a parseable release older than ReadyAPI skips the
	// probe based on its version.
	if versionErr == nil && !versioninfo.IsReadyAPISupported(pdVersion) {
		return nil
	}
	statusCode, err := s.checkTargetPDReady(checkCtx, clientURL)
	// An unparsable version may come from an old custom build that does not
	// expose /ready. A locally handled 404 is therefore compatible, while any
	// other failure still means that the target cannot be confirmed ready.
	if versionErr != nil && statusCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return errors.Annotatef(err, "target pd member %d is not ready at %s", memberID, clientURL)
	}
	return nil
}

func (s *Server) getTargetPDVersion(ctx context.Context, clientURL string) (string, error) {
	body, _, err := s.getTargetPD(ctx, clientURL, apiutil.CorePath+"/version")
	if err != nil {
		return "", err
	}
	version := &targetPDVersion{}
	if err := json.Unmarshal(body, version); err != nil {
		return "", errors.WithStack(err)
	}
	return version.Version, nil
}

func (s *Server) checkTargetPDReady(ctx context.Context, clientURL string) (int, error) {
	_, statusCode, err := s.getTargetPD(ctx, clientURL, apiutil.CoreV2Path+"/ready")
	return statusCode, err
}

func (s *Server) getTargetPD(ctx context.Context, clientURL, path string) ([]byte, int, error) {
	httpClient := s.GetHTTPClient()
	if httpClient == nil {
		return nil, 0, errors.New("pd http client is nil")
	}
	url := strings.TrimRight(clientURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	req.Header.Set(apiutil.PDAllowFollowerHandleHeader, "true")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, errors.WithStack(err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, errors.Errorf("unexpected status %d from %s: %s", resp.StatusCode, url, string(body))
	}
	return body, resp.StatusCode, nil
}
