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
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

const memberReadyCheckTimeout = 5 * time.Second

type targetPDVersion struct {
	Version string `json:"version"`
}

// CheckMemberReadyForLeaderTransfer checks whether the target PD member can be promoted to leader.
func (s *Server) CheckMemberReadyForLeaderTransfer(ctx context.Context, memberID uint64) error {
	clientURLs, err := s.getMemberClientURLs(memberID)
	if err != nil {
		return err
	}
	if len(clientURLs) == 0 {
		return errors.Errorf("target pd member %d has no client url", memberID)
	}

	var lastErr error
	for _, clientURL := range clientURLs {
		if err := s.checkMemberReadyURL(ctx, memberID, clientURL); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return errors.Errorf("target pd member %d is not ready for leader transfer, tried urls %v, last error: %v", memberID, clientURLs, lastErr)
}

func (s *Server) getMemberClientURLs(memberID uint64) ([]string, error) {
	members, err := s.GetMembers()
	if err != nil {
		return nil, err
	}
	if clientURLs := findMemberClientURLs(members, memberID); len(clientURLs) > 0 {
		return clientURLs, nil
	}
	members, err = s.ReloadMembers()
	if err != nil {
		return nil, err
	}
	clientURLs := findMemberClientURLs(members, memberID)
	if len(clientURLs) == 0 {
		return nil, errors.Errorf("target pd member %d not found", memberID)
	}
	return clientURLs, nil
}

func findMemberClientURLs(members []*pdpb.Member, memberID uint64) []string {
	for _, member := range members {
		if member.GetMemberId() == memberID {
			return member.GetClientUrls()
		}
	}
	return nil
}

func (s *Server) checkMemberReadyURL(ctx context.Context, memberID uint64, clientURL string) error {
	checkCtx, cancel := context.WithTimeout(ctx, memberReadyCheckTimeout)
	defer cancel()

	version, err := s.getTargetPDVersion(checkCtx, clientURL)
	if err != nil {
		return errors.Annotatef(err, "failed to get target pd member %d version from %s", memberID, clientURL)
	}
	pdVersion, err := versioninfo.ParseVersion(version)
	if err != nil {
		return errors.Annotatef(err, "failed to parse target pd member %d version %q from %s", memberID, version, clientURL)
	}
	if !versioninfo.IsReadyAPISupported(pdVersion) {
		return nil
	}
	if err := s.checkTargetPDReady(checkCtx, clientURL); err != nil {
		return errors.Annotatef(err, "target pd member %d is not ready at %s", memberID, clientURL)
	}
	return nil
}

func (s *Server) getTargetPDVersion(ctx context.Context, clientURL string) (string, error) {
	body, err := s.getTargetPD(ctx, clientURL, apiutil.CorePath+"/version")
	if err != nil {
		return "", err
	}
	version := &targetPDVersion{}
	if err := json.Unmarshal(body, version); err != nil {
		return "", errors.WithStack(err)
	}
	return version.Version, nil
}

func (s *Server) checkTargetPDReady(ctx context.Context, clientURL string) error {
	_, err := s.getTargetPD(ctx, clientURL, apiutil.CoreV2Path+"/ready")
	return err
}

func (s *Server) getTargetPD(ctx context.Context, clientURL, path string) ([]byte, error) {
	httpClient := s.GetHTTPClient()
	if httpClient == nil {
		return nil, errors.New("pd http client is nil")
	}
	url := strings.TrimRight(clientURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status %d from %s: %s", resp.StatusCode, url, string(body))
	}
	return body, nil
}
