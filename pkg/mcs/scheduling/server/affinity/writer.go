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

package affinity

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

const (
	// groupTargetsPath is the PD API that writes group targets.
	groupTargetsPath = "/pd/api/v2/affinity-groups/targets"
	writeTimeout     = 3 * time.Second
)

// TargetWriter sends the targets decided by the scheduling service to PD, which
// stores them like every other affinity write. The watcher then delivers the
// stored groups back to the scheduling service.
type TargetWriter struct {
	httpClient *http.Client
	endpoints  []string
	timeout    time.Duration
	// first is the index of the endpoint that handled the last request. It is
	// tried first, so that an unreachable endpoint does not delay every write.
	first atomic.Int64
}

var _ affinity.TargetWriter = (*TargetWriter)(nil)

// NewTargetWriter creates a TargetWriter that sends requests to the PD endpoints,
// given as a comma-separated list.
func NewTargetWriter(httpClient *http.Client, backendEndpoints string) *TargetWriter {
	var endpoints []string
	for endpoint := range strings.SplitSeq(backendEndpoints, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			endpoints = append(endpoints, strings.TrimSuffix(endpoint, "/"))
		}
	}
	return &TargetWriter{httpClient: httpClient, endpoints: endpoints, timeout: writeTimeout}
}

// WriteGroupTargets implements affinity.TargetWriter. It tries the PD endpoints
// in turn, starting from the one that handled the last request, until one of
// them handles this request. Retrying on another endpoint is safe because PD
// writes a target only if its group is unchanged.
func (w *TargetWriter) WriteGroupTargets(ctx context.Context, targets []affinity.GroupTarget) ([]string, error) {
	body, err := json.Marshal(&affinity.GroupTargetsRequest{Targets: targets})
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = errors.New("no PD endpoint")
	first := int(w.first.Load())
	for i := range w.endpoints {
		index := (first + i) % len(w.endpoints)
		var written []string
		var retry bool
		written, retry, err = w.write(ctx, w.endpoints[index], body)
		if err == nil || !retry {
			w.first.Store(int64(index))
			return written, err
		}
	}
	return nil, err
}

// write sends one request and reports whether another endpoint may succeed.
// A failure to reach the endpoint, including a timeout, may succeed on another
// endpoint unless ctx is done.
func (w *TargetWriter) write(ctx context.Context, endpoint string, body []byte) (written []string, retry bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	resp, err := apiutil.PostJSONWithContext(reqCtx, w.httpClient, endpoint+groupTargetsPath, body)
	if err != nil {
		return nil, ctx.Err() == nil, errors.Trace(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ctx.Err() == nil, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		err := errors.Errorf("write affinity group targets to %s: %s %s", endpoint, resp.Status, bytes.TrimSpace(data))
		return nil, resp.StatusCode >= http.StatusInternalServerError, err
	}
	var result affinity.GroupTargetsResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, false, errors.Annotate(err, "decode affinity group targets response")
	}
	return result.WrittenGroupIDs, false, nil
}
