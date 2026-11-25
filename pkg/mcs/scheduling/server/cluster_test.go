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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

func TestUpdateBalanceRangeJobStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int
		status string
	}{
		{name: "success", code: http.StatusOK, status: "running"},
		{name: "unsupported", code: http.StatusNotFound, status: "finished"},
		{name: "conflict", code: http.StatusConflict, status: "finished"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			type observation struct {
				method string
				path   string
				header string
				body   map[string]any
				err    error
			}
			requests := make(chan observation, 1)
			pd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed := observation{
					method: r.Method,
					path:   r.URL.Path,
					header: r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader),
				}
				observed.err = json.NewDecoder(r.Body).Decode(&observed.body)
				requests <- observed
				if tc.code == http.StatusNotFound {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(tc.code)
			}))
			defer pd.Close()
			c := &Cluster{ctx: t.Context(), httpClient: pd.Client(), backendAddress: pd.URL}
			create := time.Date(2026, time.November, 11, 1, 2, 3, 123456789, time.UTC)
			err := c.UpdateBalanceRangeJobStatus(1, create, tc.status)
			switch tc.code {
			case http.StatusOK:
				re.NoError(err)
			case http.StatusNotFound:
				re.Equal(schedulers.ErrBalanceRangeJobStatusUnsupported, errors.Cause(err))
			default:
				re.Error(err)
				re.NotEqual(schedulers.ErrBalanceRangeJobStatusUnsupported, errors.Cause(err))
			}
			select {
			case observed := <-requests:
				re.NoError(observed.err)
				re.Equal(http.MethodPut, observed.method)
				re.Equal("/pd/api/v1/scheduler-config/balance-range-scheduler/job/status", observed.path)
				re.Equal("true", observed.header)
				re.Equal(map[string]any{"job-id": float64(1), "create": create.Format(time.RFC3339Nano), "status": tc.status}, observed.body)
			case <-time.After(requestTimeout):
				t.Fatal("PD did not receive the status request")
			}
		})
	}
}

func TestBalanceRangeJobStatusBackendRotation(t *testing.T) {
	re := require.New(t)
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()
	c := &Cluster{ctx: t.Context(), httpClient: healthy.Client(), backendAddress: strings.Join([]string{failed.URL, healthy.URL}, ",")}
	created := time.Now()
	re.Error(c.UpdateBalanceRangeJobStatus(1, created, "running"))
	re.NoError(c.UpdateBalanceRangeJobStatus(1, created, "running"))
	// A failed transport also rotates on the next call, rather than sticking to
	// a member that is no longer serving HTTP.
	failed.Close()
	re.Error(c.UpdateBalanceRangeJobStatus(1, created, "running"))
	re.NoError(c.UpdateBalanceRangeJobStatus(1, created, "running"))
}
