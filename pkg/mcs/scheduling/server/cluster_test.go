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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

func TestUpdateBalanceRangeJobStatus(t *testing.T) {
	re := require.New(t)
	code := http.StatusOK
	var body map[string]any
	pd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		re.Equal(http.MethodPut, r.Method)
		re.Equal("/pd/api/v1/scheduler-config/balance-range-scheduler/job/status", r.URL.Path)
		re.Equal("true", r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader))
		re.NoError(json.NewDecoder(r.Body).Decode(&body))
		if code == http.StatusNotFound {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(code)
	}))
	defer pd.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Cluster{ctx: ctx, httpClient: pd.Client(), backendAddress: pd.URL}

	re.NoError(c.UpdateBalanceRangeJobStatus(1, "running"))
	re.Equal(map[string]any{"job-id": float64(1), "status": "running"}, body)

	// the PD hasn't registered the API, e.g. it hasn't been upgraded yet
	code = http.StatusNotFound
	err := c.UpdateBalanceRangeJobStatus(1, "finished")
	re.Equal(schedulers.ErrBalanceRangeJobStatusUnsupported, errors.Cause(err))

	code = http.StatusConflict
	err = c.UpdateBalanceRangeJobStatus(1, "finished")
	re.Error(err)
	re.NotEqual(schedulers.ErrBalanceRangeJobStatusUnsupported, errors.Cause(err))
}
