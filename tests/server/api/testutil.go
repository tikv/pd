// Copyright 2023 TiKV Project Authors.
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

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/tests"
)

const (
	schedulersPrefix      = "/pd/api/v1/schedulers"
	schedulerConfigPrefix = "/pd/api/v1/scheduler-config"
)

// MustAddScheduler adds a scheduler with HTTP API.
func MustAddScheduler(
	re *require.Assertions, serverAddr string,
	schedulerName string, args map[string]any,
) {
	request := map[string]any{
		"name": schedulerName,
	}
	for arg, val := range args {
		request[arg] = val
	}
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s%s", serverAddr, schedulersPrefix), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustDeleteScheduler deletes a scheduler with HTTP API.
func MustDeleteScheduler(re *require.Assertions, serverAddr, schedulerName string) {
	httpReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s%s/%s", serverAddr, schedulersPrefix, schedulerName), http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustCallSchedulerConfigAPI calls a scheduler config with HTTP API with the given args.
func MustCallSchedulerConfigAPI(
	re *require.Assertions,
	method, serverAddr, schedulerName string, args []string,
	input map[string]any,
) {
	data, err := json.Marshal(input)
	re.NoError(err)
	args = append([]string{schedulerConfigPrefix, schedulerName}, args...)
	httpReq, err := http.NewRequest(method, fmt.Sprintf("%s%s", serverAddr, path.Join(args...)), bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}
