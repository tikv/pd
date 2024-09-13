// Copyright 2024 TiKV Project Authors.
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

	"github.com/pingcap/check"
)

const schedulersPrefix = "/pd/api/v1/schedulers"

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

// MustAddScheduler adds a scheduler with HTTP API.
func MustAddScheduler(
	c *check.C, serverAddr string,
	schedulerName string, args map[string]interface{},
) {
	request := map[string]interface{}{
		"name": schedulerName,
	}
	for arg, val := range args {
		request[arg] = val
	}
	data, err := json.Marshal(request)
	c.Assert(err, check.IsNil)

	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s%s", serverAddr, schedulersPrefix), bytes.NewBuffer(data))
	c.Assert(err, check.IsNil)
	// Send request.
	resp, err := dialClient.Do(httpReq)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	c.Assert(resp.StatusCode, check.Equals, http.StatusOK)
}
