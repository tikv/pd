// Copyright 2022 TiKV Project Authors.
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

package testutil

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/check"
	"github.com/tikv/pd/pkg/apiutil"
)

type APICheckerUtil struct {
	c *check.C
}

func NewAPICheckerUtil(c *check.C) *APICheckerUtil {
	return &APICheckerUtil{c: c}
}

func (ut *APICheckerUtil) Status(code int) func([]byte, int) {
	return func(_ []byte, i int) {
		ut.c.Assert(i, check.Equals, code)
	}
}

func (ut *APICheckerUtil) StatusOK() func([]byte, int) {
	return ut.Status(http.StatusOK)
}

func (ut *APICheckerUtil) StatusNotOK() func([]byte, int) {
	return func(_ []byte, i int) {
		ut.c.Assert(i == http.StatusOK, check.IsFalse)
	}
}

func (ut *APICheckerUtil) ExtractJSON(data interface{}) func([]byte, int) {
	return func(res []byte, _ int) {
		err := json.Unmarshal(res, data)
		ut.c.Assert(err, check.IsNil)
	}
}

func (ut *APICheckerUtil) StringContain(sub string) func([]byte, int) {
	return func(res []byte, _ int) {
		ut.c.Assert(strings.Contains(string(res), sub), check.IsTrue)
	}
}

func (ut *APICheckerUtil) StringEqual(sub string) func([]byte, int) {
	return func(res []byte, _ int) {
		ut.c.Assert(strings.Contains(string(res), sub), check.IsTrue)
	}
}

func (ut *APICheckerUtil) ReadGetJSON(client *http.Client, url string, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	return checkResp(resp, ut.StatusOK(), ut.ExtractJSON(data))
}

func (ut *APICheckerUtil) ReadGetJSONWithBody(client *http.Client, url string, input []byte, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, input)
	if err != nil {
		return err
	}
	return checkResp(resp, ut.StatusOK(), ut.ExtractJSON(data))
}

func (ut *APICheckerUtil) CheckPostJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := apiutil.PostJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func (ut *APICheckerUtil) CheckGetJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := apiutil.GetJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func (ut *APICheckerUtil) CheckPatchJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := apiutil.PatchJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func checkResp(resp *http.Response, checkOpts ...func([]byte, int)) error {
	res, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	for _, opt := range checkOpts {
		opt(res, resp.StatusCode)
	}
	return nil
}
