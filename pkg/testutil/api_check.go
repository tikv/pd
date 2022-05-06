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

// Status is used to check whether http response code is equal given code
func Status(c *check.C, code int) func([]byte, int) {
	return func(_ []byte, i int) {
		c.Assert(i, check.Equals, code)
	}
}

// StatusOK is used to check whether http response code is equal http.StatusOK
func StatusOK(c *check.C) func([]byte, int) {
	return Status(c, http.StatusOK)
}

// StatusNotOK is used to check whether http response code is not equal http.StatusOK
func StatusNotOK(c *check.C) func([]byte, int) {
	return func(_ []byte, i int) {
		c.Assert(i == http.StatusOK, check.IsFalse)
	}
}

// ExtractJSON is used to check whether given data can be extracted successfully
func ExtractJSON(c *check.C, data interface{}) func([]byte, int) {
	return func(res []byte, _ int) {
		err := json.Unmarshal(res, data)
		c.Assert(err, check.IsNil)
	}
}

// StringContain is used to check whether response context contains given string
func StringContain(c *check.C, sub string) func([]byte, int) {
	return func(res []byte, _ int) {
		c.Assert(strings.Contains(string(res), sub), check.IsTrue)
	}
}

// StringEqual is used to check whether response context equal given string
func StringEqual(c *check.C, str string) func([]byte, int) {
	return func(res []byte, _ int) {
		c.Assert(strings.Contains(string(res), str), check.IsTrue)
	}
}

// ReadGetJSON is used to do get request and check whether given data can be extracted successfully
func ReadGetJSON(c *check.C, client *http.Client, url string, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	return checkResp(resp, StatusOK(c), ExtractJSON(c, data))
}

// ReadGetJSONWithBody is used to do get request with input and check whether given data can be extracted successfully
func ReadGetJSONWithBody(c *check.C, client *http.Client, url string, input []byte, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, input)
	if err != nil {
		return err
	}
	return checkResp(resp, StatusOK(c), ExtractJSON(c, data))
}

// CheckPostJSON is used to do post request and do check options
func CheckPostJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := apiutil.PostJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckGetJSON is used to do get request and do check options
func CheckGetJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := apiutil.GetJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckPatchJSON is used to do patch request and do check options
func CheckPatchJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
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
