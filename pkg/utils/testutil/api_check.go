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

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

// Status is used to check whether http response code is equal given code
func Status(re *require.Assertions, code int) func([]byte, int) {
	return func(_ []byte, i int) {
		re.Equal(code, i)
	}
}

// StatusOK is used to check whether http response code is equal http.StatusOK
func StatusOK(re *require.Assertions) func([]byte, int) {
	return Status(re, http.StatusOK)
}

// StatusNotOK is used to check whether http response code is not equal http.StatusOK
func StatusNotOK(re *require.Assertions) func([]byte, int) {
	return func(_ []byte, i int) {
		re.NotEqual(http.StatusOK, i)
	}
}

// ExtractJSON is used to check whether given data can be extracted successfully
func ExtractJSON(re *require.Assertions, data interface{}) func([]byte, int) {
	return func(res []byte, _ int) {
		re.NoError(json.Unmarshal(res, data))
	}
}

// StringContain is used to check whether response context contains given string
func StringContain(re *require.Assertions, sub string) func([]byte, int) {
	return func(res []byte, _ int) {
		re.Contains(string(res), sub)
	}
}

// StringEqual is used to check whether response context equal given string
func StringEqual(re *require.Assertions, str string) func([]byte, int) {
	return func(res []byte, _ int) {
		re.Contains(string(res), str)
	}
}

// ReadGetJSON is used to do get request and check whether given data can be extracted successfully
func ReadGetJSON(re *require.Assertions, client *http.Client, url string, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	return checkResp(resp, StatusOK(re), ExtractJSON(re, data))
}

// ReadGetJSONWithBody is used to do get request with input and check whether given data can be extracted successfully
func ReadGetJSONWithBody(re *require.Assertions, client *http.Client, url string, input []byte, data interface{}) error {
	resp, err := apiutil.GetJSON(client, url, input)
	if err != nil {
		return err
	}
	return checkResp(resp, StatusOK(re), ExtractJSON(re, data))
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
