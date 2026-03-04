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
	"fmt"
	"io"
	"net/http"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

// Status is used to check whether http response code is equal given code.
func Status(re *require.Assertions, code int) func([]byte, int, http.Header) {
	return func(resp []byte, i int, _ http.Header) {
		re.Equal(code, i, "resp: "+string(resp))
	}
}

// StatusOK is used to check whether http response code is equal http.StatusOK.
func StatusOK(re *require.Assertions) func([]byte, int, http.Header) {
	return Status(re, http.StatusOK)
}

// StatusNotOK is used to check whether http response code is not equal http.StatusOK.
func StatusNotOK(re *require.Assertions) func([]byte, int, http.Header) {
	return func(resp []byte, i int, _ http.Header) {
		re.NotEqual(http.StatusOK, i, "resp: "+string(resp))
	}
}

// ExtractJSON is used to check whether given data can be extracted successfully.
func ExtractJSON(re *require.Assertions, data any) func([]byte, int, http.Header) {
	return func(resp []byte, _ int, _ http.Header) {
		re.NoError(json.Unmarshal(resp, data), "resp: "+string(resp))
	}
}

// StringContain is used to check whether response context contains given string.
func StringContain(re *require.Assertions, sub string) func([]byte, int, http.Header) {
	return func(resp []byte, _ int, _ http.Header) {
		re.Contains(string(resp), sub, "resp: "+string(resp))
	}
}

// StringNotContain is used to check whether response context doesn't contain given string.
func StringNotContain(re *require.Assertions, sub string) func([]byte, int, http.Header) {
	return func(resp []byte, _ int, _ http.Header) {
		re.NotContains(string(resp), sub, "resp: "+string(resp))
	}
}

// StringEqual is used to check whether response context equal given string.
func StringEqual(re *require.Assertions, str string) func([]byte, int, http.Header) {
	return func(resp []byte, _ int, _ http.Header) {
		re.Contains(string(resp), str, "resp: "+string(resp))
	}
}

// WithHeader is used to check whether response header contains given key and value.
func WithHeader(re *require.Assertions, key, value string) func([]byte, int, http.Header) {
	return func(_ []byte, _ int, header http.Header) {
		re.Equal(value, header.Get(key))
	}
}

// WithoutHeader is used to check whether response header does not contain given key.
func WithoutHeader(re *require.Assertions, key string) func([]byte, int, http.Header) {
	return func(_ []byte, _ int, header http.Header) {
		re.Empty(header.Get(key))
	}
}

// ReadGetJSON is used to do get request and check whether given data can be extracted successfully.
func ReadGetJSON(re *require.Assertions, client *http.Client, url string, data any, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	checkOpts = append(checkOpts, StatusOK(re), ExtractJSON(re, data))
	return checkResp(resp, checkOpts...)
}

// ReadGetJSONWithBody is used to do get request with input and check whether given data can be extracted successfully.
func ReadGetJSONWithBody(re *require.Assertions, client *http.Client, url string, input []byte, data any, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.GetJSON(client, url, input)
	if err != nil {
		return err
	}
	checkOpts = append(checkOpts, StatusOK(re), ExtractJSON(re, data))
	return checkResp(resp, checkOpts...)
}

// CheckPostJSON is used to do post request and do check options.
func CheckPostJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.PostJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckGetJSON is used to do get request and do check options.
func CheckGetJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.GetJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckGetUntilStatusCode is used to do get request and do check options.
func CheckGetUntilStatusCode(re *require.Assertions, client *http.Client, url string, code int) error {
	var err error
	Eventually(re, func() bool {
		resp, err2 := apiutil.GetJSON(client, url, nil)
		if err2 != nil {
			err = err2
			return true
		}
		defer resp.Body.Close()
		return resp.StatusCode == code
	})
	return err
}

// CheckPatchJSON is used to do patch request and do check options.
func CheckPatchJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.PatchJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckPutJSON is used to do put request and do check options.
func CheckPutJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.PutJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

// CheckDelete is used to do delete request and do check options.
func CheckDelete(client *http.Client, url string, checkOpts ...func([]byte, int, http.Header)) error {
	resp, err := apiutil.DoDelete(client, url)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func checkResp(resp *http.Response, checkOpts ...func([]byte, int, http.Header)) error {
	res, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	for _, opt := range checkOpts {
		opt(res, resp.StatusCode, resp.Header)
	}
	return nil
}

// TryReadGetJSON performs a GET request and unmarshals the JSON response body.
// Unlike ReadGetJSON, it returns an error instead of using testify assertions,
// making it safe for use inside Eventually condition functions.
func TryReadGetJSON(client *http.Client, url string, data any) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	return tryCheckResp(resp, http.StatusOK, data)
}

// TryReadGetJSONWithBody performs a GET request with body and unmarshals the JSON response.
// Unlike ReadGetJSONWithBody, it returns an error instead of using testify assertions.
func TryReadGetJSONWithBody(client *http.Client, url string, input []byte, data any) error {
	resp, err := apiutil.GetJSON(client, url, input)
	if err != nil {
		return err
	}
	return tryCheckResp(resp, http.StatusOK, data)
}

// TryCheckGetJSON performs a GET request and checks the response status is OK.
// Unlike CheckGetJSON with StatusOK/ExtractJSON, it returns an error instead of using testify assertions.
func TryCheckGetJSON(client *http.Client, url string, data any) error {
	resp, err := apiutil.GetJSON(client, url, nil)
	if err != nil {
		return err
	}
	return tryCheckResp(resp, http.StatusOK, data)
}

// TryCheckPostJSON performs a POST request, checks status is OK and unmarshals response.
// Returns an error instead of using testify assertions, safe for use inside Eventually.
func TryCheckPostJSON(client *http.Client, url string, reqData []byte, data any) error {
	resp, err := apiutil.PostJSON(client, url, reqData)
	if err != nil {
		return err
	}
	return tryCheckResp(resp, http.StatusOK, data)
}

// TryCheckPatchJSON performs a PATCH request, checks status is OK and unmarshals response.
// Returns an error instead of using testify assertions, safe for use inside Eventually.
func TryCheckPatchJSON(client *http.Client, url string, reqData []byte, data any) error {
	resp, err := apiutil.PatchJSON(client, url, reqData)
	if err != nil {
		return err
	}
	return tryCheckResp(resp, http.StatusOK, data)
}

func tryCheckResp(resp *http.Response, expectedCode int, data any) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != expectedCode {
		return fmt.Errorf("expected status %d, got %d: %s", expectedCode, resp.StatusCode, string(body))
	}
	if data != nil {
		return json.Unmarshal(body, data)
	}
	return nil
}
