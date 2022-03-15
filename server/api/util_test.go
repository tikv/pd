// Copyright 2016 TiKV Project Authors.
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
	"io"
	"net/http"
	"net/http/httptest"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/unrolled/render"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestJsonRespondErrorOk(c *C) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input map[string]string
	output := map[string]string{"zone": "cn", "host": "local"}
	err := apiutil.ReadJSONRespondError(rd, response, body, &input)
	c.Assert(err, IsNil)
	c.Assert(input["zone"], Equals, output["zone"])
	c.Assert(input["host"], Equals, output["host"])
	result := response.Result()
	defer result.Body.Close()
	c.Assert(result.StatusCode, Equals, 200)
}

func (s *testUtilSuite) TestJsonRespondErrorBadInput(c *C) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input []string
	err := apiutil.ReadJSONRespondError(rd, response, body, &input)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "json: cannot unmarshal object into Go value of type []string")
	result := response.Result()
	defer result.Body.Close()
	c.Assert(result.StatusCode, Equals, 400)

	{
		body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\","))
		var input []string
		err := apiutil.ReadJSONRespondError(rd, response, body, &input)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "unexpected end of JSON input")
		result := response.Result()
		defer result.Body.Close()
		c.Assert(result.StatusCode, Equals, 400)
	}
}

func checkStatusOK(c *C) func(string, int) {
	return func(_ string, i int) {
		c.Assert(i, Equals, http.StatusOK)
	}
}

func checkStatusNotOK(c *C) func(string, int) {
	return func(_ string, i int) {
		c.Assert(i == http.StatusOK, IsFalse)
	}
}

func checkPostJSON(client *http.Client, url string, data []byte, checkOpts ...func(string, int)) error {
	resp, err := postJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func checkGetJSON(client *http.Client, url string, data []byte, checkOpts ...func(string, int)) error {
	resp, err := getJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func checkPatchJSON(client *http.Client, url string, data []byte, checkOpts ...func(string, int)) error {
	resp, err := patchJSON(client, url, data)
	if err != nil {
		return err
	}
	return checkResp(resp, checkOpts...)
}

func checkResp(resp *http.Response, checkOpts ...func(string, int)) error {
	res, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	for _, opt := range checkOpts {
		opt(string(res), resp.StatusCode)
	}
	return nil
}
