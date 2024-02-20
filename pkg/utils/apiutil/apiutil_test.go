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

package apiutil

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"
)

func TestJsonRespondErrorOk(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input map[string]string
	output := map[string]string{"zone": "cn", "host": "local"}
	err := ReadJSONRespondError(rd, response, body, &input)
	re.NoError(err)
	re.Equal(output["zone"], input["zone"])
	re.Equal(output["host"], input["host"])
	result := response.Result()
	defer result.Body.Close()
	re.Equal(200, result.StatusCode)
}

func TestJsonRespondErrorBadInput(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input []string
	err := ReadJSONRespondError(rd, response, body, &input)
	re.EqualError(err, "json: cannot unmarshal object into Go value of type []string")
	result := response.Result()
	defer result.Body.Close()
	re.Equal(400, result.StatusCode)

	{
		body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\","))
		var input []string
		err := ReadJSONRespondError(rd, response, body, &input)
		re.EqualError(err, "unexpected end of JSON input")
		result := response.Result()
		defer result.Body.Close()
		re.Equal(400, result.StatusCode)
	}
}

func TestGetIPPortFromHTTPRequest(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	testCases := []struct {
		r    *http.Request
		ip   string
		port string
		err  error
	}{
		// IPv4 "X-Forwarded-For" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"127.0.0.1:5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Forwarded-For" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader:  {"127.0.0.1"},
					XForwardedPortHeader: {"5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Real-Ip" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XRealIPHeader: {"127.0.0.1:5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Real-Ip" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader:  {"127.0.0.1"},
					XForwardedPortHeader: {"5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 RemoteAddr with port
		{
			r: &http.Request{
				RemoteAddr: "127.0.0.1:5299",
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 RemoteAddr without port
		{
			r: &http.Request{
				RemoteAddr: "127.0.0.1",
			},
			ip:   "127.0.0.1",
			port: "",
		},
		// IPv6 "X-Forwarded-For" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"[::1]:5299"},
				},
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 "X-Forwarded-For" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"::1"},
				},
			},
			ip:   "::1",
			port: "",
		},
		// IPv6 "X-Real-Ip" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XRealIPHeader: {"[::1]:5299"},
				},
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 "X-Real-Ip" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"::1"},
				},
			},
			ip:   "::1",
			port: "",
		},
		// IPv6 RemoteAddr with port
		{
			r: &http.Request{
				RemoteAddr: "[::1]:5299",
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 RemoteAddr without port
		{
			r: &http.Request{
				RemoteAddr: "::1",
			},
			ip:   "::1",
			port: "",
		},
		// Abnormal case
		{
			r:    &http.Request{},
			ip:   "",
			port: "",
		},
	}
	for idx, testCase := range testCases {
		ip, port := GetIPPortFromHTTPRequest(testCase.r)
		re.Equal(testCase.ip, ip, "case %d", idx)
		re.Equal(testCase.port, port, "case %d", idx)
	}
}
