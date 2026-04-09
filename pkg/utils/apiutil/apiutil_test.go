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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestJsonRespondErrorOk(t *testing.T) {
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

type errReader struct{ err error }

func (e *errReader) Read(p []byte) (int, error) { return 0, e.err }
func (*errReader) Close() error                 { return nil }

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

func TestEnsureRewindableBody(t *testing.T) {
	re := require.New(t)

	t.Run("nil body is a no-op", func(t *testing.T) {
		r := &http.Request{}
		re.NoError(EnsureRewindableBody(r))
		re.Nil(r.Body)
		re.Nil(r.GetBody)
	})

	t.Run("http.NoBody is a no-op", func(t *testing.T) {
		r := &http.Request{Body: http.NoBody}
		re.NoError(EnsureRewindableBody(r))
		re.Equal(http.NoBody, r.Body)
		re.Nil(r.GetBody)
	})

	t.Run("existing GetBody is preserved", func(t *testing.T) {
		orig := io.NopCloser(bytes.NewBufferString("payload"))
		called := false
		getBody := func() (io.ReadCloser, error) {
			called = true
			return io.NopCloser(bytes.NewBufferString("payload")), nil
		}
		r := &http.Request{Body: orig, GetBody: getBody}
		re.NoError(EnsureRewindableBody(r))
		// Body untouched, GetBody untouched (we only check it wasn't replaced
		// by invoking it and confirming our own sentinel).
		re.Equal(orig, r.Body)
		_, err := r.GetBody()
		re.NoError(err)
		re.True(called)
	})

	t.Run("empty body is restored to NoBody", func(t *testing.T) {
		tracker := &closeTracker{Reader: bytes.NewReader(nil)}
		r := &http.Request{Body: tracker}
		re.NoError(EnsureRewindableBody(r))
		re.True(tracker.closed, "original body should be closed")
		re.Equal(http.NoBody, r.Body)
		re.EqualValues(0, r.ContentLength)
		re.NotNil(r.GetBody)
		rc, err := r.GetBody()
		re.NoError(err)
		re.Equal(http.NoBody, rc)
	})

	t.Run("non-empty body becomes rewindable", func(t *testing.T) {
		payload := []byte(`{"hello":"world"}`)
		tracker := &closeTracker{Reader: bytes.NewReader(payload)}
		r := &http.Request{Body: tracker, ContentLength: -1}
		re.NoError(EnsureRewindableBody(r))
		re.True(tracker.closed, "original body should be closed")
		re.EqualValues(len(payload), r.ContentLength)
		re.NotNil(r.GetBody)

		// Draining r.Body once should yield the payload.
		got, err := io.ReadAll(r.Body)
		re.NoError(err)
		re.Equal(payload, got)
		re.NoError(r.Body.Close())

		// GetBody should be invokable multiple times and each returned
		// ReadCloser should independently yield the same payload -- this is
		// the actual rewindability guarantee we care about.
		for i := 0; i < 3; i++ {
			rc, err := r.GetBody()
			re.NoError(err)
			got, err := io.ReadAll(rc)
			re.NoError(err)
			re.Equal(payload, got)
			re.NoError(rc.Close())
		}
	})

	t.Run("read error is propagated", func(t *testing.T) {
		wantErr := errors.New("boom")
		r := &http.Request{Body: &errReader{err: wantErr}}
		err := EnsureRewindableBody(r)
		re.ErrorIs(err, wantErr)
	})
}

func TestParseHexKeys(t *testing.T) {
	re := require.New(t)
	// Test for hex format
	hexBytes := [][]byte{[]byte(""), []byte("67"), []byte("0001020304050607"), []byte("08090a0b0c0d0e0f"), []byte("f0f1f2f3f4f5f6f7")}
	parseKeys, err := ParseHexKeys("hex", hexBytes)
	re.NoError(err)
	expectedBytes := [][]byte{[]byte(""), []byte("g"), []byte("\x00\x01\x02\x03\x04\x05\x06\x07"), []byte("\x08\t\n\x0b\x0c\r\x0e\x0f"), []byte("\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7")}
	re.Equal(expectedBytes, parseKeys)
	// Test for other format NOT hex
	hexBytes = [][]byte{[]byte("hello")}
	parseKeys, err = ParseHexKeys("other", hexBytes)
	re.NoError(err)
	re.Len(parseKeys, 1)
	re.Equal([]byte("hello"), parseKeys[0])
	// Test for wrong key
	hexBytes = [][]byte{[]byte("world")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 1)
	re.Equal([]byte("world"), parseKeys[0])
	// Test for the first key is not valid, but the second key is valid
	hexBytes = [][]byte{[]byte("world"), []byte("0001020304050607")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 2)
	re.Equal([]byte("world"), parseKeys[0])
	re.NotEqual([]byte("\x00\x01\x02\x03\x04\x05\x06\x07"), parseKeys[1])
	// Test for the first key is valid, but the second key is not valid
	hexBytes = [][]byte{[]byte("0001020304050607"), []byte("world")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 2)
	re.NotEqual([]byte("\x00\x01\x02\x03\x04\x05\x06\x07"), parseKeys[0])
	re.Equal([]byte("world"), parseKeys[1])
}
