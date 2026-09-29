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

package requestutil

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type trackingReadCloser struct {
	io.Reader
	read  bool
	close bool
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	r.read = true
	return r.Reader.Read(p)
}

func (r *trackingReadCloser) Close() error {
	r.close = true
	return nil
}

func TestGetRequestInfoWithoutBodyDoesNotConsumeBody(t *testing.T) {
	re := require.New(t)
	body := &trackingReadCloser{Reader: strings.NewReader("request-body")}
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", body)
	re.NoError(err)

	info := GetRequestInfoWithoutBody(req)
	re.Empty(info.BodyParam)
	re.False(body.read)
	re.False(body.close)

	data, err := io.ReadAll(req.Body)
	re.NoError(err)
	re.Equal("request-body", string(data))
}

func TestGetRequestInfoRestoresExactBody(t *testing.T) {
	re := require.New(t)
	const requestBody = "request-body-\x00-\xff"
	body := &trackingReadCloser{Reader: strings.NewReader(requestBody)}
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", body)
	re.NoError(err)

	info := GetRequestInfo(req)
	re.Equal(requestBody, info.BodyParam)
	re.True(body.read)
	re.True(body.close)

	data, err := io.ReadAll(req.Body)
	re.NoError(err)
	re.Equal([]byte(requestBody), data)
}
