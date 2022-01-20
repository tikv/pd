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

package requestutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tikv/pd/pkg/apiutil"
)

// RequestInfo holds service information from http.Request
type RequestInfo struct {
	ServiceLabel string
	Method       string
	Component    string
	IP           string
	TimeStamp    string
	URLParam     string
	BodyParam    string
}

func (info *RequestInfo) String() string {
	s := fmt.Sprintf("{ServiceLabel:%s, Method:%s, Component:%s, IP:%s, TimeStamp:%s, URLParam:%s, BodyParam:%s}",
		info.ServiceLabel, info.Method, info.Component, info.IP, info.TimeStamp, info.URLParam, info.BodyParam)
	return s
}

// GetRequestInfo returns request info needed from http.Request
func GetRequestInfo(r *http.Request) RequestInfo {
	return RequestInfo{
		ServiceLabel: apiutil.GetRouteName(r),
		Method:       fmt.Sprintf("%s/%s:%s", r.Proto, r.Method, r.URL.Path),
		Component:    apiutil.GetComponentNameOnHTTP(r),
		IP:           apiutil.GetIPAddrFromHTTPRequest(r),
		TimeStamp:    time.Now().Local().String(),
		URLParam:     getURLParam(r),
		BodyParam:    getBodyParam(r),
	}
}

func getURLParam(r *http.Request) string {
	buf, err := json.Marshal(r.URL.Query())
	if err != nil {
		return ""
	}
	return string(buf)
}

func getBodyParam(r *http.Request) string {
	if r.Body == nil {
		return ""
	}
	// http request body is a io.Reader between bytes.Reader and strings.Reader, it only has EOF error
	buf, _ := io.ReadAll(r.Body)
	r.Body.Close()
	bodyParam := string(buf)
	r.Body = io.NopCloser(bytes.NewBuffer(buf))
	return bodyParam
}
