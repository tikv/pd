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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

// RequestInfo holds service information from http.Request
type RequestInfo struct {
	ServiceLabel string
	Method       string
	// CallerID is used to identify the specific source of a HTTP request, it will be marked in
	// the PD HTTP client, with granularity that can be refined to a specific functionality within a component.
	CallerID       string
	IP             string
	Port           string
	URLParam       string
	BodyParam      string
	StartTimeStamp int64
}

func (info *RequestInfo) String() string {
	s := fmt.Sprintf("{ServiceLabel:%s, Method:%s, CallerID:%s, IP:%s, Port:%s, StartTime:%s, URLParam:%s, BodyParam:%s}",
		info.ServiceLabel, info.Method, info.CallerID, info.IP, info.Port, time.Unix(info.StartTimeStamp, 0), info.URLParam, info.BodyParam)
	return s
}

// GetRequestInfo returns request info needed from http.Request
func GetRequestInfo(r *http.Request) RequestInfo {
	info := GetRequestInfoWithoutBody(r)
	info.CaptureBody(r)
	return info
}

// GetRequestInfoWithoutBody returns request info without consuming or buffering
// the request body.
func GetRequestInfoWithoutBody(r *http.Request) RequestInfo {
	ip, port := apiutil.GetIPPortFromHTTPRequest(r)
	return RequestInfo{
		ServiceLabel:   apiutil.GetRouteName(r),
		Method:         fmt.Sprintf("%s/%s:%s", r.Proto, r.Method, r.URL.Path),
		CallerID:       apiutil.GetCallerIDOnHTTP(r),
		IP:             ip,
		Port:           port,
		URLParam:       getURLParam(r),
		StartTimeStamp: time.Now().Unix(),
	}
}

// CaptureBody consumes the request body into BodyParam and restores an
// equivalent body for the handler.
func (info *RequestInfo) CaptureBody(r *http.Request) {
	info.BodyParam = getBodyParam(r)
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
	buf, _ := io.ReadAll(r.Body)
	r.Body.Close()
	// Restore the body from BodyParam so both views share one long-lived backing
	// store. Restoring from buf would retain a second complete copy until the
	// handler finishes.
	bodyParam := string(buf)
	r.Body = io.NopCloser(strings.NewReader(bodyParam))
	return bodyParam
}
