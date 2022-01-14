// Copyright 2021 TiKV Project Authors.
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
	"strings"
	"time"

	"github.com/tikv/pd/pkg/apiutil"
)

// RequestInfo holds source information from http.Request
type RequestInfo struct {
	ServiceLabel string
	Method       string
	Component    string
	IP           string
	TimeStamp    string
	URLParam     string
	BodyParm     string
}

// RequestSchemaList is used to store requestSchemas which have been registered
type RequestSchemaList struct {
	requestSchemas []*RequestSchema
}

// NewRequestSchemaList returns a new RequestSchemaList with given length
func NewRequestSchemaList(len int) *RequestSchemaList {
	return &RequestSchemaList{requestSchemas: make([]*RequestSchema, 0, len)}
}

func (l *RequestSchemaList) match(path string, method string) string {
	for _, schema := range l.requestSchemas {
		if schema.match(path, method) {
			return schema.serviceLabel
		}
	}
	return ""
}

// AddServiceLabel is used to register requestSchema
func (l *RequestSchemaList) AddServiceLabel(paths []string, method, serviceLabel string) {
	l.requestSchemas = append(l.requestSchemas,
		&RequestSchema{paths: paths, method: method, serviceLabel: serviceLabel})
}

// GetServiceLabel returns service label which is defined when register router handle
func (l *RequestSchemaList) GetServiceLabel(r *http.Request) string {
	return l.match(r.URL.Path, r.Method)
}

// RequestSchema identifies http.Reuqest schema info
type RequestSchema struct {
	paths        []string
	method       string
	serviceLabel string
}

func (r *RequestSchema) match(path, method string) bool {
	if len(r.method) > 0 && r.method != method {
		return false
	}
	paths := strings.Split(strings.Trim(path, "/"), "/")
	if len(r.paths) != len(paths) {
		return false
	}
	for i := 0; i < len(paths); i++ {
		if len(r.paths[i]) == 0 {
			continue
		}
		if r.paths[i] != paths[i] {
			return false
		}
	}
	return true
}

// GetRequestInfo returns request info needed from http.Request
func (l *RequestSchemaList) GetRequestInfo(r *http.Request) RequestInfo {
	return RequestInfo{
		ServiceLabel: l.GetServiceLabel(r),
		Method:       fmt.Sprintf("HTTP/%s:%s", r.Method, r.URL.Path),
		Component:    apiutil.GetComponentNameOnHTTP(r),
		IP:           apiutil.GetIPAddrFromHTTPRequest(r),
		TimeStamp:    time.Now().Local().String(),
		URLParam:     getURLParam(r),
		BodyParm:     getBodyParam(r),
	}
}

func getURLParam(r *http.Request) string {
	buf, err := json.Marshal(r.URL.Query())
	var param = ""
	if err == nil {
		param = string(buf)
	}

	return param
}

func getBodyParam(r *http.Request) string {
	if r.Body == nil {
		return ""
	}
	buf, err := io.ReadAll(r.Body)
	var bodyParam = ""
	if err == nil {
		bodyParam = string(buf)
	}
	r.Body = io.NopCloser(bytes.NewBuffer(buf))

	return bodyParam
}
