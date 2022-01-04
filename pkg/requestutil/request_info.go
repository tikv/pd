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
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	// registeredSericeLabel is used to find service which request wants to access
	registeredSericeLabel map[requestSchema]string
)

func init() {
	registeredSericeLabel = make(map[requestSchema]string)

	if ok := addServiceLabel("/pd/api/v1/version", "GET", "GetPDVersion"); !ok {
		log.Error("Service Label Repetition", zap.String("URL PATH", "pd/api/v1/version"), zap.String("METHOD", "GET"))
	}
}

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

// RequestSchema identifies http.Reuqest schema info
type requestSchema struct {
	path   string
	method string
}

// NewRequestSchema returns a new RequestSchema
func NewRequestSchema(path string, method string) requestSchema {
	return requestSchema{path: path, method: method}
}

// getServiceLabel returns service label which is defined when register router handle
func getServiceLabel(r *http.Request) string {
	schema := NewRequestSchema(r.URL.Path, r.Method)
	return registeredSericeLabel[schema]
}

// addServiceLabel is used to add service label
// when request schema has been added, it returns false
func addServiceLabel(path string, method string, serviceLabel string) bool {
	result, ok := registeredSericeLabel[NewRequestSchema(path, method)]
	if ok && result != serviceLabel {
		return false
	}
	registeredSericeLabel[NewRequestSchema(path, method)] = serviceLabel
	return true
}

func GetRequestInfo(r *http.Request) RequestInfo {
	// todo component and ip
	return RequestInfo{
		ServiceLabel: getServiceLabel(r),
		Method:       fmt.Sprintf("HTTP/%s:%s", r.Method, r.URL.Path),
		TimeStamp:    time.Now().Local().String(),
		URLParam:     getURLParam(r),
		BodyParm:     getBodyParam(r),
	}
}

func getURLParam(r *http.Request) string {
	vars := mux.Vars(r)

	buf, err := json.Marshal(vars)
	var param = ""
	if err != nil {
		param = string(buf)
	}

	return param
}

func getBodyParam(r *http.Request) string {
	buf, err := io.ReadAll(r.Body)
	var bodyParam = ""
	if err != nil {
		bodyParam = string(buf)
	}
	r.Body = io.NopCloser(bytes.NewBuffer(buf))

	return bodyParam
}
