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
	"net/http"
	"sync"
)

var (
	// registeredSericeLabel is used to find service which request wants to access
	registeredSericeLabel map[requestSchema]string
	lock                  sync.RWMutex
)

func init() {
	registeredSericeLabel = make(map[requestSchema]string)
}

// SourceInfo holds source information from http.Request
type SourceInfo struct {
	// todo component and realIP
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

// GetServiceLabel returns service label which is defined when register router handle
func GetServiceLabel(r *http.Request) string {
	lock.RLock()
	defer lock.RUnlock()
	schema := NewRequestSchema(r.URL.Path, r.Method)
	return registeredSericeLabel[schema]
}

// AddServiceLabel is used to add service label
// when request schema has been added, it returns false
func AddServiceLabel(path string, method string, serviceLabel string) bool {
	lock.Lock()
	defer lock.Unlock()
	result, ok := registeredSericeLabel[NewRequestSchema(path, method)]
	if ok && result != serviceLabel {
		return false
	}
	registeredSericeLabel[NewRequestSchema(path, method)] = serviceLabel
	return true
}
