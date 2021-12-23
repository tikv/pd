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

package server

import (
	"net/http"

	"github.com/tikv/pd/pkg/apiutil"
)

// SelfProtectionManager is a framework to handle self protection mechanism
// Self-protection granularity is a logical service
type SelfProtectionManager struct {
	// ServiceHandlers is a map to store handler owned by different services
	ServiceHandlers map[string]*serviceSelfProtectionHandler
}

// NewSelfProtectionManager returns a new SelfProtectionManager with config
func NewSelfProtectionManager(server *Server) *SelfProtectionManager {
	handler := &SelfProtectionManager{
		ServiceHandlers: make(map[string]*serviceSelfProtectionHandler),
	}
	return handler
}

// ProcessHTTPSelfProtection is used to process http api self protection
func (h *SelfProtectionManager) ProcessHTTPSelfProtection(req *http.Request) bool {
	serviceName, findName := apiutil.GetHTTPRouteName(req)
	// if path is not registered in router, go on process
	if !findName {
		return true
	}

	serviceHandler, ok := h.ServiceHandlers[serviceName]
	// if there is no service handler, go on process
	if !ok {
		return true
	}

	httpHandler := &HTTPServiceSelfProtectionManager{
		req:     req,
		handler: serviceHandler,
	}
	return httpHandler.Handle()
}

// ServiceSelfProtectionManager is a interface for define self-protection handler by service granularity
type ServiceSelfProtectionManager interface {
	Handle() bool
}

// HTTPServiceSelfProtectionManager implement ServiceSelfProtectionManager to handle http
type HTTPServiceSelfProtectionManager struct {
	req     *http.Request
	handler *serviceSelfProtectionHandler
}

// Handle implement ServiceSelfProtectionManager defined function
func (h *HTTPServiceSelfProtectionManager) Handle() bool {
	// to be implemented
	return true
}

// serviceSelfProtectionHandler is a handler which is independent communication mode
type serviceSelfProtectionHandler struct {
	// todo APIRateLimiter
	// todo AuditLogger
}
