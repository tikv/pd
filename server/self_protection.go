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

// SelfProtectionHandler is a framework to handle self protection mechanism
// Self-protection granularity is a logical service
type SelfProtectionHandler struct {
	// ServiceHandlers is a map to store handler owned by different services
	ServiceHandlers map[string]*serviceSelfProtectionHandler
}

// NewSelfProtectionHandler returns a new SelfProtectionHandler with config
func NewSelfProtectionHandler(server *Server) *SelfProtectionHandler {
	handler := &SelfProtectionHandler{
		ServiceHandlers: make(map[string]*serviceSelfProtectionHandler),
	}
	return handler
}

// HandleHTTPSelfProtection is used to handle http api self protection
func (h *SelfProtectionHandler) HandleHTTPSelfProtection(req *http.Request) bool {
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

	httpHandler := &HTTPServiceSelfProtectionHandler{
		req:     req,
		handler: serviceHandler,
	}
	return httpHandler.Handle()
}

// ServiceSelfProtectionHandler is a interface for define self-protection handler by service granularity
type ServiceSelfProtectionHandler interface {
	Handle() bool
}

// HTTPServiceSelfProtectionHandler implement ServiceSelfProtectionHandler to handle http
type HTTPServiceSelfProtectionHandler struct {
	req     *http.Request
	handler *serviceSelfProtectionHandler
}

// Handle implement ServiceSelfProtectionHandler defined function
func (h *HTTPServiceSelfProtectionHandler) Handle() bool {
	// to be implemented
	return true
}

// serviceSelfProtectionHandler is a handler which is independent communication mode
type serviceSelfProtectionHandler struct {
	// todo APIRateLimiter
	// todo AuditLogger
}
