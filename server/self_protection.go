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

// selfProtectionManager is a framework to handle self protection mechanism
// Self-protection granularity is a logical service
type selfProtectionManager struct {
	// ServiceHandlers is a map to store handler owned by different services
	ServiceHandlers map[string]*serviceSelfProtectionHandler
}

// NewSelfProtectionManager returns a new SelfProtectionManager with config
func NewSelfProtectionManager(server *Server) *selfProtectionManager {
	return &selfProtectionManager{
		ServiceHandlers: make(map[string]*serviceSelfProtectionHandler),
	}
}

// ProcessHTTPSelfProtection is used to process http api self protection
func (h *selfProtectionManager) ProcessHTTPSelfProtection(req *http.Request) bool {
	serviceName, foundName := apiutil.GetRouteName(req)
	// if path is not registered in router, go on processing
	if !foundName {
		return true
	}

	serviceHandler, ok := h.ServiceHandlers[serviceName]
	// if there is no service handler, go on processing
	if !ok {
		return true
	}

	return HandleServiceHTTPSelfProtection(req, serviceHandler)
}

// HandleServiceHTTPSelfProtection is used to implement self-protection HTTP handler by service granularity
func HandleServiceHTTPSelfProtection(req *http.Request, handler *serviceSelfProtectionHandler) bool {
	// to be implemented
	return true
}

// serviceSelfProtectionHandler is a handler which is independent communication mode
type serviceSelfProtectionHandler struct {
	// todo APIRateLimiter
	// todo AuditLogger
}
