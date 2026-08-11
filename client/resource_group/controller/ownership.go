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

package controller

import (
	"sync"

	"github.com/tikv/pd/client/errs"
)

// controllerOwnership enforces the process-wide single-controller contract:
// at most one ResourceGroupsController acquired through
// NewResourceGroupController may exist in a process at a time. The contract
// exists because several integrations around the controller are process-global
// and carry no controller identity, e.g. the resource-control Prometheus
// collectors, the enableControllerTraceLog flag, and the client-go
// resource-control interceptor.
//
// The ownership slot is reserved before the controller is constructed (the
// constructor performs network I/O and updates process-global state), bound to
// the controller on success, and released either on a failed construction or
// by an idempotent Stop. Canceling the context passed to Start stops the run
// loop but does NOT release ownership; callers must call Stop explicitly.
type controllerOwnership struct {
	mu sync.Mutex
	// reserved is true while a constructor holds the slot but has not bound
	// a controller to it yet.
	reserved bool
	// owner is the controller currently holding the slot.
	owner *ResourceGroupsController
}

// ownership is the process-wide ownership slot.
var ownership controllerOwnership

// reserve claims the slot for a construction in progress. It returns
// ErrClientResourceGroupControllerAlreadyExists if the slot is held.
func (o *controllerOwnership) reserve() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reserved || o.owner != nil {
		return errs.ErrClientResourceGroupControllerAlreadyExists.FastGenByArgs()
	}
	o.reserved = true
	return nil
}

// unreserve releases a reservation after a failed construction.
func (o *controllerOwnership) unreserve() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reserved = false
}

// bind transfers the reservation to the successfully constructed controller.
func (o *controllerOwnership) bind(c *ResourceGroupsController) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.owner = c
	o.reserved = false
}

// release frees the slot if and only if c is the current owner, so a stale
// controller's Stop cannot release a newer controller's slot.
func (o *controllerOwnership) release(c *ResourceGroupsController) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.owner == c {
		o.owner = nil
	}
}
