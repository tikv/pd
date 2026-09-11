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

// controllerOwnership enforces the single-controller contract of
// tikv/pd#11080: at most one ResourceGroupsController acquired through
// NewResourceGroupController may exist in a process at a time, because the
// integrations around the controller (Prometheus collectors, the trace-log
// flag, the client-go interceptor) are process-global and carry no
// controller identity. The slot is released only by an explicit Stop; in
// particular, canceling the context passed to Start does not release it.
type controllerOwnership struct {
	mu sync.Mutex
	// reserved marks a construction in progress, before a controller is
	// bound to the slot.
	reserved bool
	owner    *ResourceGroupsController
}

var ownership controllerOwnership

func (o *controllerOwnership) reserve() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reserved || o.owner != nil {
		return errs.ErrClientResourceGroupControllerAlreadyExists.FastGenByArgs()
	}
	o.reserved = true
	return nil
}

func (o *controllerOwnership) unreserve() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reserved = false
}

func (o *controllerOwnership) bind(c *ResourceGroupsController) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.owner = c
	o.reserved = false
}

// release only honors the current owner, so a stale controller's Stop cannot
// free a newer controller's slot.
func (o *controllerOwnership) release(c *ResourceGroupsController) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.owner == c {
		o.owner = nil
	}
}

func (o *controllerOwnership) owns(c *ResourceGroupsController) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.owner == c
}
