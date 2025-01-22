// Copyright 2025 TiKV Project Authors.
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

package servicediscovery

import (
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

type leaderSwitchedCallbackFunc func(string) error

// serviceCallbacks contains all the callback functions for service discovery events
type serviceCallbacks struct {
	sync.RWMutex
	// serviceModeUpdateCb will be called when the service mode gets updated
	serviceModeUpdateCb func(pdpb.ServiceMode)
	// leaderSwitchedCbs will be called after the leader switched
	leaderSwitchedCbs []leaderSwitchedCallbackFunc
	// membersChangedCbs will be called after there is any membership change in the
	// leader and followers
	membersChangedCbs []func()
}

func newServiceCallbacks() *serviceCallbacks {
	return &serviceCallbacks{
		leaderSwitchedCbs: make([]leaderSwitchedCallbackFunc, 0),
		membersChangedCbs: make([]func(), 0),
	}
}

func (c *serviceCallbacks) setServiceModeUpdateCallback(cb func(pdpb.ServiceMode)) {
	c.Lock()
	defer c.Unlock()
	c.serviceModeUpdateCb = cb
}

func (c *serviceCallbacks) addLeaderSwitchedCallback(cb leaderSwitchedCallbackFunc) {
	c.Lock()
	defer c.Unlock()
	c.leaderSwitchedCbs = append(c.leaderSwitchedCbs, cb)
}

func (c *serviceCallbacks) addMembersChangedCallback(cb func()) {
	c.Lock()
	defer c.Unlock()
	c.membersChangedCbs = append(c.membersChangedCbs, cb)
}

func (c *serviceCallbacks) onServiceModeUpdate(mode pdpb.ServiceMode) {
	c.RLock()
	cb := c.serviceModeUpdateCb
	c.RUnlock()

	if cb == nil {
		return
	}
	cb(mode)
}

func (c *serviceCallbacks) onLeaderSwitched(leader string) error {
	c.RLock()
	cbs := make([]leaderSwitchedCallbackFunc, len(c.leaderSwitchedCbs))
	copy(cbs, c.leaderSwitchedCbs)
	c.RUnlock()

	var err error
	for _, cb := range cbs {
		if cb == nil {
			continue
		}
		err = cb(leader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *serviceCallbacks) onMembersChanged() {
	c.RLock()
	cbs := make([]func(), len(c.membersChangedCbs))
	copy(cbs, c.membersChangedCbs)
	c.RUnlock()

	for _, cb := range cbs {
		if cb == nil {
			continue
		}
		cb()
	}
}
