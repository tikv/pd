// Copyright 2019 TiKV Project Authors.
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

package cluster

import (
	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// StoreLimiter adjust the store limit dynamically
type StoreLimiter struct {
	m       syncutil.RWMutex
	opt     sc.ConfProvider
	scene   map[storelimit.Type]*storelimit.Scene
	state   *State
	current LoadState
}

// NewStoreLimiter builds a store limiter object using the operator controller
func NewStoreLimiter(opt sc.ConfProvider) *StoreLimiter {
	defaultScene := map[storelimit.Type]*storelimit.Scene{
		storelimit.AddPeer:    storelimit.DefaultScene(storelimit.AddPeer),
		storelimit.RemovePeer: storelimit.DefaultScene(storelimit.RemovePeer),
	}

	return &StoreLimiter{
		opt:     opt,
		state:   NewState(),
		scene:   defaultScene,
		current: LoadStateNone,
	}
}

// ReplaceStoreLimitScene replaces the store limit values for different scenes
func (s *StoreLimiter) ReplaceStoreLimitScene(scene *storelimit.Scene, limitType storelimit.Type) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.scene == nil {
		s.scene = make(map[storelimit.Type]*storelimit.Scene)
	}
	s.scene[limitType] = scene
}

// StoreLimitScene returns the current limit for different scenes
func (s *StoreLimiter) StoreLimitScene(limitType storelimit.Type) *storelimit.Scene {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.scene[limitType]
}
