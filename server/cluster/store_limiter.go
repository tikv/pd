// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License

package cluster

import (
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/schedule"
	"go.uber.org/zap"
)

// StoreLimiter adjust the store limit dynamically
type StoreLimiter struct {
	m       sync.RWMutex
	oc      *schedule.OperatorController
	scene   *schedule.StoreLimitScene
	state   *State
	current LoadState
}

// NewStoreLimiter builds a store limiter object using the operator controller
func NewStoreLimiter(c *schedule.OperatorController) *StoreLimiter {
	return &StoreLimiter{
		oc:      c,
		state:   NewState(),
		scene:   schedule.DefaultStoreLimitScene(),
		current: LoadStateNone,
	}
}

// Collect the store statistics and update the cluster state
func (s *StoreLimiter) Collect(stats *pdpb.StoreStats) {
	s.m.Lock()
	defer s.m.Unlock()

	log.Debug("collected statistics", zap.Reflect("stats", stats))
	s.state.Collect((*StatEntry)(stats))

	rate := float64(0)
	state := s.state.State()
	switch state {
	case LoadStateIdle:
		rate = float64(s.scene.Idle) / schedule.StoreBalanceBaseTime
	case LoadStateLow:
		rate = float64(s.scene.Low) / schedule.StoreBalanceBaseTime
	case LoadStateNormal:
		rate = float64(s.scene.Normal) / schedule.StoreBalanceBaseTime
	case LoadStateHigh:
		rate = float64(s.scene.High) / schedule.StoreBalanceBaseTime
	}

	if rate > 0 {
		s.oc.SetAllStoresLimitAuto(rate)
		log.Info("change store limit for cluster", zap.Stringer("state", state), zap.Float64("rate", rate))
		s.current = state
		collectClusterStateCurrent(state)
	}
}

func collectClusterStateCurrent(state LoadState) {
	for i := LoadStateNone; i <= LoadStateHigh; i++ {
		if i == state {
			clusterStateCurrent.WithLabelValues(state.String()).Set(1)
			continue
		}
		clusterStateCurrent.WithLabelValues(i.String()).Set(0)
	}
}

// ReplaceStoreLimitScene replaces the store limit values for different scenes
func (s *StoreLimiter) ReplaceStoreLimitScene(scene *schedule.StoreLimitScene) {
	s.m.Lock()
	defer s.m.Unlock()
	s.scene = scene
}

// StoreLimitScene returns the current limit for different scenes
func (s *StoreLimiter) StoreLimitScene() *schedule.StoreLimitScene {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.scene
}
