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
	"github.com/pingcap/pd/server/schedule"
	"go.uber.org/zap"
)

// StateCalcDuration is the duration being used to caculate the cluster
// state
const StateCalcDuration = 300 // 5 minutes

// StoreLimiter adjust the store limit dynamically
type StoreLimiter struct {
	m     sync.RWMutex
	oc    *schedule.OperatorController
	scene *schedule.StoreLimitScene
	state *ClusterState
}

// NewStoreLimiter builds a store limiter object using the operator controller
func NewStoreLimiter(c *schedule.OperatorController) *StoreLimiter {
	return &StoreLimiter{
		oc:    c,
		state: NewClusterState(),
		scene: schedule.DefaultStoreLimitScene(),
	}
}

// Collect the store statistics and update the cluster state
func (s *StoreLimiter) Collect(stats *pdpb.StoreStats) {
	s.m.Lock()
	defer s.m.Unlock()

	s.state.Collect((*StatEntry)(stats))

	rate := float64(0)
	read, written := s.state.cst.Keys(StateCalcDuration)
	if read == 0 && written == 0 {
		log.Info("no keys read or written, the cluster is idle")
		rate = float64(s.scene.Idle) / schedule.StoreBalanceBaseTime
		s.oc.SetAllStoresLimit(rate, schedule.StoreLimitAuto)
		return
	}
	state := s.state.State(StateCalcDuration)
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
		s.oc.SetAllStoresLimit(rate, schedule.StoreLimitAuto)
		log.Info("change store limit for cluster", zap.Stringer("state", state), zap.Float64("rate", rate))
	}
}
