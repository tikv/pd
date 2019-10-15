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
// limitations under the License.

package main

import (
	"time"

	"github.com/pingcap/pd/server/schedule"
)

type userBaseScheduler struct {
	opController *schedule.OperatorController
}

// options for interval of schedulers
const (
	MaxScheduleInterval = time.Second * 5
	MinScheduleInterval = time.Millisecond * 10

	ScheduleIntervalFactor = 1.3
)

func newUserBaseScheduler(opController *schedule.OperatorController) *userBaseScheduler {
	return &userBaseScheduler{opController: opController}
}

func (s *userBaseScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (s *userBaseScheduler) Cleanup(cluster schedule.Cluster) {}

func (s *userBaseScheduler) GetMinInterval() time.Duration {
	return MinScheduleInterval
}

func (s *userBaseScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return minDuration(time.Duration(float64(interval)*ScheduleIntervalFactor), MaxScheduleInterval)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
