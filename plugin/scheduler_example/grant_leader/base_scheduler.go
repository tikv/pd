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
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/opt"
)

// options for interval of schedulers
const (
	MaxScheduleInterval     = time.Second * 5
	MinScheduleInterval     = time.Millisecond * 10
	MinSlowScheduleInterval = time.Second * 3

	ScheduleIntervalFactor = 1.3
)

type intervalGrowthType int

const (
	exponentailGrowth intervalGrowthType = iota
	linearGrowth
	zeroGrowth
)

// intervalGrow calculates the next interval of balance.
func intervalGrow(x time.Duration, maxInterval time.Duration, typ intervalGrowthType) time.Duration {
	switch typ {
	case exponentailGrowth:
		return minDuration(time.Duration(float64(x)*ScheduleIntervalFactor), maxInterval)
	case linearGrowth:
		return minDuration(x+MinSlowScheduleInterval, maxInterval)
	case zeroGrowth:
		return x
	default:
		log.Fatal("unknown interval growth type")
	}
	return 0
}

type userBaseScheduler struct {
	opController *schedule.OperatorController
}

func newUserBaseScheduler(opController *schedule.OperatorController) *userBaseScheduler {
	return &userBaseScheduler{opController: opController}
}

// ServeHTTP is used to serve HTTP request
func (s *userBaseScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implements")
}

// GetNextInterval is used to get min schedule interval.
func (s *userBaseScheduler) GetMinInterval() time.Duration {
	return MinScheduleInterval
}

// EncodeConfig is used to encode config
func (s *userBaseScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(nil)
}

// GetNextInterval is used to get next interval.
func (s *userBaseScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, MaxScheduleInterval, exponentailGrowth)
}

// Prepare is used to do some prepare work before the scheduler runs
func (s *userBaseScheduler) Prepare(cluster opt.Cluster) error { return nil }

// Cleanup corresponds to prepare, which is used to do some cleanup work when the scheduler stops
func (s *userBaseScheduler) Cleanup(cluster opt.Cluster) {}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
