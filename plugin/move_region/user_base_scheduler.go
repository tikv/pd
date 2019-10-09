package main

import (
	"github.com/pingcap/pd/server/schedule"
	"time"
)

type userBaseScheduler struct {
	opController *schedule.OperatorController
}

// options for interval of schedulers
const (
	MaxScheduleInterval     = time.Second * 5
	MinScheduleInterval     = time.Millisecond * 10

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
