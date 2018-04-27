// Copyright 2018 PingCAP, Inc.
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

package schedulers

import (
	"fmt"
	"net/url"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("scatter-range", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		if len(args) != 3 {
			return nil, errors.New("should specify the range and the name")
		}
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		name := args[2]
		return newScatterRangeScheduler(limiter, []string{startKey, endKey, name}), nil
	})
}

type scatterRangeScheduler struct {
	*baseScheduler
	name          string
	filters       []schedule.Filter
	startKey      []byte
	endKey        []byte
	balanceLeader schedule.Scheduler
	balanceRegion schedule.Scheduler
}

// newScatterRangeScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newScatterRangeScheduler(limiter *schedule.Limiter, args []string) schedule.Scheduler {
	base := newBaseScheduler(limiter)
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
		schedule.NewSnapshotCountFilter(),
		schedule.NewPendingPeerCountFilter(),
	}
	return &scatterRangeScheduler{
		baseScheduler: base,
		startKey:      []byte(args[0]),
		endKey:        []byte(args[1]),
		name:          fmt.Sprintf("scatter-range-leader-%s", args[2]),
		filters:       filters,
		balanceLeader: newBalanceLeaderScheduler(limiter),
		balanceRegion: newBalanceRegionScheduler(limiter),
	}
}

func (l *scatterRangeScheduler) GetName() string {
	return l.name
}

func (l *scatterRangeScheduler) GetType() string {
	return "scatter-range-leader"
}

func (l *scatterRangeScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.limiter.OperatorCount(schedule.OpRange) < 1
}

func (l *scatterRangeScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) []*schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	c := schedule.GenRangeCluster(cluster, l.startKey, l.endKey)
	c.SetTolerantSizeRatio(2)
	ops := l.balanceLeader.Schedule(c, schedule.NewOpInfluence(nil, cluster))
	if len(ops) > 0 {
		ops[0].SetDesc("scatter-range-leader")
		ops[0].AttachKind(schedule.OpRange)
		return ops
	}
	ops = l.balanceRegion.Schedule(c, schedule.NewOpInfluence(nil, cluster))
	if len(ops) > 0 {
		ops[0].SetDesc("scatter-range-region")
		ops[0].AttachKind(schedule.OpRange)
		return ops
	}
	return nil
}
