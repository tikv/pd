// Copyright 2017 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("grantLeader", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("grantLeader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return newGrantLeaderScheduler(opt, id), nil
	})
}

const scheduleInterval = time.Millisecond * 10

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	opt     schedule.Options
	name    string
	storeID uint64
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opt schedule.Options, storeID uint64) schedule.Scheduler {
	return &grantLeaderScheduler{
		opt:     opt,
		name:    fmt.Sprintf("grant-leader-scheduler-%d", storeID),
		storeID: storeID,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.name
}

func (s *grantLeaderScheduler) GetInterval() time.Duration {
	return schedule.MinScheduleInterval
}

func (s *grantLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *grantLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return errors.Trace(cluster.BlockStore(s.storeID))
}

func (s *grantLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *grantLeaderScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandFollowerRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: s.storeID}
	return schedule.NewOperator("grantLeader", region.GetId(), core.LeaderKind, step)
}
