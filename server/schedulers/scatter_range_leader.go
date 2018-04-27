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
	"bytes"
	"fmt"
	"math"
	"net/url"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("scatter-range-leader", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
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
		return newScatterRangeLeaderScheduler(limiter, []string{startKey, endKey, name}), nil
	})
}

type scatterRangeLeaderScheduler struct {
	*baseScheduler
	name     string
	filters  []schedule.Filter
	startKey []byte
	endKey   []byte
}

// newScatterRangeLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newScatterRangeLeaderScheduler(limiter *schedule.Limiter, args []string) schedule.Scheduler {
	base := newBaseScheduler(limiter)
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
		schedule.NewSnapshotCountFilter(),
		schedule.NewPendingPeerCountFilter(),
	}
	return &scatterRangeLeaderScheduler{
		baseScheduler: base,
		startKey:      []byte(args[0]),
		endKey:        []byte(args[1]),
		name:          fmt.Sprintf("scatter-range-leader-%s", args[2]),
		filters:       filters,
	}
}

func (l *scatterRangeLeaderScheduler) GetName() string {
	return l.name
}

func (l *scatterRangeLeaderScheduler) GetType() string {
	return "scatter-range-leader"
}

func (l *scatterRangeLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.limiter.OperatorCount(schedule.OpRange) < 1
}

func (l *scatterRangeLeaderScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) []*schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	regions := core.NewRegionsInfo()
	startKey := l.startKey
	loopEnd := false
	for !loopEnd {
		collect := cluster.ScanRegions(startKey, scanLimit)
		if len(collect) == 0 {
			break
		}
		for _, r := range collect {
			if bytes.Compare(r.StartKey, l.endKey) < 0 {
				regions.SetRegion(r)
			} else {
				loopEnd = true
				break
			}
			if string(r.EndKey) == "" {
				loopEnd = true
				break
			}
			startKey = r.EndKey
		}
	}
	var (
		source *core.StoreInfo
		target *core.StoreInfo
	)
	maxScore := int64(math.MinInt32)
	minScore := int64(math.MaxInt32)
	for _, s := range stores {
		score := regions.GetStoreLeaderRegionSize(s.GetId())
		if score > maxScore {
			maxScore = score
			source = s
		}
	}
	for _, s := range stores {
		if schedule.FilterTarget(cluster, s, l.filters) {
			continue
		}
		scoreGuard := schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)
		if scoreGuard.FilterTarget(cluster, s) {
			continue
		}
		score := regions.GetStoreLeaderRegionSize(s.GetId())
		if score < minScore {
			minScore = score
			target = s
		}
	}

	if source == target {
		schedulerCounter.WithLabelValues(l.GetName(), "no_need").Inc()
		return nil
	}
	sourceRegion := regions.RandLeaderRegion(source.GetId())
	if sourceRegion == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_source_region").Inc()
		return nil
	}

	var steps []schedule.OperatorStep
	followers := sourceRegion.GetFollowers()
	for storeID := range followers {
		if l.shouldBalance(source.GetId(), storeID, sourceRegion, regions) {
			target = cluster.GetStore(storeID)
			break
		}
	}
	if _, ok := followers[target.GetId()]; ok {
		step := schedule.TransferLeader{FromStore: source.GetId(), ToStore: target.GetId()}
		op := schedule.NewOperator("scatter-range-leader", sourceRegion.GetId(), schedule.OpRange|schedule.OpLeader, step)
		return []*schedule.Operator{op}
	}
	peer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		if _, ok := followers[target.GetId()]; ok {
			step := schedule.TransferLeader{FromStore: source.GetId(), ToStore: target.GetId()}
			op := schedule.NewOperator("scatter-range-leader", sourceRegion.GetId(), schedule.OpRange|schedule.OpLeader, step)
			return []*schedule.Operator{op}
		}
		return nil
	}
	steps = append(steps, schedule.AddPeer{ToStore: target.GetId(), PeerID: peer.GetId()})
	steps = append(steps, schedule.TransferLeader{FromStore: source.GetId(), ToStore: target.GetId()})
	steps = append(steps, schedule.RemovePeer{FromStore: source.GetId()})
	op := schedule.NewOperator("scatter-range-leader", sourceRegion.GetId(), schedule.OpRange|schedule.OpLeader|schedule.OpBalance, steps...)
	return []*schedule.Operator{op}
}
func (l *scatterRangeLeaderScheduler) shouldBalance(sourceID, targetID uint64, sourceRegion *core.RegionInfo, regions *core.RegionsInfo) bool {
	// more smaller TolerantSizeRatio
	// use 2 in here
	tolerantSizeRatio := 2
	sourceScore, targetScore := regions.GetStoreLeaderRegionSize(sourceID), regions.GetStoreLeaderRegionSize(targetID)
	if sourceScore-targetScore < sourceRegion.ApproximateSize*int64(tolerantSizeRatio) {
		return false
	}
	return true
}
