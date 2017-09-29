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
	"math"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/montanaflynn/stats"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

// options for interval of schedulers
const (
	MaxScheduleInterval     = time.Minute
	MinScheduleInterval     = time.Millisecond * 10
	MinSlowScheduleInterval = time.Second * 3

	ScheduleIntervalFactor = 1.3
)

// scheduleTransferLeader schedules a region to transfer leader to the peer.
func scheduleTransferLeader(cluster schedule.Cluster, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()
	if len(stores) == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	var averageLeader float64
	for _, s := range stores {
		averageLeader += float64(s.LeaderScore()) / float64(len(stores))
	}

	mostLeaderStore := s.SelectSource(stores, filters...)
	leastLeaderStore := s.SelectTarget(stores, filters...)

	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderStore != nil {
		mostLeaderDistance = math.Abs(mostLeaderStore.LeaderScore() - averageLeader)
	}
	if leastLeaderStore != nil {
		leastLeaderDistance = math.Abs(leastLeaderStore.LeaderScore() - averageLeader)
	}
	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "already_balanced").Inc()
		return nil, nil
	}

	if mostLeaderDistance > leastLeaderDistance {
		// Transfer a leader out of mostLeaderStore.
		region := cluster.RandLeaderRegion(mostLeaderStore.GetId())
		if region == nil {
			schedulerCounter.WithLabelValues(schedulerName, "no_leader_region").Inc()
			return nil, nil
		}
		targetStores := cluster.GetFollowerStores(region)
		target := s.SelectTarget(targetStores)
		if target == nil {
			schedulerCounter.WithLabelValues(schedulerName, "no_target_store").Inc()
			return nil, nil
		}

		return region, region.GetStorePeer(target.GetId())
	}

	// Transfer a leader into leastLeaderStore.
	region := cluster.RandFollowerRegion(leastLeaderStore.GetId())
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_target_peer").Inc()
		return nil, nil
	}
	return region, region.GetStorePeer(leastLeaderStore.GetId())
}

// scheduleRemovePeer schedules a region to remove the peer.
func scheduleRemovePeer(cluster schedule.Cluster, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()

	source := s.SelectSource(stores, filters...)
	if source == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	region := cluster.RandFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.RandLeaderRegion(source.GetId())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}

// scheduleAddPeer schedules a new peer.
func scheduleAddPeer(cluster schedule.Cluster, s schedule.Selector, filters ...schedule.Filter) *metapb.Peer {
	stores := cluster.GetStores()

	target := s.SelectTarget(stores, filters...)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newPeer
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

const (
	bootstrapBalanceCount = 10
	bootstrapBalanceDiff  = 2
)

// minBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func minBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}

// shouldBalance returns true if we should balance the source and target store.
// The min balance diff provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
func shouldBalance(source, target *core.StoreInfo, kind core.ResourceKind) bool {
	sourceCount := source.ResourceCount(kind)
	sourceScore := source.ResourceScore(kind)
	targetScore := target.ResourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}
	diffRatio := 1 - targetScore/sourceScore
	diffCount := diffRatio * float64(sourceCount)
	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(cluster schedule.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

type intervalGrowthType int

const (
	exponentailGrowth intervalGrowthType = iota
	linearGrowth
	zeroGrowth
)

func intervalGrow(x time.Duration, maxInterval time.Duration, typ intervalGrowthType) time.Duration {
	switch typ {
	case exponentailGrowth:
		return minDuration(time.Duration(float64(x)*ScheduleIntervalFactor), maxInterval)
	case linearGrowth:
		return minDuration(x+MinSlowScheduleInterval, maxInterval)
	case zeroGrowth:
		return x
	default:
		log.Fatal("unKnow interval growth type")
	}
	return 0
}
