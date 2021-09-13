// Copyright 2021 TiKV Project Authors.
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

package checker

import (
	"time"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
)

// the default value of priority queue size
const defaultPriorityQueueSize = 1280

// PriorityChecker ensures high priority region should run first
type PriorityChecker struct {
	cluster opt.Cluster
	opts    *config.PersistOptions
	queue   *cache.PriorityQueue
}

// NewPriorityChecker creates a priority checker.
func NewPriorityChecker(cluster opt.Cluster) *PriorityChecker {
	return &PriorityChecker{
		cluster: cluster,
		opts:    cluster.GetOpts(),
		queue:   cache.NewPriorityQueue(defaultPriorityQueueSize),
	}
}

// GetType returns PriorityChecker's type
func (p *PriorityChecker) GetType() string {
	return "priority-checker"
}

// RegionPriorityEntry records region priority info
type RegionPriorityEntry struct {
	Attempt  int
	Last     time.Time
	regionID uint64
}

// ID implement PriorityQueueItem interface
func (r RegionPriorityEntry) ID() uint64 {
	return r.regionID
}

// NewRegionEntry construct of region priority entry
func NewRegionEntry(regionID uint64) *RegionPriorityEntry {
	return &RegionPriorityEntry{regionID: regionID, Last: time.Now(), Attempt: 1}
}

// Check check region's replicas, it will put into priority queue if the region lack of replicas.
func (p *PriorityChecker) Check(region *core.RegionInfo) (fit *placement.RegionFit) {
	var makeupCount int
	if p.opts.IsPlacementRulesEnabled() {
		makeupCount, fit = p.checkRegionInPlacementRule(region)
	} else {
		makeupCount = p.checkRegionInReplica(region)
	}
	priority := 0 - makeupCount
	p.addOrRemoveRegion(priority, region.GetID())
	return
}

// checkRegionInPlacementRule check region in placement rule mode
func (p *PriorityChecker) checkRegionInPlacementRule(region *core.RegionInfo) (makeupCount int, fit *placement.RegionFit) {
	fit = opt.FitRegion(p.cluster, region)
	if len(fit.RuleFits) == 0 {
		return
	}

	for _, rf := range fit.RuleFits {
		// skip learn rule
		if rf.Rule.Role == placement.Learner {
			continue
		}
		makeupCount = makeupCount + rf.Rule.Count - len(rf.Peers)
	}
	return
}

// checkReplicas check region in replica mode
func (p *PriorityChecker) checkRegionInReplica(region *core.RegionInfo) (makeupCount int) {
	return p.opts.GetMaxReplicas() - len(region.GetPeers())
}

// addOrRemoveRegion add or remove region from  queue
// it will remove if region's priority equal 0
// it's Attempt will increase if region's priority equal last
func (p *PriorityChecker) addOrRemoveRegion(priority int, regionID uint64) {
	if priority < 0 {
		if entry := p.queue.Get(regionID); entry != nil && entry.Priority == priority {
			e := entry.Value.(*RegionPriorityEntry)
			e.Attempt = e.Attempt + 1
			e.Last = time.Now()
		}
		entry := NewRegionEntry(regionID)
		p.queue.Put(priority, entry)
	} else {
		p.queue.Remove(regionID)
	}
}

// GetPriorityRegions returns all regions in priority queue that needs rerun
func (p *PriorityChecker) GetPriorityRegions() (ids []uint64) {
	entries := p.queue.Elems()
	for _, e := range entries {
		re := e.Value.(*RegionPriorityEntry)
		// avoid to some priority region occupy checker, region don't need check on next check interval
		// the next run time is : last_time+retry*10*patrol_region_interval
		if t := re.Last.Add(time.Duration(re.Attempt*10) * p.opts.GetPatrolRegionInterval()); t.Before(time.Now()) {
			ids = append(ids, re.regionID)
		}
	}
	return
}

// RemovePriorityRegion removes priority region from priority queue
func (p *PriorityChecker) RemovePriorityRegion(regionID uint64) {
	p.queue.Remove(regionID)
}
