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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// defaultPriorityQueueSize is the default value of priority queue size.
const defaultPriorityQueueSize = 1280

// PriorityInspector ensures high priority region should run first.
type PriorityInspector struct {
	cluster sche.CheckerCluster
	conf    config.CheckerConfigProvider
	mu      struct {
		syncutil.RWMutex
		queue *cache.PriorityQueue
	}
}

// NewPriorityInspector creates a priority inspector.
func NewPriorityInspector(cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *PriorityInspector {
	res := &PriorityInspector{
		cluster: cluster,
		conf:    conf,
	}
	res.mu.queue = cache.NewPriorityQueue(defaultPriorityQueueSize)
	return res
}

// RegionPriorityEntry records region priority info.
type RegionPriorityEntry struct {
	Attempt  int
	Last     time.Time
	regionID uint64
}

// ID implements PriorityQueueItem interface.
func (r RegionPriorityEntry) ID() uint64 {
	return r.regionID
}

// NewRegionEntry constructs a region priority entry.
func NewRegionEntry(regionID uint64) *RegionPriorityEntry {
	return &RegionPriorityEntry{regionID: regionID, Last: time.Now(), Attempt: 1}
}

// Inspect inspects region's replicas, it will put into priority queue if the region lack of replicas.
func (p *PriorityInspector) Inspect(region *core.RegionInfo) (fit *placement.RegionFit) {
	var makeupCount int
	if p.conf.IsPlacementRulesEnabled() {
		makeupCount, fit = p.inspectRegionInPlacementRule(region)
	} else {
		makeupCount = p.inspectRegionInReplica(region)
	}
	priority := 0 - makeupCount
	p.addOrRemoveRegion(priority, region.GetID())
	return
}

// inspectRegionInPlacementRule inspects region in placement rule mode.
func (p *PriorityInspector) inspectRegionInPlacementRule(region *core.RegionInfo) (makeupCount int, fit *placement.RegionFit) {
	fit = p.cluster.GetRuleManager().FitRegion(p.cluster, region)
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

// inspectReplicas inspects region in replica mode.
func (p *PriorityInspector) inspectRegionInReplica(region *core.RegionInfo) (makeupCount int) {
	return p.conf.GetMaxReplicas() - len(region.GetPeers())
}

// addOrRemoveRegion add or remove region from queue.
// It will remove if region's priority equal 0.
// It's Attempt will increase if region's priority equal last.
func (p *PriorityInspector) addOrRemoveRegion(priority int, regionID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if priority < 0 {
		if entry := p.mu.queue.Get(regionID); entry != nil && entry.Priority == priority {
			e := entry.Value.(*RegionPriorityEntry)
			e.Attempt++
			e.Last = time.Now()
			p.mu.queue.Put(priority, e)
		} else {
			entry := NewRegionEntry(regionID)
			p.mu.queue.Put(priority, entry)
		}
	} else {
		p.mu.queue.Remove(regionID)
	}
}

// GetPriorityRegions returns all regions in priority queue that needs rerun.
func (p *PriorityInspector) GetPriorityRegions() (ids []uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entries := p.mu.queue.Elems()
	for _, e := range entries {
		re := e.Value.(*RegionPriorityEntry)
		// avoid to some priority region occupy checker, region don't need check on next check interval
		// the next run time is : last_time+retry*10*patrol_region_interval
		if t := re.Last.Add(time.Duration(re.Attempt*10) * p.conf.GetPatrolRegionInterval()); t.Before(time.Now()) {
			ids = append(ids, re.regionID)
		}
	}
	return
}

// RemovePriorityRegion removes priority region from priority queue.
func (p *PriorityInspector) RemovePriorityRegion(regionID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.queue.Remove(regionID)
}

// getQueueLen returns the length of priority queue.
// it's only used for test.
func (p *PriorityInspector) getQueueLen() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mu.queue.Len()
}
