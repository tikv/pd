// Copyright 2019 TiKV Project Authors.
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
	"bytes"
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	checkSuspectRangesInterval = 100 * time.Millisecond
	// DefaultPendingRegionCacheSize is the default length of waiting list.
	DefaultPendingRegionCacheSize = 100000
	// It takes about 1.3 minutes(1000000/128*10/60/1000) to iterate 1 million regions(with DefaultPatrolRegionInterval=10ms).
	patrolScanRegionLimit = 128
	suspectRegionLimit    = 1024
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	pendingProcessedRegionsGauge = regionListGauge.WithLabelValues("pending_processed_regions")
	priorityListGauge            = regionListGauge.WithLabelValues("priority_list")
	denyCheckersByLabelerCounter = labeler.LabelerEventCounter.WithLabelValues("checkers", "deny")
)

// Controller is used to manage all checkers.
type Controller struct {
	ctx                     context.Context
	cluster                 sche.CheckerCluster
	conf                    config.CheckerConfigProvider
	opController            *operator.Controller
	learnerChecker          *LearnerChecker
	replicaChecker          *ReplicaChecker
	ruleChecker             *RuleChecker
	splitChecker            *SplitChecker
	mergeChecker            *MergeChecker
	jointStateChecker       *JointStateChecker
	priorityInspector       *PriorityInspector
	pendingProcessedRegions cache.Cache
	suspectKeyRanges        *cache.TTLString // suspect key-range regions that may need fix
	patrolRegionContext     *PatrolRegionContext
}

// NewController create a new Controller.
func NewController(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider, ruleManager *placement.RuleManager, labeler *labeler.RegionLabeler, opController *operator.Controller) *Controller {
	pendingProcessedRegions := cache.NewDefaultCache(DefaultPendingRegionCacheSize)
	return &Controller{
		ctx:                     ctx,
		cluster:                 cluster,
		conf:                    conf,
		opController:            opController,
		learnerChecker:          NewLearnerChecker(cluster),
		replicaChecker:          NewReplicaChecker(cluster, conf, pendingProcessedRegions),
		ruleChecker:             NewRuleChecker(ctx, cluster, ruleManager, pendingProcessedRegions),
		splitChecker:            NewSplitChecker(cluster, ruleManager, labeler),
		mergeChecker:            NewMergeChecker(ctx, cluster, conf),
		jointStateChecker:       NewJointStateChecker(cluster),
		priorityInspector:       NewPriorityInspector(cluster, conf),
		pendingProcessedRegions: pendingProcessedRegions,
		suspectKeyRanges:        cache.NewStringTTL(ctx, time.Minute, 3*time.Minute),
		patrolRegionContext:     &PatrolRegionContext{},
	}
}

// PatrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *Controller) PatrolRegions() {
	c.patrolRegionContext.init(c.cluster)
	defer c.patrolRegionContext.stop()
	var (
		key     []byte
		regions []*core.RegionInfo
	)
	for {
		select {
		case <-c.patrolRegionContext.ticker.C:
			c.patrolRegionContext.updateTickerIfNeeded()
		case <-c.ctx.Done():
			patrolCheckRegionsGauge.Set(0)
			c.patrolRegionContext.setPatrolRegionsDuration(0)
			return
		}
		if c.cluster.IsSchedulingHalted() {
			continue
		}

		// Check priority regions first.
		c.checkPriorityRegions()
		// Check pending processed regions first.
		c.checkPendingProcessedRegions()

		key, regions = c.checkRegions(key)
		if len(regions) == 0 {
			continue
		}
		// Updates the label level isolation statistics.
		c.cluster.UpdateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			c.patrolRegionContext.roundUpdateMetrics()
		}
		failpoint.Inject("breakPatrol", func() {
			failpoint.Break()
		})
	}
}

// GetPatrolRegionsDuration returns the duration of the last patrol region round.
func (c *Controller) GetPatrolRegionsDuration() time.Duration {
	return c.patrolRegionContext.getPatrolRegionsDuration()
}

func (c *Controller) checkRegions(startKey []byte) (key []byte, regions []*core.RegionInfo) {
	regions = c.cluster.ScanRegions(startKey, nil, patrolScanRegionLimit)
	if len(regions) == 0 {
		// Resets the scan key.
		key = nil
		return
	}

	for _, region := range regions {
		c.tryAddOperators(region)
		key = region.GetEndKey()
	}
	return
}

func (c *Controller) checkPendingProcessedRegions() {
	ids := c.GetPendingProcessedRegions()
	pendingProcessedRegionsGauge.Set(float64(len(ids)))
	for _, id := range ids {
		region := c.cluster.GetRegion(id)
		c.tryAddOperators(region)
	}
}

// checkPriorityRegions checks priority regions
func (c *Controller) checkPriorityRegions() {
	items := c.GetPriorityRegions()
	removes := make([]uint64, 0)
	priorityListGauge.Set(float64(len(items)))
	for _, id := range items {
		region := c.cluster.GetRegion(id)
		if region == nil {
			removes = append(removes, id)
			continue
		}
		ops := c.CheckRegion(region)
		// it should skip if region needs to merge
		if len(ops) == 0 || ops[0].Kind()&operator.OpMerge != 0 {
			continue
		}
		if !c.opController.ExceedStoreLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
		}
	}
	for _, v := range removes {
		c.RemovePriorityRegions(v)
	}
}

// CheckRegion will check the region and add a new operator if needed.
// The function is exposed for test purpose.
func (c *Controller) CheckRegion(region *core.RegionInfo) []*operator.Operator {
	// If PD has restarted, it needs to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController

	if op := c.jointStateChecker.Check(region); op != nil {
		return []*operator.Operator{op}
	}

	if op := c.splitChecker.Check(region); op != nil {
		return []*operator.Operator{op}
	}

	if c.conf.IsPlacementRulesEnabled() {
		skipRuleCheck := c.cluster.GetCheckerConfig().IsPlacementRulesCacheEnabled() &&
			c.cluster.GetRuleManager().IsRegionFitCached(c.cluster, region)
		if skipRuleCheck {
			// If the fit is fetched from cache, it seems that the region doesn't need check
			failpoint.Inject("assertShouldNotCache", func() {
				panic("cached shouldn't be used")
			})
			ruleCheckerGetCacheCounter.Inc()
		} else {
			failpoint.Inject("assertShouldCache", func() {
				panic("cached should be used")
			})
			fit := c.priorityInspector.Inspect(region)
			if op := c.ruleChecker.CheckWithFit(region, fit); op != nil {
				if opController.OperatorCount(operator.OpReplica) < c.conf.GetReplicaScheduleLimit() {
					return []*operator.Operator{op}
				}
				operator.OperatorLimitCounter.WithLabelValues(c.ruleChecker.Name(), operator.OpReplica.String()).Inc()
				c.pendingProcessedRegions.Put(region.GetID(), nil)
			}
		}
	} else {
		if op := c.learnerChecker.Check(region); op != nil {
			return []*operator.Operator{op}
		}
		if op := c.replicaChecker.Check(region); op != nil {
			if opController.OperatorCount(operator.OpReplica) < c.conf.GetReplicaScheduleLimit() {
				return []*operator.Operator{op}
			}
			operator.OperatorLimitCounter.WithLabelValues(c.replicaChecker.Name(), operator.OpReplica.String()).Inc()
			c.pendingProcessedRegions.Put(region.GetID(), nil)
		}
	}
	// skip the joint checker, split checker and rule checker when region label is set to "schedule=deny".
	// those checkers are help to make region health, it's necessary to skip them when region is set to deny.
	if cl, ok := c.cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler }); ok {
		l := cl.GetRegionLabeler()
		if l.ScheduleDisabled(region) {
			denyCheckersByLabelerCounter.Inc()
			return nil
		}
	}

	if c.mergeChecker != nil {
		allowed := opController.OperatorCount(operator.OpMerge) < c.conf.GetMergeScheduleLimit()
		if !allowed {
			operator.OperatorLimitCounter.WithLabelValues(c.mergeChecker.GetType(), operator.OpMerge.String()).Inc()
		} else if ops := c.mergeChecker.Check(region); ops != nil {
			// It makes sure that two operators can be added successfully altogether.
			return ops
		}
	}
	return nil
}

func (c *Controller) tryAddOperators(region *core.RegionInfo) {
	if region == nil {
		// the region could be recent split, continue to wait.
		return
	}
	id := region.GetID()
	if c.opController.GetOperator(id) != nil {
		c.RemovePendingProcessedRegion(id)
		return
	}
	ops := c.CheckRegion(region)
	if len(ops) == 0 {
		return
	}

	if !c.opController.ExceedStoreLimit(ops...) {
		c.opController.AddWaitingOperator(ops...)
		c.RemovePendingProcessedRegion(id)
	} else {
		c.AddPendingProcessedRegions(id)
	}
}

// GetMergeChecker returns the merge checker.
func (c *Controller) GetMergeChecker() *MergeChecker {
	return c.mergeChecker
}

// GetRuleChecker returns the rule checker.
func (c *Controller) GetRuleChecker() *RuleChecker {
	return c.ruleChecker
}

// GetPendingProcessedRegions returns the pending processed regions in the cache.
func (c *Controller) GetPendingProcessedRegions() []uint64 {
	pendingRegions := make([]uint64, 0)
	for _, item := range c.pendingProcessedRegions.Elems() {
		pendingRegions = append(pendingRegions, item.Key)
	}
	return pendingRegions
}

// AddPendingProcessedRegions adds the pending processed region into the cache.
func (c *Controller) AddPendingProcessedRegions(ids ...uint64) {
	for _, id := range ids {
		c.pendingProcessedRegions.Put(id, nil)
	}
}

// RemovePendingProcessedRegion removes the pending processed region from the cache.
func (c *Controller) RemovePendingProcessedRegion(id uint64) {
	c.pendingProcessedRegions.Remove(id)
}

// GetPriorityRegions returns the region in priority queue
func (c *Controller) GetPriorityRegions() []uint64 {
	return c.priorityInspector.GetPriorityRegions()
}

// RemovePriorityRegions removes priority region from priority queue
func (c *Controller) RemovePriorityRegions(id uint64) {
	c.priorityInspector.RemovePriorityRegion(id)
}

// CheckSuspectRanges would pop one suspect key range group
// The regions of new version key range and old version key range would be placed into
// the suspect regions map
func (c *Controller) CheckSuspectRanges() {
	ticker := time.NewTicker(checkSuspectRangesInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			keyRange, success := c.PopOneSuspectKeyRange()
			if !success {
				continue
			}
			regions := c.cluster.ScanRegions(keyRange[0], keyRange[1], suspectRegionLimit)
			if len(regions) == 0 {
				continue
			}
			regionIDList := make([]uint64, 0, len(regions))
			for _, region := range regions {
				regionIDList = append(regionIDList, region.GetID())
			}
			// if the last region's end key is smaller the keyRange[1] which means there existed the remaining regions between
			// keyRange[0] and keyRange[1] after scan regions, so we put the end key and keyRange[1] into Suspect KeyRanges
			lastRegion := regions[len(regions)-1]
			if lastRegion.GetEndKey() != nil && bytes.Compare(lastRegion.GetEndKey(), keyRange[1]) < 0 {
				c.AddSuspectKeyRange(lastRegion.GetEndKey(), keyRange[1])
			}
			c.AddPendingProcessedRegions(regionIDList...)
		}
	}
}

// AddSuspectKeyRange adds the key range with the its ruleID as the key
// The instance of each keyRange is like following format:
// [2][]byte: start key/end key
func (c *Controller) AddSuspectKeyRange(start, end []byte) {
	c.suspectKeyRanges.Put(keyutil.BuildKeyRangeKey(start, end), [2][]byte{start, end})
}

// PopOneSuspectKeyRange gets one suspect keyRange group.
// it would return value and true if pop success, or return empty [][2][]byte and false
// if suspectKeyRanges couldn't pop keyRange group.
func (c *Controller) PopOneSuspectKeyRange() ([2][]byte, bool) {
	_, value, success := c.suspectKeyRanges.Pop()
	if !success {
		return [2][]byte{}, false
	}
	v, ok := value.([2][]byte)
	if !ok {
		return [2][]byte{}, false
	}
	return v, true
}

// ClearSuspectKeyRanges clears the suspect keyRanges, only for unit test
func (c *Controller) ClearSuspectKeyRanges() {
	c.suspectKeyRanges.Clear()
}

// IsPendingRegion returns true if the given region is in the pending list.
func (c *Controller) IsPendingRegion(regionID uint64) bool {
	_, exist := c.ruleChecker.pendingList.Get(regionID)
	return exist
}

// GetPauseController returns pause controller of the checker
func (c *Controller) GetPauseController(name string) (*PauseController, error) {
	switch name {
	case "learner":
		return &c.learnerChecker.PauseController, nil
	case "replica":
		return &c.replicaChecker.PauseController, nil
	case "rule":
		return &c.ruleChecker.PauseController, nil
	case "split":
		return &c.splitChecker.PauseController, nil
	case "merge":
		return &c.mergeChecker.PauseController, nil
	case "joint-state":
		return &c.jointStateChecker.PauseController, nil
	default:
		return nil, errs.ErrCheckerNotFound.FastGenByArgs()
	}
}

// PatrolRegionContext is used to store the context of patrol regions.
type PatrolRegionContext struct {
	cluster sche.CheckerCluster
	ticker  *time.Ticker
	// config
	interval time.Duration
	// status
	patrolRoundStartTime time.Time
	mu                   struct {
		syncutil.RWMutex
		duration time.Duration
	}
}

func (p *PatrolRegionContext) init(cluster sche.CheckerCluster) {
	p.cluster = cluster
	p.interval = cluster.GetCheckerConfig().GetPatrolRegionInterval()
	p.patrolRoundStartTime = time.Now()
	p.ticker = time.NewTicker(p.interval)
}

func (p *PatrolRegionContext) stop() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
}

func (p *PatrolRegionContext) updateTickerIfNeeded() {
	// Note: we reset the ticker here to support updating configuration dynamically.
	newInterval := p.cluster.GetCheckerConfig().GetPatrolRegionInterval()
	if p.interval != newInterval {
		p.interval = newInterval
		p.ticker.Reset(newInterval)
		log.Info("checkers starts patrol regions with new interval", zap.Duration("interval", newInterval))
	}
}

func (p *PatrolRegionContext) getPatrolRegionsDuration() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mu.duration
}

func (p *PatrolRegionContext) setPatrolRegionsDuration(dur time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.duration = dur
}

func (p *PatrolRegionContext) roundUpdateMetrics() {
	dur := time.Since(p.patrolRoundStartTime)
	patrolCheckRegionsGauge.Set(dur.Seconds())
	p.setPatrolRegionsDuration(dur)
	p.patrolRoundStartTime = time.Now()
}
