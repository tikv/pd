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
	"sync"
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
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	checkSuspectRangesInterval = 100 * time.Millisecond
	// DefaultWaitingCacheSize is the default length of waiting list.
	DefaultWaitingCacheSize = 100000
	// For 1,024,000 regions, patrolScanRegionLimit is 1000, which is max(patrolScanRegionMinLimit, 1,024,000/patrolRegionPartition)
	// It takes about 10s to iterate 1,024,000 regions(with DefaultPatrolRegionInterval=10ms) where other steps are not considered.
	patrolScanRegionMinLimit = 128
	patrolRegionChanLen      = 1024
	patrolRegionPartition    = 1024
)

var (
	denyCheckersByLabelerCounter = labeler.LabelerEventCounter.WithLabelValues("checkers", "deny")
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	waitingListGauge  = regionListGauge.WithLabelValues("waiting_list")
	priorityListGauge = regionListGauge.WithLabelValues("priority_list")
)

// Controller is used to manage all checkers.
type Controller struct {
	ctx                 context.Context
	cluster             sche.CheckerCluster
	conf                config.CheckerConfigProvider
	opController        *operator.Controller
	learnerChecker      *LearnerChecker
	replicaChecker      *ReplicaChecker
	ruleChecker         *RuleChecker
	splitChecker        *SplitChecker
	mergeChecker        *MergeChecker
	jointStateChecker   *JointStateChecker
	priorityInspector   *PriorityInspector
	regionWaitingList   cache.Cache
	suspectRegions      *cache.TTLUint64 // suspectRegions are regions that may need fix
	suspectKeyRanges    *cache.TTLString // suspect key-range regions that may need fix
	patrolRegionContext *PatrolRegionContext
}

// NewController create a new Controller.
func NewController(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider, ruleManager *placement.RuleManager, labeler *labeler.RegionLabeler, opController *operator.Controller) *Controller {
	regionWaitingList := cache.NewDefaultCache(DefaultWaitingCacheSize)
	return &Controller{
		ctx:                 ctx,
		cluster:             cluster,
		conf:                conf,
		opController:        opController,
		learnerChecker:      NewLearnerChecker(cluster),
		replicaChecker:      NewReplicaChecker(cluster, conf, regionWaitingList),
		ruleChecker:         NewRuleChecker(ctx, cluster, ruleManager, regionWaitingList),
		splitChecker:        NewSplitChecker(cluster, ruleManager, labeler),
		mergeChecker:        NewMergeChecker(ctx, cluster, conf),
		jointStateChecker:   NewJointStateChecker(cluster),
		priorityInspector:   NewPriorityInspector(cluster, conf),
		regionWaitingList:   regionWaitingList,
		suspectRegions:      cache.NewIDTTL(ctx, time.Minute, 3*time.Minute),
		suspectKeyRanges:    cache.NewStringTTL(ctx, time.Minute, 3*time.Minute),
		patrolRegionContext: &PatrolRegionContext{},
	}
}

// PatrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *Controller) PatrolRegions() {
	c.patrolRegionContext.init(c.ctx, c.cluster)
	c.patrolRegionContext.startPatrolRegionWorkers(c)
	defer c.patrolRegionContext.stop()
	ticker := time.NewTicker(c.patrolRegionContext.getInterval())
	defer ticker.Stop()

	var (
		key     []byte
		regions []*core.RegionInfo
	)
	for {
		select {
		case <-ticker.C:
			c.updateTickerIfNeeded(ticker)
			c.updatePatrolWorkersIfNeeded()
			if c.cluster.IsSchedulingHalted() {
				for len(c.patrolRegionContext.regionChan) > 0 {
					<-c.patrolRegionContext.regionChan
				}
				log.Debug("skip patrol regions due to scheduling is halted")
				continue
			}

			// wait for the regionChan to be drained
			if len(c.patrolRegionContext.regionChan) > 0 {
				continue
			}

			// Check priority regions first.
			c.checkPriorityRegions()
			// Check suspect regions first.
			c.checkSuspectRegions()
			// Check regions in the waiting list
			c.checkWaitingRegions()

			key, regions = c.checkRegions(key)
			if len(regions) == 0 {
				continue
			}
			// Updates the label level isolation statistics.
			c.cluster.UpdateRegionsLabelLevelStats(regions)
			// Update metrics and scan limit if a full scan is done.
			if len(key) == 0 {
				c.patrolRegionContext.roundUpdateMetrics()
				c.patrolRegionContext.setScanLimit(calculateScanLimit(c.cluster))
			}
			failpoint.Inject("break-patrol", func() {
				time.Sleep(100 * time.Millisecond) // ensure the regions are handled by the workers
				failpoint.Return()
			})
		case <-c.ctx.Done():
			patrolCheckRegionsGauge.Set(0)
			c.patrolRegionContext.setPatrolRegionsDuration(0)
			log.Info("patrol regions has been stopped")
			return
		}
	}
}

func (c *Controller) updateTickerIfNeeded(ticker *time.Ticker) {
	// Note: we reset the ticker here to support updating configuration dynamically.
	newInterval := c.cluster.GetCheckerConfig().GetPatrolRegionInterval()
	if c.patrolRegionContext.getInterval() != newInterval {
		c.patrolRegionContext.setInterval(newInterval)
		ticker.Reset(newInterval)
		log.Info("checkers starts patrol regions with new interval", zap.Duration("interval", newInterval))
	}
}

func (c *Controller) updatePatrolWorkersIfNeeded() {
	newWorkersCount := c.cluster.GetCheckerConfig().GetPatrolRegionWorkerCount()
	if c.patrolRegionContext.getWorkerCount() != newWorkersCount {
		oldWorkersCount := c.patrolRegionContext.getWorkerCount()
		c.patrolRegionContext.setWorkerCount(newWorkersCount)
		// Stop the old workers and start the new workers.
		c.patrolRegionContext.workersCancel()
		c.patrolRegionContext.wg.Wait()
		c.patrolRegionContext.workersCtx, c.patrolRegionContext.workersCancel = context.WithCancel(c.ctx)
		c.patrolRegionContext.startPatrolRegionWorkers(c)
		log.Info("checkers starts patrol regions with new workers count",
			zap.Int("old-workers-count", oldWorkersCount),
			zap.Int("new-workers-count", newWorkersCount))
	}
}

// GetPatrolRegionsDuration returns the duration of the last patrol region round.
func (c *Controller) GetPatrolRegionsDuration() time.Duration {
	if c == nil {
		return 0
	}
	return c.patrolRegionContext.getPatrolRegionsDuration()
}

func (c *Controller) checkRegions(startKey []byte) (key []byte, regions []*core.RegionInfo) {
	regions = c.cluster.ScanRegions(startKey, nil, c.patrolRegionContext.getScanLimit())
	if len(regions) == 0 {
		// Resets the scan key.
		key = nil
		return
	}

	for _, region := range regions {
		c.patrolRegionContext.regionChan <- region
		key = region.GetEndKey()
	}
	return
}

func (c *Controller) checkSuspectRegions() {
	for _, id := range c.GetSuspectRegions() {
		region := c.cluster.GetRegion(id)
		c.patrolRegionContext.regionChan <- region
	}
}

func (c *Controller) checkWaitingRegions() {
	items := c.GetWaitingRegions()
	waitingListGauge.Set(float64(len(items)))
	for _, item := range items {
		region := c.cluster.GetRegion(item.Key)
		c.patrolRegionContext.regionChan <- region
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
				operator.OperatorLimitCounter.WithLabelValues(c.ruleChecker.GetType(), operator.OpReplica.String()).Inc()
				c.regionWaitingList.Put(region.GetID(), nil)
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
			operator.OperatorLimitCounter.WithLabelValues(c.replicaChecker.GetType(), operator.OpReplica.String()).Inc()
			c.regionWaitingList.Put(region.GetID(), nil)
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
		c.RemoveWaitingRegion(id)
		c.RemoveSuspectRegion(id)
		return
	}
	ops := c.CheckRegion(region)
	if len(ops) == 0 {
		return
	}

	if !c.opController.ExceedStoreLimit(ops...) {
		c.opController.AddWaitingOperator(ops...)
		c.RemoveWaitingRegion(id)
		c.RemoveSuspectRegion(id)
	} else {
		c.AddWaitingRegion(region)
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

// GetWaitingRegions returns the regions in the waiting list.
func (c *Controller) GetWaitingRegions() []*cache.Item {
	return c.regionWaitingList.Elems()
}

// AddWaitingRegion returns the regions in the waiting list.
func (c *Controller) AddWaitingRegion(region *core.RegionInfo) {
	c.regionWaitingList.Put(region.GetID(), nil)
}

// RemoveWaitingRegion removes the region from the waiting list.
func (c *Controller) RemoveWaitingRegion(id uint64) {
	c.regionWaitingList.Remove(id)
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
			log.Info("check suspect key ranges has been stopped")
			return
		case <-ticker.C:
			keyRange, success := c.PopOneSuspectKeyRange()
			if !success {
				continue
			}
			limit := 1024
			regions := c.cluster.ScanRegions(keyRange[0], keyRange[1], limit)
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
			c.AddSuspectRegions(regionIDList...)
		}
	}
}

// AddSuspectRegions adds regions to suspect list.
func (c *Controller) AddSuspectRegions(regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		c.suspectRegions.Put(regionID, nil)
	}
}

// GetSuspectRegions gets all suspect regions.
func (c *Controller) GetSuspectRegions() []uint64 {
	return c.suspectRegions.GetAllID()
}

// RemoveSuspectRegion removes region from suspect list.
func (c *Controller) RemoveSuspectRegion(id uint64) {
	c.suspectRegions.Remove(id)
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

// ClearSuspectRegions clears the suspect regions, only for unit test
func (c *Controller) ClearSuspectRegions() {
	c.suspectRegions.Clear()
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
	syncutil.RWMutex
	// config
	interval    time.Duration
	workerCount int
	scanLimit   int
	// status
	patrolRoundStartTime time.Time
	duration             time.Duration
	// workers
	workersCtx    context.Context
	workersCancel context.CancelFunc
	regionChan    chan *core.RegionInfo
	wg            sync.WaitGroup
}

func (p *PatrolRegionContext) init(ctx context.Context, cluster sche.CheckerCluster) {
	p.interval = cluster.GetCheckerConfig().GetPatrolRegionInterval()
	p.workerCount = cluster.GetCheckerConfig().GetPatrolRegionWorkerCount()

	p.scanLimit = calculateScanLimit(cluster)
	p.patrolRoundStartTime = time.Now()

	p.regionChan = make(chan *core.RegionInfo, patrolRegionChanLen)
	p.workersCtx, p.workersCancel = context.WithCancel(ctx)
}

func (p *PatrolRegionContext) stop() {
	close(p.regionChan)
	p.workersCancel()
	p.wg.Wait()
}

func (p *PatrolRegionContext) getWorkerCount() int {
	p.RLock()
	defer p.RUnlock()
	return p.workerCount
}

func (p *PatrolRegionContext) setWorkerCount(count int) {
	p.Lock()
	defer p.Unlock()
	p.workerCount = count
}

func (p *PatrolRegionContext) getInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()
	return p.interval
}

func (p *PatrolRegionContext) setInterval(interval time.Duration) {
	p.Lock()
	defer p.Unlock()
	p.interval = interval
}

func (p *PatrolRegionContext) getScanLimit() int {
	p.RLock()
	defer p.RUnlock()
	return p.scanLimit
}

func (p *PatrolRegionContext) setScanLimit(limit int) {
	p.Lock()
	defer p.Unlock()
	p.scanLimit = limit
}

func calculateScanLimit(cluster sche.CheckerCluster) int {
	return max(patrolScanRegionMinLimit, cluster.GetTotalRegionCount()/patrolRegionPartition)
}

func (p *PatrolRegionContext) getPatrolRegionsDuration() time.Duration {
	p.RLock()
	defer p.RUnlock()
	return p.duration
}

func (p *PatrolRegionContext) setPatrolRegionsDuration(dur time.Duration) {
	p.Lock()
	defer p.Unlock()
	p.duration = dur
}

func (p *PatrolRegionContext) startPatrolRegionWorkers(c *Controller) {
	for i := 0; i < p.getWorkerCount(); i++ {
		p.wg.Add(1)
		go func(i int) {
			defer logutil.LogPanic()
			defer p.wg.Done()
			for {
				select {
				case region, ok := <-p.regionChan:
					if !ok {
						log.Debug("region channel is closed", zap.Int("worker-id", i))
						return
					}
					c.tryAddOperators(region)
				case <-p.workersCtx.Done():
					log.Debug("region worker is closed", zap.Int("worker-id", i))
					return
				}
			}
		}(i)
	}
}

func (p *PatrolRegionContext) roundUpdateMetrics() {
	dur := time.Since(p.patrolRoundStartTime)
	patrolCheckRegionsGauge.Set(dur.Seconds())
	p.setPatrolRegionsDuration(dur)
	p.patrolRoundStartTime = time.Now()
}
