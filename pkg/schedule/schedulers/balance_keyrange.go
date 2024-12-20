// Copyright 2024 TiKV Project Authors.
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

package schedulers

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"go.uber.org/zap"
)

type StoreRegionSet struct {
	ID   uint64
	Info *metapb.Store
	// If the region still exists durgin migration
	RegionIDSet  map[uint64]bool
	OriginalPeer map[uint64]*metapb.Peer
}

type MigrationOp struct {
	FromStore    uint64          `json:"from_store"`
	ToStore      uint64          `json:"to_store"`
	ToStoreInfo  *metapb.Store   `json:"to_store_info"`
	OriginalPeer *metapb.Peer    `json:"original_peer"`
	Regions      map[uint64]bool `json:"regions"`
}

func pickRegions(n int, fromStore *StoreRegionSet, toStore *StoreRegionSet) *MigrationOp {
	o := MigrationOp{
		FromStore:   fromStore.ID,
		ToStore:     toStore.ID,
		ToStoreInfo: toStore.Info,
		Regions:     make(map[uint64]bool),
	}

	for i := 0; i < n; i++ {
		for r, fromHasRegion := range fromStore.RegionIDSet {
			if !fromHasRegion {
				continue
			}
			if toHasRegion, exist := toStore.RegionIDSet[r]; !exist || !toHasRegion {
				// If toStore doesn't has this region, then create a move op.
				o.Regions[r] = false
				o.OriginalPeer = fromStore.OriginalPeer[r]
				toStore.RegionIDSet[r] = true
				fromStore.RegionIDSet[r] = false
				break
			}
		}
	}
	return &o
}

func buildMigrationPlan(stores []*StoreRegionSet) ([]int, []int, []*MigrationOp, int) {
	totalPeersCount := 0
	if len(stores) == 0 {
		log.Info("no stores for migration")
		return []int{}, []int{}, []*MigrationOp{}, 0
	}
	for _, store := range stores {
		totalPeersCount += len(store.RegionIDSet)
	}
	for _, store := range stores {
		percentage := 100 * float64(len(store.RegionIDSet)) / float64(totalPeersCount)
		log.Info("!!! store region dist",
			zap.Uint64("store-id", store.ID),
			zap.Int("num-region", len(store.RegionIDSet)),
			zap.String("percentage", fmt.Sprintf("%.2f%%", percentage)))
	}
	avr := totalPeersCount / len(stores)
	remainder := totalPeersCount % len(stores)
	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(stores, func(lhs, rhs *StoreRegionSet) int {
		return -cmp.Compare(len(lhs.RegionIDSet), len(rhs.RegionIDSet))
	})
	expectedCount := []int{}
	for i := 0; i < remainder; i++ {
		expectedCount = append(expectedCount, avr+1)
	}
	for i := remainder; i < len(stores); i++ {
		expectedCount = append(expectedCount, avr)
	}

	senders := []int{}
	receivers := []int{}
	sendersVolume := []int{}
	receiversVolume := []int{}
	for i, store := range stores {
		if len(store.RegionIDSet) < expectedCount[i] {
			receivers = append(receivers, i)
			receiversVolume = append(receiversVolume, expectedCount[i]-len(store.RegionIDSet))
		}
		if len(store.RegionIDSet) > expectedCount[i] {
			senders = append(senders, i)
			sendersVolume = append(sendersVolume, len(store.RegionIDSet)-expectedCount[i])
		}
	}

	ops := []*MigrationOp{}
	movements := 0
	for i, senderIndex := range senders {
		fromStore := stores[senderIndex]
		for {
			if sendersVolume[i] <= 0 {
				break
			}
			for j, receiverIndex := range receivers {
				toStore := stores[receiverIndex]
				if receiversVolume[j] > 0 {
					n := sendersVolume[i]
					if n > receiversVolume[j] {
						n = receiversVolume[j]
					}
					receiversVolume[j] -= n
					sendersVolume[i] -= n
					op := pickRegions(n, fromStore, toStore)
					movements += len(op.Regions)
					ops = append(ops, op)
				}
			}
		}
	}

	return senders, receivers, ops, movements
}

type OperatorWrapper struct {
	Operator  *operator.Operator `json:"operators"`
	Region    *core.RegionInfo
	FromStore uint64
	Peer      *metapb.Peer
}
type MigrationPlan struct {
	ErrorCode  uint64             `json:"error_code"`
	StartKey   []byte             `json:"start_key"`
	EndKey     []byte             `json:"end_key"`
	Ops        []*MigrationOp     `json:"ops"`
	Operators  []*OperatorWrapper `json:"operators"`
	Running    []*OperatorWrapper `json:"running"`
	TotalCount int                `json:"total"`
	StartTime  time.Time
}

func computeCandidateStores(requiredLabels []*metapb.StoreLabel, stores []*metapb.Store, regions []*core.RegionInfo) []*StoreRegionSet {
	candidates := make([]*StoreRegionSet, 0)
	for _, s := range stores {
		storeLabelMap := make(map[string]*metapb.StoreLabel)
		for _, l := range s.GetLabels() {
			storeLabelMap[l.Key] = l
		}
		gotLabels := true
		for _, larg := range requiredLabels {
			if l, ok := storeLabelMap[larg.Key]; ok {
				if larg.Value != l.Value {
					gotLabels = false
					break
				}
			} else {
				gotLabels = false
				break
			}
		}

		if !gotLabels {
			continue
		}
		candidate := &StoreRegionSet{
			ID:           s.GetId(),
			Info:         s,
			RegionIDSet:  make(map[uint64]bool),
			OriginalPeer: make(map[uint64]*metapb.Peer),
		}
		for _, r := range regions {
			for _, p := range r.GetPeers() {
				if p.StoreId == s.GetId() {
					candidate.RegionIDSet[r.GetID()] = true
					candidate.OriginalPeer[r.GetID()] = p
				}
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func buildErrorMigrationPlan() *MigrationPlan {
	return &MigrationPlan{ErrorCode: 1, Ops: nil, Operators: nil}
}

// RedistibuteRegions checks if regions are imbalanced and rebalance them.
func RedistibuteRegions(c sche.SchedulerCluster, startKey, endKey []byte, requiredLabels []*metapb.StoreLabel) (*MigrationPlan, error) {
	if c == nil {
		return buildErrorMigrationPlan(), errs.ErrNotBootstrapped.GenWithStackByArgs()
	}

	regions := c.ScanRegions(startKey, endKey, -1)
	regionIDMap := make(map[uint64]*core.RegionInfo)
	for _, r := range regions {
		regionIDMap[r.GetID()] = r
	}

	stores := make([]*metapb.Store, 0)
	for _, s := range c.GetStores() {
		stores = append(stores, s.GetMeta())
	}
	candidates := computeCandidateStores(requiredLabels, stores, regions)

	senders, receivers, ops, movements := buildMigrationPlan(candidates)

	log.Info("balance keyrange plan details", zap.Any("startKey", startKey), zap.Any("endKey", endKey), zap.Any("senders", senders), zap.Any("receivers", receivers), zap.Any("movements", movements), zap.Any("ops", ops), zap.Any("stores", stores), zap.Any("candidates", len(candidates)))

	operators := make([]*OperatorWrapper, 0)
	for _, op := range ops {
		for rid := range op.Regions {
			newPeer := &metapb.Peer{StoreId: op.ToStore, Role: op.OriginalPeer.Role, IsWitness: op.OriginalPeer.IsWitness}
			log.Debug("Create balace region op", zap.Uint64("from", op.FromStore), zap.Uint64("to", op.ToStore), zap.Uint64("region_id", rid))
			o, err := operator.CreateMovePeerOperator("balance-keyrange", c, regionIDMap[rid], operator.OpReplica, op.FromStore, newPeer)
			if err != nil {
				log.Info("Failed to create operator", zap.Any("startKey", startKey), zap.Any("endKey", endKey), zap.Any("op", o), zap.Error(err))
				return buildErrorMigrationPlan(), err
			}
			operators = append(operators, &OperatorWrapper{
				Operator:  o,
				Region:    regionIDMap[rid],
				FromStore: op.FromStore,
				Peer:      newPeer,
			})
		}
	}

	return &MigrationPlan{
		ErrorCode:  0,
		StartKey:   startKey,
		EndKey:     endKey,
		Ops:        ops,
		Operators:  operators,
		Running:    make([]*OperatorWrapper, 0),
		TotalCount: len(operators),
	}, nil
}

type balanceKeyrangeSchedulerConfig struct {
	baseDefaultSchedulerConfig

	Range             core.KeyRange
	RequiredLabels    []*metapb.StoreLabel
	BatchSize         uint64
	MaxRunMillis      int64
	removeSchedulerCb func(string) error
}

type balanceKeyrangeScheduler struct {
	*BaseScheduler
	*retryQuota
	mu            sync.Mutex // Protect migrationPlan
	name          string
	conf          *balanceKeyrangeSchedulerConfig
	migrationPlan *MigrationPlan
}

// newBalanceKeyrangeScheduler creates a scheduler that tends to keep key-range on
// each store balanced.
func newBalanceKeyrangeScheduler(opController *operator.Controller, conf *balanceKeyrangeSchedulerConfig, opts ...BalanceKeyrangeCreateOption) Scheduler {
	scheduler := &balanceKeyrangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceKeyrangeScheduler, conf),
		retryQuota:    newRetryQuota(),
		name:          types.BalanceKeyrangeScheduler.String(),
		conf:          conf,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	return scheduler
}

// BalanceKeyrangeCreateOption is used to create a scheduler with an option.
type BalanceKeyrangeCreateOption func(s *balanceKeyrangeScheduler)

// WithBalanceKeyrangeName sets the name for the scheduler.
func WithBalanceKeyrangeName(name string) BalanceKeyrangeCreateOption {
	return func(s *balanceKeyrangeScheduler) {
		s.name = name
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *balanceKeyrangeScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *balanceKeyrangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpKeyrange) < 1
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpKeyrange)
	}
	return allowed
}

// IsFinished is true if the former schedule is finished, or there is no former schedule at all.
func (s *balanceKeyrangeScheduler) IsFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.migrationPlan == nil || (len(s.migrationPlan.Operators) == 0 && len(s.migrationPlan.Running) == 0)
}

// IsTimeout is true if the schedule took too much time and needs to be canceled.
func (s *balanceKeyrangeScheduler) IsTimeout() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.migrationPlan == nil {
		// Should not be called in this case, however, could be considered as a timeout case, and then delete the scheduler.
		return true
	} else {
		return time.Since(s.migrationPlan.StartTime).Milliseconds() > s.conf.MaxRunMillis
	}
}

// Generate a json that returns scheduling status.
func (s *balanceKeyrangeScheduler) GetStatus() any {
	s.mu.Lock()
	defer s.mu.Unlock()

	scheduling := false
	running := make([]*OperatorWrapper, 0)
	pending := make([]*OperatorWrapper, 0)
	total := 0
	if s.migrationPlan != nil {
		scheduling = true
		running = s.migrationPlan.Running
		pending = s.migrationPlan.Operators
		total = s.migrationPlan.TotalCount
		j := struct {
			Scheduling bool               `json:"scheduling"`
			RunningOps []*OperatorWrapper `json:"running"`
			Pending    []*OperatorWrapper `json:"pending"`
			TotalCount int                `json:"total"`
			ErrMsg     string             `json:"err_msg"`
		}{
			Scheduling: scheduling,
			RunningOps: running,
			Pending:    pending,
			TotalCount: total,
			ErrMsg:     "",
		}
		return j
	}
	return nil
}

// Schedule implements the Scheduler interface.
func (s *balanceKeyrangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	balanceKeyrangeScheduleCounter.Inc()

	doShutdown := func() {
		s.migrationPlan = nil
		if s.conf.removeSchedulerCb != nil {
			s.conf.removeSchedulerCb(types.BalanceKeyrangeScheduler.String())
		}
	}

	// When a pd cluster is bootstrapping, it should not create this scheduler.
	// However, in order to prevent a wrong operation which could also be from outside pd, we reject a schedule of a universal key-range.
	if len(s.conf.Range.StartKey) == 0 && len(s.conf.Range.EndKey) == 0 {
		log.Debug("Invalid keyrange", zap.ByteString("startKey", s.conf.Range.StartKey), zap.ByteString("endStartKey", s.conf.Range.EndKey), zap.Any("plan", s.migrationPlan))
		doShutdown()
		return []*operator.Operator{}, []plan.Plan{}
	}

	oc := s.BaseScheduler.OpController
	rangeChanged := true
	if s.migrationPlan != nil {
		// If there is a ongoing schedule,
		// - If it is timeout, then return.
		if s.IsTimeout() {
			log.Info("balance keyrange range timeout", zap.ByteString("planStartKey", s.migrationPlan.StartKey), zap.ByteString("planStartKey", s.migrationPlan.EndKey), zap.Any("StartTime", s.migrationPlan.StartTime), zap.Any("timeout", s.conf.MaxRunMillis))
			doShutdown()
			return []*operator.Operator{}, make([]plan.Plan, 0)
		}
		// - Then check if there comes a new schedule
		rangeChanged = !bytes.Equal(s.conf.Range.StartKey, s.migrationPlan.StartKey) || !bytes.Equal(s.conf.Range.EndKey, s.migrationPlan.EndKey)

		running := make([]*OperatorWrapper, 0)
		rerun := make([]*OperatorWrapper, 0)
		for _, opw := range s.migrationPlan.Running {
			op := opw.Operator
			canceledByStoreLimit := op.Status() == operator.CANCELED && op.GetAdditionalInfo("cancel-reason") == string(operator.ExceedStoreLimit)

			if canceledByStoreLimit {
				o, err := operator.CreateMovePeerOperator("balance-keyrange", cluster, opw.Region, operator.OpReplica, opw.FromStore, opw.Peer)
				opw.Operator = o
				if err == nil {
					rerun = append(rerun, opw)
				}
			} else if op.Status() == operator.CREATED || op.Status() == operator.STARTED {
				running = append(running, opw)
			}
		}
		s.migrationPlan.Running = running
		s.migrationPlan.Operators = append(s.migrationPlan.Operators, rerun...)
	}
	if s.IsFinished() {
		// If the current schedule is finished.
		if rangeChanged {
			// If there comes a new schedule task, generate a new schedule.
			p, err := RedistibuteRegions(cluster, s.conf.Range.StartKey, s.conf.Range.EndKey, s.conf.RequiredLabels)
			if err != nil {
				log.Error("balance keyrange can't generate plan", zap.Error(err))
			}
			s.migrationPlan = p
			s.migrationPlan.StartTime = time.Now()
		} else {
			// Otherwise, just shutdown.
			log.Info("shutdown balance keyrange", zap.Any("conf", s.conf))
			doShutdown()
			return []*operator.Operator{}, []plan.Plan{}
		}
	} else {
		if rangeChanged {
			// If the current schedule is ongoing, we should not schedule a new one.
			// However, if they did, we can not do anything, but reject here and clean the previous plan, because the conf has been changed.
			log.Error("balance keyrange range mismatch, clean the former plan", zap.ByteString("confStartKey", s.conf.Range.StartKey), zap.ByteString("confEndKey", s.conf.Range.EndKey), zap.ByteString("planStartKey", s.migrationPlan.StartKey), zap.ByteString("planStartKey", s.migrationPlan.EndKey), zap.Bool("IsFinished", s.IsFinished()))
			s.migrationPlan = nil
			return []*operator.Operator{}, make([]plan.Plan, 0)
		}
		// This is the normal branch that the current schedule is ongoing.
	}

	batchSize := s.conf.BatchSize
	limit := oc.GetSchedulerMaxWaitingOperator()
	queued := oc.GetWopCount(types.BalanceKeyrangeScheduler.String())
	if queued >= limit {
		batchSize = 0
	} else {
		if batchSize > limit-queued {
			batchSize = limit - queued
		}
	}
	var part []*operator.Operator
	scheduleSize := int(batchSize)
	if scheduleSize <= len(s.migrationPlan.Operators) {
		for _, opw := range s.migrationPlan.Operators[:scheduleSize] {
			part = append(part, opw.Operator)
		}
		s.migrationPlan.Running = append(s.migrationPlan.Running, s.migrationPlan.Operators[:scheduleSize]...)
		s.migrationPlan.Operators = s.migrationPlan.Operators[scheduleSize:]
	} else {
		for _, opw := range s.migrationPlan.Operators {
			part = append(part, opw.Operator)
		}
		s.migrationPlan.Running = append(s.migrationPlan.Running, s.migrationPlan.Operators...)
		s.migrationPlan.Operators = make([]*OperatorWrapper, 0)
	}
	return part, make([]plan.Plan, 0)
}
