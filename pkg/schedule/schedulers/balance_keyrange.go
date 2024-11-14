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
	ID           uint64
	Info         *core.StoreInfo
	RegionIDSet  map[uint64]bool
	OriginalPeer map[uint64]*metapb.Peer
}

type MigrationOp struct {
	FromStore    uint64          `json:"from_store"`
	ToStore      uint64          `json:"to_store"`
	ToStoreInfo  *core.StoreInfo `json:"to_store_info"`
	OriginalPeer *metapb.Peer    `json:"original_peer"`
	Regions      map[uint64]bool `json:"regions"`
}

func PickRegions(n int, fromStore *StoreRegionSet, toStore *StoreRegionSet) *MigrationOp {
	o := MigrationOp{
		FromStore:   fromStore.ID,
		ToStore:     toStore.ID,
		ToStoreInfo: toStore.Info,
		Regions:     make(map[uint64]bool),
	}
	for r, removed := range fromStore.RegionIDSet {
		if n == 0 {
			break
		}
		if removed {
			continue
		}
		if _, exist := toStore.RegionIDSet[r]; !exist {
			// If toStore doesn't has this region, then create a move op.
			o.Regions[r] = false
			o.OriginalPeer = fromStore.OriginalPeer[r]
			fromStore.RegionIDSet[r] = true
			n--
		}
	}
	return &o
}

func buildMigrationPlan(stores []*StoreRegionSet) ([]int, []int, []*MigrationOp) {
	totalRegionCount := 0
	if len(stores) == 0 {
		log.Info("no stores for migration")
		return []int{}, []int{}, []*MigrationOp{}
	}
	for _, store := range stores {
		totalRegionCount += len(store.RegionIDSet)
	}
	for _, store := range stores {
		percentage := 100 * float64(len(store.RegionIDSet)) / float64(totalRegionCount)
		log.Info("!!! store region dist",
			zap.Uint64("store-id", store.ID),
			zap.Int("num-region", len(store.RegionIDSet)),
			zap.String("percentage", fmt.Sprintf("%.2f%%", percentage)))
	}
	avr := totalRegionCount / len(stores)
	remainder := totalRegionCount % len(stores)
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
					ops = append(ops, PickRegions(n, fromStore, toStore))
				}
			}
		}
	}

	return senders, receivers, ops
}

type MigrationPlan struct {
	ErrorCode uint64               `json:"error_code"`
	StartKey  []byte               `json:"start_key"`
	EndKey    []byte               `json:"end_key"`
	Ops       []*MigrationOp       `json:"ops"`
	Operators []*operator.Operator `json:"operators"`
}

func ComputeCandidateStores(requiredLabels []*metapb.StoreLabel, stores []*core.StoreInfo, regions []*core.RegionInfo) []*StoreRegionSet {
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
			ID:           s.GetID(),
			Info:         s,
			RegionIDSet:  make(map[uint64]bool),
			OriginalPeer: make(map[uint64]*metapb.Peer),
		}
		for _, r := range regions {
			for _, p := range r.GetPeers() {
				if p.StoreId == s.GetID() {
					candidate.RegionIDSet[r.GetID()] = false
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
	// startKey, err := hex.DecodeString(string(rawStartKey))
	// if err != nil {
	// 	return buildErrorMigrationPlan(), err
	// }
	// endKey, err := hex.DecodeString(string(rawEndKey))
	// if err != nil {
	// 	return buildErrorMigrationPlan(), err
	// }
	if c == nil {
		return buildErrorMigrationPlan(), errs.ErrNotBootstrapped.GenWithStackByArgs()
	}

	regions := c.ScanRegions(startKey, endKey, -1)
	regionIDMap := make(map[uint64]*core.RegionInfo)
	for _, r := range regions {
		regionIDMap[r.GetID()] = r
	}

	stores := c.GetStores()
	candidates := ComputeCandidateStores(requiredLabels, stores, regions)

	senders, receivers, ops := buildMigrationPlan(candidates)

	log.Info("Migration plan details", zap.Any("startKey", startKey), zap.Any("DstartKey", startKey), zap.Any("endKey", endKey), zap.Any("senders", senders), zap.Any("receivers", receivers), zap.Any("ops", ops), zap.Any("stores", stores))

	operators := make([]*operator.Operator, 0)
	for _, op := range ops {
		for rid := range op.Regions {
			newPeer := &metapb.Peer{StoreId: op.ToStore, Role: op.OriginalPeer.Role, IsWitness: op.OriginalPeer.IsWitness}
			log.Debug("Create balace region op", zap.Uint64("from", op.FromStore), zap.Uint64("to", op.ToStore), zap.Uint64("region_id", rid))
			o, err := operator.CreateMovePeerOperator("balance-keyrange", c, regionIDMap[rid], operator.OpReplica, op.FromStore, newPeer)
			if err != nil {
				return buildErrorMigrationPlan(), err
			}
			operators = append(operators, o)
		}
	}

	return &MigrationPlan{
		ErrorCode: 0,
		StartKey:  startKey,
		EndKey:    endKey,
		Ops:       ops,
		Operators: operators,
	}, nil
}

type balanceKeyrangeSchedulerConfig struct {
	baseDefaultSchedulerConfig

	Range          core.KeyRange `json:"range"`
	RequiredLabels []*metapb.StoreLabel
	BatchSize      uint64
}

type balanceKeyrangeScheduler struct {
	*BaseScheduler
	*retryQuota
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
	return s.migrationPlan == nil || len(s.migrationPlan.Operators) == 0
}

// IsTimeout is true if the schedule took too much time and needs to be canceled.
func (s *balanceKeyrangeScheduler) IsTimeout() bool {
	// TODO
	return false
}

// Schedule implements the Scheduler interface.
func (s *balanceKeyrangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	balanceKeyrangeScheduleCounter.Inc()

	if s.IsFinished() {
		// Generate a new schedule.
		p, err := RedistibuteRegions(cluster, s.conf.Range.StartKey, s.conf.Range.EndKey, s.conf.RequiredLabels)
		if err != nil {
			log.Error("balance keyrange can't generate plan", zap.Error(err))
		}
		s.migrationPlan = p
	}

	if !bytes.Equal(s.conf.Range.StartKey, s.migrationPlan.StartKey) || !bytes.Equal(s.conf.Range.EndKey, s.migrationPlan.EndKey) {
		log.Error("balance keyrange range mismatch", zap.ByteString("confStartKey", s.conf.Range.StartKey), zap.ByteString("confEndKey", s.conf.Range.EndKey), zap.ByteString("planStartKey", s.migrationPlan.StartKey), zap.ByteString("planStartKey", s.migrationPlan.EndKey), zap.Bool("IsFinished", s.IsFinished()))
		return []*operator.Operator{}, make([]plan.Plan, 0)
	}

	batchSize := int(s.conf.BatchSize)
	var part []*operator.Operator
	if batchSize <= len(s.migrationPlan.Operators) {
		part = s.migrationPlan.Operators[:batchSize]
		s.migrationPlan.Operators = s.migrationPlan.Operators[batchSize:]
	} else {
		part = s.migrationPlan.Operators
		s.migrationPlan.Operators = make([]*operator.Operator, 0)
	}
	return part, make([]plan.Plan, 0)
}
