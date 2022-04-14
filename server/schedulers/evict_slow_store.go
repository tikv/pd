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

package schedulers

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.uber.org/zap"
)

const (
	// EvictSlowStoreName is evict leader scheduler name.
	EvictSlowStoreName = "evict-slow-store-scheduler"
	// EvictSlowStoreType is evict leader scheduler type.
	EvictSlowStoreType = "evict-slow-store"

	slowStoreEvictThreshold   = 100
	slowStoreRecoverThreshold = 1
)

func init() {
	schedule.RegisterSliceDecoderBuilder(EvictSlowStoreType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(EvictSlowStoreType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictSlowStoreSchedulerConfig{storage: storage, EvictedStores: make([]uint64, 0)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowStoreScheduler(opController, conf), nil
	})
}

type evictSlowStoreSchedulerConfig struct {
	storage       endpoint.ConfigStorage
	EvictedStores []uint64 `json:"evict-stores"`
}

func (conf *evictSlowStoreSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	data, err := schedule.EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictSlowStoreSchedulerConfig) getSchedulerName() string {
	return EvictSlowStoreName
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.EvictedStores[0]
}

func (conf *evictSlowStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.EvictedStores = []uint64{id}
	return conf.Persist()
}

func (conf *evictSlowStoreSchedulerConfig) clearAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		err = conf.Persist()
	}
	return
}

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf *evictSlowStoreSchedulerConfig
}

func (s *evictSlowStoreScheduler) GetName() string {
	return EvictSlowStoreName
}

func (s *evictSlowStoreScheduler) GetType() string {
	return EvictSlowStoreType
}

func (s *evictSlowStoreScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *evictSlowStoreScheduler) Prepare(cluster schedule.Cluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	return nil
}

func (s *evictSlowStoreScheduler) Cleanup(cluster schedule.Cluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictSlowStoreScheduler) prepareEvictLeader(cluster schedule.Cluster, storeID uint64) error {
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed")
		return err
	}

	return cluster.SlowStoreEvicted(storeID)
}

func (s *evictSlowStoreScheduler) cleanupEvictLeader(cluster schedule.Cluster) {
	evictSlowStore, err := s.conf.clearAndPersist()
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", evictSlowStore))
	}
	if evictSlowStore == 0 {
		return
	}
	cluster.SlowStoreRecovered(evictSlowStore)
}

func (s *evictSlowStoreScheduler) schedulerEvictLeader(cluster schedule.Cluster) []*operator.Operator {
	storeMap := map[uint64][]core.KeyRange{
		s.conf.EvictedStores[0]: {core.NewKeyRange("", "")},
	}
	return scheduleEvictLeaderBatch(s.GetName(), s.GetType(), cluster, storeMap, EvictLeaderBatchSize)
}

func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	if len(s.conf.EvictedStores) != 0 {
		allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
		if !allowed {
			operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
		}
		return allowed
	}
	return true
}

func (s *evictSlowStoreScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator

	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the sheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
		} else if store.GetSlowScore() <= slowStoreRecoverThreshold {
			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
		} else {
			return s.schedulerEvictLeader(cluster)
		}
		s.cleanupEvictLeader(cluster)
		return ops
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// If there is more than onw slow store do nothing.
			if slowStore != nil {
				return ops
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return ops
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return ops
	}
	return s.schedulerEvictLeader(cluster)
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *schedule.OperatorController, conf *evictSlowStoreSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &evictSlowStoreScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
	return s
}
