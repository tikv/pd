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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
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
			if len(args) != 1 && len(args) != 0 {
				return errs.ErrSchedulerConfig.FastGenByArgs("evicted-store")
			}
			conf, ok := v.(*evictSlowStoreSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			if len(args) == 1 {
				id, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				conf.EvictedStores = []uint64{id}
			}
			return nil
		}
	})

	schedule.RegisterScheduler(EvictSlowStoreType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictSlowStoreSchedulerConfig{storage: storage, EvictedStores: make([]uint64, 0)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowStoreScheduler(opController, conf), nil
	})
}

type evictSlowStoreSchedulerConfig struct {
	storage       *core.Storage
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

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf                 *evictSlowStoreSchedulerConfig
	evictLeaderScheduler schedule.Scheduler
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

func (s *evictSlowStoreScheduler) Prepare(cluster opt.Cluster) error {
	if s.evictLeaderScheduler != nil {
		if err := s.evictLeaderScheduler.Prepare(cluster); err != nil {
			return err
		}
	}
	return nil
}

func (s *evictSlowStoreScheduler) Cleanup(cluster opt.Cluster) {
	if s.evictLeaderScheduler != nil {
		s.evictLeaderScheduler.Cleanup(cluster)
	}
}

func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	if s.evictLeaderScheduler != nil {
		return s.evictLeaderScheduler.IsScheduleAllowed(cluster)
	}
	return true
}

func (s *evictSlowStoreScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator

	evictedStores := s.conf.EvictedStores
	if len(evictedStores) != 0 {
		store := cluster.GetStore(evictedStores[0])
		if store == nil || store.IsTombstone() {
			// Previous slow store had been removed, remove the sheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Stringer("store", store.GetMeta()))
		} else if store.GetSlowScore() <= slowStoreRecoverThreshold {
			log.Info("slow store has been recovered",
				zap.Stringer("store", store.GetMeta()))
		} else {
			return s.evictLeaderScheduler.Schedule(cluster)
		}
		s.conf.EvictedStores = []uint64{0}
		s.conf.Persist()
		s.evictLeaderScheduler.Cleanup(cluster)
		s.evictLeaderScheduler = nil
	} else {
		slowStores := make([]*core.StoreInfo, 0)
		for _, store := range cluster.GetStores() {
			if store.IsTombstone() {
				continue
			}

			if store.IsUp() && store.IsSlow() {
				slowStores = append(slowStores, store)
			}
		}

		if len(slowStores) == 1 && slowStores[0].GetSlowScore() >= slowStoreEvictThreshold {
			store := slowStores[0]
			log.Info("detected slow store, start to evict leaders",
				zap.Stringer("store", store.GetMeta()))
			s.conf.EvictedStores = []uint64{store.GetID()}
			err := s.conf.Persist()
			if err != nil {
				return ops
			}
			s.initEvictLeaderScheduler()
			err = s.evictLeaderScheduler.Prepare(cluster)
			if err != nil {
				log.Info("prepare for evicting leader failed", zap.Error(err), zap.Stringer("store", store.GetMeta()))
				return ops
			}
			ops = s.evictLeaderScheduler.Schedule(cluster)
		} else if len(slowStores) > 1 {
			storeIds := make([]uint64, len(slowStores))
			for _, store := range slowStores {
				storeIds = append(storeIds, store.GetID())
			}
			log.Info("detected slow stores", zap.Reflect("stores", storeIds))
		}
	}

	return ops
}

func (s *evictSlowStoreScheduler) initEvictLeaderScheduler() {
	evictLeaderConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: s.conf.storage}
	evictLeaderConf.BuildWithArgs([]string{strconv.FormatUint(s.conf.EvictedStores[0], 10)})
	s.evictLeaderScheduler = newEvictLeaderScheduler(s.OpController, evictLeaderConf)
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *schedule.OperatorController, conf *evictSlowStoreSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &evictSlowStoreScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
	if len(conf.EvictedStores) != 0 {
		s.initEvictLeaderScheduler()
	}
	return s
}
