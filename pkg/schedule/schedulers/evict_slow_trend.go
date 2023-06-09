// Copyright 2023 TiKV Project Authors.
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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"go.uber.org/zap"
)

const (
	// EvictSlowTrendName is evict leader by slow trend scheduler name.
	EvictSlowTrendName = "evict-slow-trend-scheduler"
	// EvictSlowTrendType is evict leader by slow trend scheduler type.
	EvictSlowTrendType = "evict-slow-trend"
)

type evictSlowTrendSchedulerConfig struct {
	storage              endpoint.ConfigStorage
	evictCandidate       uint64
	candidateCaptureTime time.Time

	// Only evict one store for now
	EvictedStores []uint64 `json:"evict-by-trend-stores"`
}

func (conf *evictSlowTrendSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	data, err := EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictSlowTrendSchedulerConfig) getSchedulerName() string {
	return EvictSlowTrendName
}

func (conf *evictSlowTrendSchedulerConfig) getStores() []uint64 {
	return conf.EvictedStores
}

func (conf *evictSlowTrendSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	if conf.evictedStore() != id {
		return nil
	}
	return []core.KeyRange{core.NewKeyRange("", "")}
}

func (conf *evictSlowTrendSchedulerConfig) evictedStore() uint64 {
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.EvictedStores[0]
}

func (conf *evictSlowTrendSchedulerConfig) candidate() uint64 {
	return conf.evictCandidate
}

func (conf *evictSlowTrendSchedulerConfig) captureTS() time.Time {
	return conf.candidateCaptureTime
}

func (conf *evictSlowTrendSchedulerConfig) candidateCapturedSecs() uint64 {
	return uint64(time.Since(conf.candidateCaptureTime).Seconds())
}

func (conf *evictSlowTrendSchedulerConfig) captureCandidate(id uint64) {
	conf.evictCandidate = id
	conf.candidateCaptureTime = time.Now()
}

func (conf *evictSlowTrendSchedulerConfig) popCandidate() uint64 {
	id := conf.evictCandidate
	conf.evictCandidate = 0
	return id
}

func (conf *evictSlowTrendSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.EvictedStores = []uint64{id}
	return conf.Persist()
}

func (conf *evictSlowTrendSchedulerConfig) clearAndPersist(cluster sche.ScheduleCluster) (oldID uint64, err error) {
	oldID = conf.evictedStore()
	if oldID == 0 {
		return
	}
	address := "?"
	store := cluster.GetStore(oldID)
	if store != nil {
		address = store.GetAddress()
	}
	storeSlowTrendEvictedStatusGauge.WithLabelValues(address, strconv.FormatUint(oldID, 10)).Set(0)
	conf.EvictedStores = []uint64{}
	return oldID, conf.Persist()
}

type evictSlowTrendScheduler struct {
	*BaseScheduler
	conf *evictSlowTrendSchedulerConfig
}

func (s *evictSlowTrendScheduler) GetName() string {
	return EvictSlowTrendName
}

func (s *evictSlowTrendScheduler) GetType() string {
	return EvictSlowTrendType
}

func (s *evictSlowTrendScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

func (s *evictSlowTrendScheduler) Prepare(cluster sche.ScheduleCluster) error {
	evictedStoreID := s.conf.evictedStore()
	if evictedStoreID == 0 {
		return nil
	}
	return cluster.SlowTrendEvicted(evictedStoreID)
}

func (s *evictSlowTrendScheduler) Cleanup(cluster sche.ScheduleCluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictSlowTrendScheduler) prepareEvictLeader(cluster sche.ScheduleCluster, storeID uint64) error {
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-trend-scheduler persist config failed", zap.Uint64("store-id", storeID))
		return err
	}
	return cluster.SlowTrendEvicted(storeID)
}

func (s *evictSlowTrendScheduler) cleanupEvictLeader(cluster sche.ScheduleCluster) {
	evictedStoreID, err := s.conf.clearAndPersist(cluster)
	if err != nil {
		log.Info("evict-slow-trend-scheduler persist config failed", zap.Uint64("store-id", evictedStoreID))
	}
	if evictedStoreID != 0 {
		cluster.SlowTrendRecovered(evictedStoreID)
	}
}

func (s *evictSlowTrendScheduler) scheduleEvictLeader(cluster sche.ScheduleCluster) []*operator.Operator {
	store := cluster.GetStore(s.conf.evictedStore())
	if store == nil {
		return nil
	}
	storeSlowTrendEvictedStatusGauge.WithLabelValues(store.GetAddress(), strconv.FormatUint(store.GetID(), 10)).Set(1)
	return scheduleEvictLeaderBatch(s.GetName(), s.GetType(), cluster, s.conf, EvictLeaderBatchSize)
}

func (s *evictSlowTrendScheduler) IsScheduleAllowed(cluster sche.ScheduleCluster) bool {
	if s.conf.evictedStore() == 0 {
		return true
	}
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *evictSlowTrendScheduler) Schedule(cluster sche.ScheduleCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	var ops []*operator.Operator

	if s.conf.evictedStore() != 0 {
		store := cluster.GetStore(s.conf.evictedStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("store evicted by slow trend has been removed",
				zap.Uint64("store-id", store.GetID()))
			storeSlowTrendActionStatusGauge.WithLabelValues("evict.stop:removed").Inc()
		} else if checkStoreCanRecover(cluster, store) {
			log.Info("store evicted by slow trend has been recovered",
				zap.Uint64("store-id", store.GetID()))
			storeSlowTrendActionStatusGauge.WithLabelValues("evict.stop:recovered").Inc()
		} else {
			storeSlowTrendActionStatusGauge.WithLabelValues("evict.continue").Inc()
			return s.scheduleEvictLeader(cluster), nil
		}
		s.cleanupEvictLeader(cluster)
		return ops, nil
	}

	candFreshCaptured := false
	if s.conf.candidate() == 0 {
		candidate := chooseEvictCandidate(cluster)
		if candidate != nil {
			storeSlowTrendActionStatusGauge.WithLabelValues("cand.captured").Inc()
			s.conf.captureCandidate(candidate.GetID())
			candFreshCaptured = true
		}
	} else {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.continue").Inc()
	}
	slowStoreID := s.conf.candidate()
	if slowStoreID == 0 {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none").Inc()
		return ops, nil
	}

	slowStore := cluster.GetStore(slowStoreID)
	if !candFreshCaptured && checkStoreFasterThanOthers(cluster, slowStore) {
		s.conf.popCandidate()
		log.Info("slow store candidate by trend has been cancel",
			zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.cancel:too-faster").Inc()
		return ops, nil
	}
	slowStoreRecordTS := s.conf.captureTS()
	if !checkStoresAreUpdated(cluster, slowStoreID, slowStoreRecordTS) {
		log.Info("slow store candidate waiting for other stores to update heartbeats",
			zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.wait").Inc()
		return ops, nil
	}

	candCapturedSecs := s.conf.candidateCapturedSecs()
	log.Info("detected slow store by trend, start to evict leaders",
		zap.Uint64("store-id", slowStoreID),
		zap.Uint64("candidate-captured-secs", candCapturedSecs))
	storeSlowTrendMiscGauge.WithLabelValues("cand.captured.secs").Set(float64(candCapturedSecs))
	err := s.prepareEvictLeader(cluster, s.conf.popCandidate())
	if err != nil {
		log.Info("prepare for evicting leader by slow trend failed", zap.Error(err), zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("evict.prepare.err").Inc()
		return ops, nil
	}
	storeSlowTrendActionStatusGauge.WithLabelValues("evict.start").Inc()
	return s.scheduleEvictLeader(cluster), nil
}

func newEvictSlowTrendScheduler(opController *operator.Controller, conf *evictSlowTrendSchedulerConfig) Scheduler {
	return &evictSlowTrendScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		conf:          conf,
	}
}

func chooseEvictCandidate(cluster sche.ScheduleCluster) (slowStore *core.StoreInfo) {
	stores := cluster.GetStores()
	if len(stores) < 3 {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none:too-few").Inc()
		return
	}
	var candidates []*core.StoreInfo
	var affectedStoreCount int
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		slowTrend := store.GetSlowTrend()
		if slowTrend != nil && slowTrend.CauseRate > alterEpsilon && slowTrend.ResultRate < -alterEpsilon {
			candidates = append(candidates, store)
			storeSlowTrendActionStatusGauge.WithLabelValues("cand.add").Inc()
			log.Info("evict-slow-trend-scheduler pre-canptured candidate",
				zap.Uint64("store-id", store.GetID()),
				zap.Float64("cause-rate", slowTrend.CauseRate),
				zap.Float64("result-rate", slowTrend.ResultRate),
				zap.Float64("cause-value", slowTrend.CauseValue),
				zap.Float64("result-value", slowTrend.ResultValue))
		}
		if slowTrend != nil && slowTrend.ResultRate < -alterEpsilon {
			affectedStoreCount += 1
		}
	}
	if len(candidates) == 0 {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none:no-fit").Inc()
		return
	}

	// TODO: Calculate to judge if one store is way slower than the others
	if len(candidates) > 1 {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none:too-many").Inc()
		return
	}
	store := candidates[0]

	affectedStoreThreshold := int(float64(len(stores)) * cluster.GetOpts().GetSlowStoreEvictingAffectedStoreRatioThreshold())
	if affectedStoreCount < affectedStoreThreshold {
		log.Info("evict-slow-trend-scheduler failed to confirm candidate: it only affect a few stores", zap.Uint64("store-id", store.GetID()))
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none:affect-a-few").Inc()
		return
	}

	if !checkStoreSlowerThanOthers(cluster, store) {
		log.Info("evict-slow-trend-scheduler failed to confirm candidate: it's not slower than others", zap.Uint64("store-id", store.GetID()))
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.none:not-slower").Inc()
		return
	}

	storeSlowTrendActionStatusGauge.WithLabelValues("cand.add").Inc()
	log.Info("evict-slow-trend-scheduler captured candidate", zap.Uint64("store-id", store.GetID()))
	return store
}

func checkStoresAreUpdated(cluster sche.ScheduleCluster, slowStoreID uint64, slowStoreRecordTS time.Time) bool {
	stores := cluster.GetStores()
	if len(stores) <= 1 {
		return false
	}
	expected := (len(stores) + 1) / 2
	updatedStores := 0
	for _, store := range stores {
		if store.IsRemoved() {
			updatedStores += 1
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			updatedStores += 1
			continue
		}
		if store.GetID() == slowStoreID {
			updatedStores += 1
			continue
		}
		if slowStoreRecordTS.Before(store.GetLastHeartbeatTS()) {
			updatedStores += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("stores.check-updated:count").Set(float64(updatedStores))
	storeSlowTrendMiscGauge.WithLabelValues("stores.check-updated:expected").Set(float64(expected))
	return updatedStores >= expected
}

func checkStoreSlowerThanOthers(cluster sche.ScheduleCluster, target *core.StoreInfo) bool {
	stores := cluster.GetStores()
	expected := (len(stores)*2 + 1) / 3
	targetSlowTrend := target.GetSlowTrend()
	if targetSlowTrend == nil {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.check-slower:no-data").Inc()
		return false
	}
	slowerThanStoresNum := 0
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		if store.GetID() == target.GetID() {
			continue
		}
		slowTrend := store.GetSlowTrend()
		// Use `SlowTrend.ResultValue` at first, but not good, `CauseValue` is better
		// Greater `CuaseValue` means slower
		if slowTrend != nil && (targetSlowTrend.CauseValue-slowTrend.CauseValue) > alterEpsilon && slowTrend.CauseValue > alterEpsilon {
			slowerThanStoresNum += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("store.check-slower:count").Set(float64(slowerThanStoresNum))
	storeSlowTrendMiscGauge.WithLabelValues("store.check-slower:expected").Set(float64(expected))
	return slowerThanStoresNum >= expected
}

func checkStoreCanRecover(cluster sche.ScheduleCluster, target *core.StoreInfo) bool {
	/*
		//
		// This might not be necessary,
		//   and it also have tiny chances to cause `stuck in evicted`
		//   status when this store restarted,
		//   the `become fast` might be ignore on tikv side
		//   because of the detecting windows are not fully filled yet.
		// Hence, we disabled this event capturing by now but keep the code here for further checking.
		//

		// Wait for the evicted store's `become fast` event
		slowTrend := target.GetSlowTrend()
		if slowTrend == nil || slowTrend.CauseRate >= 0 && slowTrend.ResultRate <= 0 {
			storeSlowTrendActionStatusGauge.WithLabelValues("recover.reject:no-event").Inc()
			return false
		} else {
			storeSlowTrendActionStatusGauge.WithLabelValues("recover.judging:got-event").Inc()
		}
	*/
	return checkStoreFasterThanOthers(cluster, target)
}

func checkStoreFasterThanOthers(cluster sche.ScheduleCluster, target *core.StoreInfo) bool {
	stores := cluster.GetStores()
	expected := (len(stores) + 1) / 2
	targetSlowTrend := target.GetSlowTrend()
	if targetSlowTrend == nil {
		storeSlowTrendActionStatusGauge.WithLabelValues("cand.check-faster:no-data").Inc()
		return false
	}
	fasterThanStores := 0
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		if store.GetID() == target.GetID() {
			continue
		}
		slowTrend := store.GetSlowTrend()
		// Greater `CuaseValue` means slower
		if slowTrend != nil && targetSlowTrend.CauseValue <= slowTrend.CauseValue*1.1 &&
			slowTrend.CauseValue > alterEpsilon && targetSlowTrend.CauseValue > alterEpsilon {
			fasterThanStores += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("store.check-faster:count").Set(float64(fasterThanStores))
	storeSlowTrendMiscGauge.WithLabelValues("store.check-faster:expected").Set(float64(expected))
	return fasterThanStores >= expected
}

const alterEpsilon = 1e-9
