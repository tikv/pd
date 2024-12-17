// Copyright 2022 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// defaultHotDegree is the default hot region threshold.
	defaultHotDegree  = 3
	defaultSplitLimit = 10
)

func initSplitBucketConfig() *splitBucketSchedulerConfig {
	return &splitBucketSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		Degree:          defaultHotDegree,
		SplitLimit:      defaultSplitLimit,
	}
}

type splitBucketSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	Degree     int    `json:"degree"`
	SplitLimit uint64 `json:"split-limit"`
}

func (conf *splitBucketSchedulerConfig) clone() *splitBucketSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &splitBucketSchedulerConfig{
		Degree: conf.Degree,
	}
}

func (conf *splitBucketSchedulerConfig) getDegree() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Degree
}

func (conf *splitBucketSchedulerConfig) getSplitLimit() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.SplitLimit
}

type splitBucketScheduler struct {
	*BaseScheduler
	conf    *splitBucketSchedulerConfig
	handler http.Handler
}

type splitBucketHandler struct {
	conf *splitBucketSchedulerConfig
	rd   *render.Render
}

func (h *splitBucketHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := h.conf.clone()
	h.rd.JSON(w, http.StatusOK, conf)
}

func (h *splitBucketHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	h.conf.Lock()
	defer h.conf.Unlock()
	rd := render.New(render.Options{IndentJSON: true})
	oldc, _ := json.Marshal(h.conf)
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := json.Unmarshal(data, h.conf); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	newc, _ := json.Marshal(h.conf)
	if !bytes.Equal(oldc, newc) {
		if err := h.conf.save(); err != nil {
			log.Warn("failed to save config", errs.ZapError(err))
		}
		rd.Text(w, http.StatusOK, "Config is updated.")
		return
	}

	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ok := reflectutil.FindSameFieldByJSON(h.conf, m)
	if ok {
		rd.Text(w, http.StatusOK, "Config is the same with origin, so do nothing.")
		return
	}

	rd.Text(w, http.StatusBadRequest, "Config item is not found.")
}

func newSplitBucketHandler(conf *splitBucketSchedulerConfig) http.Handler {
	h := &splitBucketHandler{
		conf: conf,
		rd:   render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	return router
}

func newSplitBucketScheduler(opController *operator.Controller, conf *splitBucketSchedulerConfig) *splitBucketScheduler {
	base := NewBaseScheduler(opController, types.SplitBucketScheduler, conf)
	handler := newSplitBucketHandler(conf)
	ret := &splitBucketScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
	return ret
}

// ReloadConfig implement Scheduler interface.
func (s *splitBucketScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	newCfg := &splitBucketSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.SplitLimit = newCfg.SplitLimit
	s.conf.Degree = newCfg.Degree
	return nil
}

// ServerHTTP implement Http server.
func (s *splitBucketScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed return true if the sum of executing opSplit operator is less  .
func (s *splitBucketScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	if !cluster.GetStoreConfig().IsEnableRegionBucket() {
		splitBucketDisableCounter.Inc()
		return false
	}
	allowed := s.BaseScheduler.OpController.OperatorCount(operator.OpSplit) < s.conf.getSplitLimit()
	if !allowed {
		splitBuckerSplitLimitCounter.Inc()
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpSplit)
	}
	return allowed
}

type splitBucketPlan struct {
	hotBuckets         map[uint64][]*buckets.BucketStat
	cluster            sche.SchedulerCluster
	conf               *splitBucketSchedulerConfig
	hotRegionSplitSize int64
}

// Schedule return operators if some bucket is too hot.
func (s *splitBucketScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	splitBucketScheduleCounter.Inc()
	conf := s.conf.clone()
	plan := &splitBucketPlan{
		conf:               conf,
		cluster:            cluster,
		hotBuckets:         cluster.BucketsStats(conf.getDegree()),
		hotRegionSplitSize: cluster.GetSchedulerConfig().GetMaxMovableHotPeerSize(),
	}
	return s.splitBucket(plan), nil
}

func (s *splitBucketScheduler) splitBucket(plan *splitBucketPlan) []*operator.Operator {
	var splitBucket *buckets.BucketStat
	for regionID, buckets := range plan.hotBuckets {
		region := plan.cluster.GetRegion(regionID)
		// skip if the region doesn't exist
		if region == nil {
			splitBucketNoRegionCounter.Inc()
			continue
		}
		// region size is less than split region size
		if region.GetApproximateSize() <= plan.hotRegionSplitSize {
			splitBucketRegionTooSmallCounter.Inc()
			continue
		}
		if op := s.OpController.GetOperator(regionID); op != nil {
			splitBucketOperatorExistCounter.Inc()
			continue
		}
		for _, bucket := range buckets {
			// the key range of the bucket must less than the region.
			// like bucket: [001 100] and region: [001 100] will not pass.
			// like bucket: [003 100] and region: [002 100] will pass.
			if bytes.Compare(bucket.StartKey, region.GetStartKey()) < 0 || bytes.Compare(bucket.EndKey, region.GetEndKey()) > 0 {
				splitBucketKeyRangeNotMatchCounter.Inc()
				continue
			}
			if bytes.Equal(bucket.StartKey, region.GetStartKey()) && bytes.Equal(bucket.EndKey, region.GetEndKey()) {
				splitBucketNoSplitKeysCounter.Inc()
				continue
			}

			if splitBucket == nil || bucket.HotDegree > splitBucket.HotDegree {
				splitBucket = bucket
			}
		}
	}
	if splitBucket != nil {
		region := plan.cluster.GetRegion(splitBucket.RegionID)
		if region == nil {
			return nil
		}
		splitKey := make([][]byte, 0)
		if bytes.Compare(region.GetStartKey(), splitBucket.StartKey) < 0 {
			splitKey = append(splitKey, splitBucket.StartKey)
		}
		if bytes.Compare(region.GetEndKey(), splitBucket.EndKey) > 0 {
			splitKey = append(splitKey, splitBucket.EndKey)
		}
		op, err := operator.CreateSplitRegionOperator(s.GetName(), region, operator.OpSplit,
			pdpb.CheckPolicy_USEKEY, splitKey)
		if err != nil {
			splitBucketCreateOperatorFailCounter.Inc()
			return nil
		}
		splitBucketNewOperatorCounter.Inc()
		op.SetAdditionalInfo("hot-degree", strconv.FormatInt(int64(splitBucket.HotDegree), 10))
		return []*operator.Operator{op}
	}
	return nil
}
