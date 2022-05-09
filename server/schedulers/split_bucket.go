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

	"math"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/statistics/buckets"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	SplitBucketName = "split-bucket-scheduler"
	SplitBucketType = "split-bucket"

	DefaultHotDegree = 3
	DefaultSplitSize = 512
)

func init() {
	schedule.RegisterSliceDecoderBuilder(SplitBucketType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(SplitBucketType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := initSplitBucketConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newSplitBucketScheduler(opController, conf), nil
	})
}

func initSplitBucketConfig() *splitBucketSchedulerConfig {
	return &splitBucketSchedulerConfig{
		Degree:          DefaultHotDegree,
		SplitRegionSize: DefaultSplitSize,
	}
}

type splitBucketSchedulerConfig struct {
	mu              syncutil.RWMutex
	storage         endpoint.ConfigStorage
	Degree          int   `json:"degree"`
	SplitRegionSize int64 `json:"split_region_size"`
}

func (c *splitBucketSchedulerConfig) Clone() *splitBucketSchedulerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &splitBucketSchedulerConfig{
		Degree:          c.Degree,
		SplitRegionSize: c.SplitRegionSize,
	}
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

func (h *splitBucketHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	h.conf.mu.RLock()
	defer h.conf.mu.RUnlock()
	conf := h.conf.Clone()
	_ = h.rd.JSON(w, http.StatusOK, conf)
}

func newSplitBucketHandler(conf *splitBucketSchedulerConfig) http.Handler {
	h := &splitBucketHandler{
		conf: conf,
		rd:   render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.ListConfig).Methods(http.MethodGet)
	return router
}

func newSplitBucketScheduler(opController *schedule.OperatorController, conf *splitBucketSchedulerConfig) *splitBucketScheduler {
	base := NewBaseScheduler(opController)
	handler := newSplitBucketHandler(conf)
	ret := &splitBucketScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
	return ret
}

func (s *splitBucketScheduler) GetName() string {
	return SplitBucketName
}

func (s *splitBucketScheduler) GetType() string {
	return SplitBucketType
}

func (s *splitBucketScheduler) ServerHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed return true.
func (s *splitBucketScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return true
}

// Schedule return operators if some bucket is too hot.
func (s *splitBucketScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	conf := s.conf.Clone()
	hotBuckets := cluster.BucketsStats(conf.Degree)
	degree := math.MinInt32
	var splitKeys [][]byte
	var region *core.RegionInfo
	for regionID, buckets := range hotBuckets {
		region = cluster.GetRegion(regionID)
		// skip if region is not exist
		if region == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
			continue
		}
		// region size is less than split region size
		if region.GetApproximateKeys() <= conf.SplitRegionSize {
			schedulerCounter.WithLabelValues(s.GetName(), "region-too-small").Inc()
			continue
		}
		if op := s.OpController.GetOperator(regionID); op != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "operator-exist").Inc()
			continue
		}
		for _, bucket := range buckets {
			keys := checkSplit(region, bucket)
			if hg := bucket.GetHotDegree(); hg > degree && len(keys) > 0 {
				splitKeys = keys
				degree = hg
			}
		}
	}
	if len(splitKeys) > 0 {
		op, err := operator.CreateSplitRegionOperator(SplitBucketType, region, operator.OpSplit,
			pdpb.CheckPolicy_USEKEY, splitKeys)
		if err != nil {
			log.Info("create split operator failed", zap.Error(err))
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil
		}
		return []*operator.Operator{op}
	}
	return nil
}

func checkSplit(region *core.RegionInfo, state *buckets.BucketStat) [][]byte {
	splitKeys := make([][]byte, 0)
	if bytes.Compare(state.StartKey, region.GetStartKey()) > 0 {
		splitKeys = append(splitKeys, state.StartKey)
	}

	if bytes.Compare(state.EndKey, region.GetEndKey()) < 0 {
		splitKeys = append(splitKeys, state.EndKey)
	}
	return splitKeys
}
