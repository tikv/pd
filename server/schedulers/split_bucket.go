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
	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/unrolled/render"
	"net/http"
)

const (
	SplitBucketName = "split-bucket-scheduler"
	SplitBucketType = "split-bucket"
)

func init() {

}

type splitBucketSchedulerConfig struct {
	mu syncutil.RWMutex
}

func (c *splitBucketSchedulerConfig) Clone() *splitBucketSchedulerConfig {
	return &splitBucketSchedulerConfig{}
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

func (s *splitBucketScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {

	return nil
}
