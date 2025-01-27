// Copyright 2025 TiKV Project Authors.
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
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type balanceRangeSchedulerHandler struct {
	rd     *render.Render
	config *balanceRangeSchedulerConfig
}

func newBalanceRangeHandler(conf *balanceRangeSchedulerConfig) http.Handler {
	handler := &balanceRangeSchedulerHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceRangeSchedulerHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	if err := handler.rd.JSON(w, http.StatusOK, conf); err != nil {
		log.Error("failed to marshal balance key range scheduler config", errs.ZapError(err))
	}
}

type balanceRangeSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	balanceRangeSchedulerParam
}

type balanceRangeSchedulerParam struct {
	Role      string          `json:"role"`
	Engine    string          `json:"engine"`
	Timeout   time.Duration   `json:"timeout"`
	Ranges    []core.KeyRange `json:"ranges"`
	TableName string          `json:"table-name"`
}

func (conf *balanceRangeSchedulerConfig) clone() *balanceRangeSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceRangeSchedulerParam{
		Ranges:    ranges,
		Role:      conf.Role,
		Engine:    conf.Engine,
		Timeout:   conf.Timeout,
		TableName: conf.TableName,
	}
}

// EncodeConfig serializes the config.
func (s *balanceRangeScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf)
}

// ReloadConfig reloads the config.
func (s *balanceRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceRangeSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Timeout = newCfg.Timeout
	s.conf.Role = newCfg.Role
	s.conf.Engine = newCfg.Engine
	return nil
}

type balanceRangeScheduler struct {
	*BaseScheduler
	conf          *balanceRangeSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Schedule schedules the balance key range operator.
func (*balanceRangeScheduler) Schedule(_cluster sche.SchedulerCluster, _dryRun bool) ([]*operator.Operator, []plan.Plan) {
	log.Debug("balance key range scheduler is scheduling, need to implement")
	return nil, nil
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpRange)
	}
	return allowed
}

// BalanceRangeCreateOption is used to create a scheduler with an option.
type BalanceRangeCreateOption func(s *balanceRangeScheduler)

// newBalanceRangeScheduler creates a scheduler that tends to keep given peer role on
// special store balanced.
func newBalanceRangeScheduler(opController *operator.Controller, conf *balanceRangeSchedulerConfig, options ...BalanceRangeCreateOption) Scheduler {
	s := &balanceRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceRangeScheduler, conf),
		conf:          conf,
		handler:       newBalanceRangeHandler(conf),
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, OperatorLevel: constant.Medium},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}
