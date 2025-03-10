// Copyright 2017 TiKV Project Authors.
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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type grantLeaderSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	cluster           *core.BasicCluster
	removeSchedulerCb func(name string) error
}

func (conf *grantLeaderSchedulerConfig) buildWithArgs(args []string) error {
	if len(args) < 1 {
		return errs.ErrSchedulerConfig.FastGenByArgs("id")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errs.ErrStrconvParseUint.Wrap(err)
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return err
	}
	conf.Lock()
	defer conf.Unlock()
	conf.StoreIDWithRanges[id] = ranges
	return nil
}

func (conf *grantLeaderSchedulerConfig) clone() *grantLeaderSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	newStoreIDWithRanges := make(map[uint64][]core.KeyRange)
	for k, v := range conf.StoreIDWithRanges {
		newStoreIDWithRanges[k] = v
	}
	return &grantLeaderSchedulerConfig{
		StoreIDWithRanges: newStoreIDWithRanges,
	}
}

func (conf *grantLeaderSchedulerConfig) persist() error {
	conf.RLock()
	defer conf.RUnlock()
	return conf.save()
}

func (conf *grantLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.RLock()
	defer conf.RUnlock()
	ranges := conf.StoreIDWithRanges[id]
	res := make([]string, 0, len(ranges)*2)
	for index := range ranges {
		res = append(res, (string)(ranges[index].StartKey), (string)(ranges[index].EndKey))
	}
	return res
}

func (conf *grantLeaderSchedulerConfig) removeStore(id uint64) (succ bool, last bool) {
	conf.Lock()
	defer conf.Unlock()
	_, exists := conf.StoreIDWithRanges[id]
	succ, last = false, false
	if exists {
		delete(conf.StoreIDWithRanges, id)
		conf.cluster.ResumeLeaderTransfer(id, constant.Out)
		succ = true
		last = len(conf.StoreIDWithRanges) == 0
	}
	return succ, last
}

func (conf *grantLeaderSchedulerConfig) resetStore(id uint64, keyRange []core.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	if err := conf.cluster.PauseLeaderTransfer(id, constant.Out); err != nil {
		log.Error("pause leader transfer failed", zap.Uint64("store-id", id), errs.ZapError(err))
	}
	conf.StoreIDWithRanges[id] = keyRange
}

func (conf *grantLeaderSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	if ranges, exist := conf.StoreIDWithRanges[id]; exist {
		return ranges
	}
	return nil
}

func (conf *grantLeaderSchedulerConfig) getStoreIDWithRanges() map[uint64][]core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	storeIDWithRanges := make(map[uint64][]core.KeyRange)
	for id, ranges := range conf.StoreIDWithRanges {
		storeIDWithRanges[id] = ranges
	}
	return storeIDWithRanges
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*BaseScheduler
	conf    *grantLeaderSchedulerConfig
	handler http.Handler
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *operator.Controller, conf *grantLeaderSchedulerConfig) Scheduler {
	base := NewBaseScheduler(opController, types.GrantLeaderScheduler, conf)
	handler := newGrantLeaderHandler(conf)
	return &grantLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
}

// ServeHTTP implements the http.Handler interface.
func (s *grantLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// EncodeConfig implements the Scheduler interface.
func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *grantLeaderScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	newCfg := &grantLeaderSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	pauseAndResumeLeaderTransfer(s.conf.cluster, constant.Out, s.conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	s.conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *grantLeaderScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	s.conf.RLock()
	defer s.conf.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id, constant.Out); err != nil {
			res = err
		}
	}
	return res
}

// CleanConfig implements the Scheduler interface.
func (s *grantLeaderScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id, constant.Out)
	}
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *grantLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *grantLeaderScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	grantLeaderCounter.Inc()
	storeIDWithRanges := s.conf.getStoreIDWithRanges()
	ops := make([]*operator.Operator, 0, len(storeIDWithRanges))
	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	for id, ranges := range storeIDWithRanges {
		region := filter.SelectOneRegion(cluster.RandFollowerRegions(id, ranges), nil, pendingFilter, downFilter)
		if region == nil {
			grantLeaderNoFollowerCounter.Inc()
			continue
		}

		op, err := operator.CreateForceTransferLeaderOperator(s.GetName(), cluster, region, id, operator.OpLeader)
		if err != nil {
			log.Debug("fail to create grant leader operator", errs.ZapError(err))
			continue
		}
		op.Counters = append(op.Counters, grantLeaderNewOperatorCounter)
		op.SetPriorityLevel(constant.High)
		ops = append(ops, op)
	}

	return ops, nil
}

type grantLeaderHandler struct {
	rd     *render.Render
	config *grantLeaderSchedulerConfig
}

func (handler *grantLeaderHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	var exists bool
	var id uint64
	idFloat, ok := input["store_id"].(float64)
	if ok {
		id = (uint64)(idFloat)
		handler.config.RLock()
		if _, exists = handler.config.StoreIDWithRanges[id]; !exists {
			if err := handler.config.cluster.PauseLeaderTransfer(id, constant.Out); err != nil {
				handler.config.RUnlock()
				handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		handler.config.RUnlock()
		args = append(args, strconv.FormatUint(id, 10))
	}

	ranges, ok := (input["ranges"]).([]string)
	if ok {
		args = append(args, ranges...)
	} else if exists {
		args = append(args, handler.config.getRanges(id)...)
	}

	err := handler.config.buildWithArgs(args)
	if err != nil {
		log.Error("fail to build config", errs.ZapError(err))
		handler.config.Lock()
		handler.config.cluster.ResumeLeaderTransfer(id, constant.Out)
		handler.config.Unlock()
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	err = handler.config.persist()
	if err != nil {
		log.Error("fail to persist config", errs.ZapError(err))
		_, _ = handler.config.removeStore(id)
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
}

func (handler *grantLeaderHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *grantLeaderHandler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["store_id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	var resp any
	keyRanges := handler.config.getKeyRangesByID(id)
	succ, last := handler.config.removeStore(id)
	if succ {
		err = handler.config.persist()
		if err != nil {
			handler.config.resetStore(id, keyRanges)
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if last {
			if err := handler.config.removeSchedulerCb(types.GrantLeaderScheduler.String()); err != nil {
				if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
					handler.rd.JSON(w, http.StatusNotFound, err.Error())
				} else {
					handler.config.resetStore(id, keyRanges)
					handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
				}
				return
			}
			resp = lastStoreDeleteInfo
		}
		handler.rd.JSON(w, http.StatusOK, resp)
		return
	}

	handler.rd.JSON(w, http.StatusNotFound, errs.ErrScheduleConfigNotExist.FastGenByArgs().Error())
}

func newGrantLeaderHandler(config *grantLeaderSchedulerConfig) http.Handler {
	h := &grantLeaderHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	router.HandleFunc("/delete/{store_id}", h.deleteConfig).Methods(http.MethodDelete)
	return router
}
