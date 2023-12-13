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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
)

const (
	// GrantLeaderName is grant leader scheduler name.
	GrantLeaderName = "grant-leader-scheduler"
	// GrantLeaderType is grant leader scheduler type.
	GrantLeaderType = "grant-leader"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	grantLeaderCounter            = schedulerCounter.WithLabelValues(GrantLeaderName, "schedule")
	grantLeaderNoFollowerCounter  = schedulerCounter.WithLabelValues(GrantLeaderName, "no-follower")
	grantLeaderNewOperatorCounter = schedulerCounter.WithLabelValues(GrantLeaderName, "new-operator")
)

type grantLeaderSchedulerConfig struct {
	syncutil.RWMutex
	storage           endpoint.ConfigStorage
	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	cluster           *core.BasicCluster
	removeSchedulerCb func(name string) error
}

func (conf *grantLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errs.ErrSchedulerConfig.FastGenByArgs("id")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
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

func (conf *grantLeaderSchedulerConfig) Clone() *grantLeaderSchedulerConfig {
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

func (conf *grantLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.RLock()
	defer conf.RUnlock()
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *grantLeaderSchedulerConfig) getSchedulerName() string {
	return GrantLeaderName
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
		conf.cluster.ResumeLeaderTransfer(id)
		succ = true
		last = len(conf.StoreIDWithRanges) == 0
	}
	return succ, last
}

func (conf *grantLeaderSchedulerConfig) resetStore(id uint64, keyRange []core.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	conf.cluster.PauseLeaderTransfer(id)
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
	base := NewBaseScheduler(opController)
	handler := newGrantLeaderHandler(conf)
	return &grantLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
}

func (s *grantLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *grantLeaderScheduler) GetName() string {
	return GrantLeaderName
}

func (s *grantLeaderScheduler) GetType() string {
	return GrantLeaderType
}

func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

func (s *grantLeaderScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &grantLeaderSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	pauseAndResumeLeaderTransfer(s.conf.cluster, s.conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	s.conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	return nil
}

func (s *grantLeaderScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	s.conf.RLock()
	defer s.conf.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *grantLeaderScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *grantLeaderScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
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

		op, err := operator.CreateForceTransferLeaderOperator(GrantLeaderType, cluster, region, region.GetLeader().GetStoreId(), id, operator.OpLeader)
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

func (handler *grantLeaderHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
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
			if err := handler.config.cluster.PauseLeaderTransfer(id); err != nil {
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

	handler.config.BuildWithArgs(args)
	err := handler.config.Persist()
	if err != nil {
		handler.config.removeStore(id)
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *grantLeaderHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *grantLeaderHandler) DeleteConfig(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["store_id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	var resp interface{}
	keyRanges := handler.config.getKeyRangesByID(id)
	succ, last := handler.config.removeStore(id)
	if succ {
		err = handler.config.Persist()
		if err != nil {
			handler.config.resetStore(id, keyRanges)
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if last {
			if err := handler.config.removeSchedulerCb(GrantLeaderName); err != nil {
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
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	router.HandleFunc("/delete/{store_id}", h.DeleteConfig).Methods(http.MethodDelete)
	return router
}
