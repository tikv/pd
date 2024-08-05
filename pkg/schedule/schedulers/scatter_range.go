// Copyright 2018 TiKV Project Authors.
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
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	types "github.com/tikv/pd/pkg/schedule/type"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
)

const (
	// ScatterRangeType is scatter range scheduler type
	ScatterRangeType = "scatter-range"
	// ScatterRangeName is scatter range scheduler name
	ScatterRangeName = "scatter-range"
)

type scatterRangeSchedulerConfig struct {
	syncutil.RWMutex
	storage   endpoint.ConfigStorage
	RangeName string `json:"range-name"`
	StartKey  string `json:"start-key"`
	EndKey    string `json:"end-key"`
}

func (conf *scatterRangeSchedulerConfig) buildWithArgs(args []string) error {
	if len(args) != 3 {
		return errs.ErrSchedulerConfig.FastGenByArgs("ranges and name")
	}
	conf.Lock()
	defer conf.Unlock()

	conf.RangeName = args[0]
	conf.StartKey = args[1]
	conf.EndKey = args[2]
	return nil
}

func (conf *scatterRangeSchedulerConfig) clone() *scatterRangeSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &scatterRangeSchedulerConfig{
		StartKey:  conf.StartKey,
		EndKey:    conf.EndKey,
		RangeName: conf.RangeName,
	}
}

func (conf *scatterRangeSchedulerConfig) persist() error {
	name := conf.getSchedulerName()
	conf.RLock()
	defer conf.RUnlock()
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *scatterRangeSchedulerConfig) getRangeName() string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.RangeName
}

func (conf *scatterRangeSchedulerConfig) getStartKey() []byte {
	conf.RLock()
	defer conf.RUnlock()
	return []byte(conf.StartKey)
}

func (conf *scatterRangeSchedulerConfig) getEndKey() []byte {
	conf.RLock()
	defer conf.RUnlock()
	return []byte(conf.EndKey)
}

func (conf *scatterRangeSchedulerConfig) getSchedulerName() string {
	conf.RLock()
	defer conf.RUnlock()
	return fmt.Sprintf("scatter-range-%s", conf.RangeName)
}

type scatterRangeScheduler struct {
	*BaseScheduler
	config        *scatterRangeSchedulerConfig
	balanceLeader Scheduler
	balanceRegion Scheduler
	handler       http.Handler
}

// newScatterRangeScheduler creates a scheduler that balances the distribution of leaders and regions that in the specified key range.
func newScatterRangeScheduler(opController *operator.Controller, config *scatterRangeSchedulerConfig) Scheduler {
	base := NewBaseScheduler(opController, types.ScatterRangeScheduler)

	handler := newScatterRangeHandler(config)
	scheduler := &scatterRangeScheduler{
		BaseScheduler: base,
		config:        config,
		handler:       handler,
		balanceLeader: newBalanceLeaderScheduler(
			opController,
			&balanceLeaderSchedulerConfig{Ranges: []core.KeyRange{core.NewKeyRange("", "")}},
			// the name will not be persisted
			WithBalanceLeaderName("scatter-range-leader"),
		),
		balanceRegion: newBalanceRegionScheduler(
			opController,
			&balanceRegionSchedulerConfig{Ranges: []core.KeyRange{core.NewKeyRange("", "")}},
			// the name will not be persisted
			WithBalanceRegionName("scatter-range-region"),
		),
	}
	scheduler.name = config.getSchedulerName()
	return scheduler
}

// ServeHTTP implements the http.Handler interface.
func (l *scatterRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.ServeHTTP(w, r)
}

// EncodeConfig implements the Scheduler interface.
func (l *scatterRangeScheduler) EncodeConfig() ([]byte, error) {
	l.config.RLock()
	defer l.config.RUnlock()
	return EncodeConfig(l.config)
}

// ReloadConfig implements the Scheduler interface.
func (l *scatterRangeScheduler) ReloadConfig() error {
	l.config.Lock()
	defer l.config.Unlock()
	cfgData, err := l.config.storage.LoadSchedulerConfig(l.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &scatterRangeSchedulerConfig{}
	if err := DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	l.config.RangeName = newCfg.RangeName
	l.config.StartKey = newCfg.StartKey
	l.config.EndKey = newCfg.EndKey
	return nil
}

// IsScheduleAllowed implements the Scheduler interface.
func (l *scatterRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	return l.allowBalanceLeader(cluster) || l.allowBalanceRegion(cluster)
}

func (l *scatterRangeScheduler) allowBalanceLeader(cluster sche.SchedulerCluster) bool {
	allowed := l.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(l.GetType(), operator.OpLeader)
	}
	return allowed
}

func (l *scatterRangeScheduler) allowBalanceRegion(cluster sche.SchedulerCluster) bool {
	allowed := l.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(l.GetType(), operator.OpRegion)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (l *scatterRangeScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	scatterRangeCounter.Inc()
	// isolate a new cluster according to the key range
	c := genRangeCluster(cluster, l.config.getStartKey(), l.config.getEndKey())
	c.SetTolerantSizeRatio(2)
	if l.allowBalanceLeader(cluster) {
		ops, _ := l.balanceLeader.Schedule(c, false)
		if len(ops) > 0 {
			ops[0].SetDesc(fmt.Sprintf("scatter-range-leader-%s", l.config.getRangeName()))
			ops[0].AttachKind(operator.OpRange)
			ops[0].Counters = append(ops[0].Counters,
				scatterRangeNewOperatorCounter,
				scatterRangeNewLeaderOperatorCounter)
			return ops, nil
		}
		scatterRangeNoNeedBalanceLeaderCounter.Inc()
	}
	if l.allowBalanceRegion(cluster) {
		ops, _ := l.balanceRegion.Schedule(c, false)
		if len(ops) > 0 {
			ops[0].SetDesc(fmt.Sprintf("scatter-range-region-%s", l.config.getRangeName()))
			ops[0].AttachKind(operator.OpRange)
			ops[0].Counters = append(ops[0].Counters,
				scatterRangeNewOperatorCounter,
				scatterRangeNewRegionOperatorCounter)
			return ops, nil
		}
		scatterRangeNoNeedBalanceRegionCounter.Inc()
	}

	return nil, nil
}

type scatterRangeHandler struct {
	rd     *render.Render
	config *scatterRangeSchedulerConfig
}

func (handler *scatterRangeHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	name, ok := input["range-name"].(string)
	if ok {
		if name != handler.config.getRangeName() {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("Cannot change the range name, please delete this schedule").Error())
			return
		}
		args = append(args, name)
	} else {
		args = append(args, handler.config.getRangeName())
	}

	startKey, ok := input["start-key"].(string)
	if ok {
		args = append(args, startKey)
	} else {
		args = append(args, string(handler.config.getStartKey()))
	}

	endKey, ok := input["end-key"].(string)
	if ok {
		args = append(args, endKey)
	} else {
		args = append(args, string(handler.config.getEndKey()))
	}
	err := handler.config.buildWithArgs(args)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	err = handler.config.persist()
	if err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *scatterRangeHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func newScatterRangeHandler(config *scatterRangeSchedulerConfig) http.Handler {
	h := &scatterRangeHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	return router
}
