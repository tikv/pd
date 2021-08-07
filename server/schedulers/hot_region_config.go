// Copyright 2020 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
)

const (
	// BytePriority indicates hot-region-scheduler prefer byte dim
	BytePriority = "byte"
	// KeyPriority indicates hot-region-scheduler prefer key dim
	KeyPriority = "key"
	// QueryPriority indicates hot-region-scheduler prefer query dim
	QueryPriority = "qps"

	// Scheduling has a bigger impact on TiFlash, so it needs to be corrected in configuration items
	// In the default config, the TiKV difference is 1.05*1.05-1 = 0.1025, and the TiFlash difference is 1.15*1.15-1 = 0.3225
	tiflashToleranceRatioCorrection = 0.1
)

var defaultConfig = prioritiesConfig{
	readLeader:  []string{QueryPriority, BytePriority},
	writeLeader: []string{KeyPriority, BytePriority},
	writePeer:   []string{BytePriority, KeyPriority},
}

// because tikv below 5.2.0 does not report query information, we will use byte and key as the scheduling dimensions
var lowConfig = prioritiesConfig{
	readLeader:  []string{BytePriority, KeyPriority},
	writeLeader: []string{KeyPriority, BytePriority},
	writePeer:   []string{BytePriority, KeyPriority},
}

// params about hot region.
func initHotRegionScheduleConfig() *hotRegionSchedulerConfig {
	return &hotRegionSchedulerConfig{
		MinHotByteRate:         100,
		MinHotKeyRate:          10,
		MinHotQueryRate:        10,
		MaxZombieRounds:        3,
		MaxPeerNum:             1000,
		ByteRateRankStepRatio:  0.05,
		KeyRateRankStepRatio:   0.05,
		QueryRateRankStepRatio: 0.05,
		CountRankStepRatio:     0.01,
		GreatDecRatio:          0.95,
		MinorDecRatio:          0.99,
		SrcToleranceRatio:      1.05, // Tolerate 5% difference
		DstToleranceRatio:      1.05, // Tolerate 5% difference
		ReadPriorities:         defaultConfig.readLeader,
		WriteLeaderPriorities:  defaultConfig.writeLeader,
		WritePeerPriorities:    defaultConfig.writePeer,
		StrictPickingStore:     true,
		EnableForTiFlash:       true,
	}
}

type hotRegionSchedulerConfig struct {
	sync.RWMutex
	storage *core.Storage

	MinHotByteRate  float64 `json:"min-hot-byte-rate"`
	MinHotKeyRate   float64 `json:"min-hot-key-rate"`
	MinHotQueryRate float64 `json:"min-hot-query-rate"`
	MaxZombieRounds int     `json:"max-zombie-rounds"`
	MaxPeerNum      int     `json:"max-peer-number"`

	// rank step ratio decide the step when calculate rank
	// step = max current * rank step ratio
	ByteRateRankStepRatio  float64  `json:"byte-rate-rank-step-ratio"`
	KeyRateRankStepRatio   float64  `json:"key-rate-rank-step-ratio"`
	QueryRateRankStepRatio float64  `json:"query-rate-rank-step-ratio"`
	CountRankStepRatio     float64  `json:"count-rank-step-ratio"`
	GreatDecRatio          float64  `json:"great-dec-ratio"`
	MinorDecRatio          float64  `json:"minor-dec-ratio"`
	SrcToleranceRatio      float64  `json:"src-tolerance-ratio"`
	DstToleranceRatio      float64  `json:"dst-tolerance-ratio"`
	ReadPriorities         []string `json:"read-priorities"`

	// For first priority of write leader, it is better to consider key rate or query rather than byte
	WriteLeaderPriorities []string `json:"write-leader-priorities"`
	WritePeerPriorities   []string `json:"write-peer-priorities"`
	StrictPickingStore    bool     `json:"strict-picking-store,string"`

	// Separately control whether to start hotspot scheduling for TiFlash
	EnableForTiFlash bool `json:"enable-for-tiflash,string"`
}

func (conf *hotRegionSchedulerConfig) EncodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return schedule.EncodeConfig(conf)
}

func (conf *hotRegionSchedulerConfig) GetStoreStatZombieDuration() time.Duration {
	conf.RLock()
	defer conf.RUnlock()
	return time.Duration(conf.MaxZombieRounds) * statistics.StoreHeartBeatReportInterval * time.Second
}

func (conf *hotRegionSchedulerConfig) GetRegionsStatZombieDuration() time.Duration {
	conf.RLock()
	defer conf.RUnlock()
	return time.Duration(conf.MaxZombieRounds) * statistics.RegionHeartBeatReportInterval * time.Second
}

func (conf *hotRegionSchedulerConfig) GetMaxPeerNumber() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MaxPeerNum
}

func (conf *hotRegionSchedulerConfig) GetSrcToleranceRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.SrcToleranceRatio
}

func (conf *hotRegionSchedulerConfig) SetSrcToleranceRatio(tol float64) {
	conf.Lock()
	defer conf.Unlock()
	conf.SrcToleranceRatio = tol
}

func (conf *hotRegionSchedulerConfig) GetDstToleranceRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.DstToleranceRatio
}

func (conf *hotRegionSchedulerConfig) SetDstToleranceRatio(tol float64) {
	conf.Lock()
	defer conf.Unlock()
	conf.DstToleranceRatio = tol
}

func (conf *hotRegionSchedulerConfig) GetByteRankStepRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.ByteRateRankStepRatio
}

func (conf *hotRegionSchedulerConfig) GetKeyRankStepRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.KeyRateRankStepRatio
}

func (conf *hotRegionSchedulerConfig) GetQueryRateRankStepRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.QueryRateRankStepRatio
}

func (conf *hotRegionSchedulerConfig) GetCountRankStepRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.CountRankStepRatio
}

func (conf *hotRegionSchedulerConfig) GetGreatDecRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.GreatDecRatio
}

func (conf *hotRegionSchedulerConfig) GetMinorGreatDecRatio() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MinorDecRatio
}

func (conf *hotRegionSchedulerConfig) GetMinHotKeyRate() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MinHotKeyRate
}

func (conf *hotRegionSchedulerConfig) GetMinHotByteRate() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MinHotByteRate
}

func (conf *hotRegionSchedulerConfig) GetEnableForTiFlash() bool {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EnableForTiFlash
}

func (conf *hotRegionSchedulerConfig) SetEnableForTiFlash(enable bool) {
	conf.RLock()
	defer conf.RUnlock()
	conf.EnableForTiFlash = enable
}

func (conf *hotRegionSchedulerConfig) GetMinHotQueryRate() float64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MinHotQueryRate
}

func (conf *hotRegionSchedulerConfig) GetReadPriorities() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.ReadPriorities
}

func (conf *hotRegionSchedulerConfig) GetWriteLeaderPriorities() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.WriteLeaderPriorities
}

func (conf *hotRegionSchedulerConfig) GetWritePeerPriorities() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.WritePeerPriorities
}

func (conf *hotRegionSchedulerConfig) IsStrictPickingStoreEnabled() bool {
	conf.RLock()
	defer conf.RUnlock()
	return conf.StrictPickingStore
}

func (conf *hotRegionSchedulerConfig) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	router := mux.NewRouter()
	router.HandleFunc("/list", conf.handleGetConfig).Methods("GET")
	router.HandleFunc("/config", conf.handleSetConfig).Methods("POST")
	router.ServeHTTP(w, r)
}

func (conf *hotRegionSchedulerConfig) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	conf.RLock()
	defer conf.RUnlock()
	rd := render.New(render.Options{IndentJSON: true})
	rd.JSON(w, http.StatusOK, conf)
}

func (conf *hotRegionSchedulerConfig) handleSetConfig(w http.ResponseWriter, r *http.Request) {
	conf.Lock()
	defer conf.Unlock()
	rd := render.New(render.Options{IndentJSON: true})
	oldc, _ := json.Marshal(conf)
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := json.Unmarshal(data, conf); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	newc, _ := json.Marshal(conf)
	if !bytes.Equal(oldc, newc) {
		conf.persist()
		rd.Text(w, http.StatusOK, "success")
	}

	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	t := reflect.TypeOf(conf).Elem()
	for i := 0; i < t.NumField(); i++ {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if _, ok := m[jsonTag]; ok {
			rd.Text(w, http.StatusOK, "no changed")
			return
		}
	}

	rd.Text(w, http.StatusBadRequest, "config item not found")
}

func (conf *hotRegionSchedulerConfig) persist() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err

	}
	return conf.storage.SaveScheduleConfig(HotRegionName, data)
}

type prioritiesConfig struct {
	readLeader  []string
	writeLeader []string
	writePeer   []string
}

func getReadLeaderPriorities(c *prioritiesConfig) []string {
	return c.readLeader
}

func getWriteLeaderPriorities(c *prioritiesConfig) []string {
	return c.writeLeader
}

func getWritePeerPriorities(c *prioritiesConfig) []string {
	return c.writePeer
}
