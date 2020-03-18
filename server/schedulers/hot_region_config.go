// Copyright 2020 PingCAP, Inc.
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
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/statistics"
	"github.com/unrolled/render"
)

// params about hot region.
func initHotRegionScheduleConfig() *hotRegionSchedulerConfig {
	return &hotRegionSchedulerConfig{
		MinHotByteRate:        100,
		MinHotKeyRate:         10,
		MaxZombieRounds:       3,
		ByteRateRankStepRatio: 0.05,
		KeyRateRankStepRatio:  0.05,
		CountRankStepRatio:    0.01,
		GreatDecRatio:         0.95,
		MinorDecRatio:         0.99,
		MaxPeerNum:            1000,
	}
}

type hotRegionSchedulerConfig struct {
	sync.RWMutex
	storage *core.Storage

	MinHotByteRate  float64 `json:"min-hot-byte-rate"`
	MinHotKeyRate   float64 `json:"min-hot-key-rate"`
	MaxZombieRounds int     `json:"max-zombie-rounds"`
	MaxPeerNum      int     `json:"max-peer-number"`

	// rank step ratio decide the step when calculate rank
	// step = max current * rank step ratio
	ByteRateRankStepRatio float64 `json:"byte-rate-rank-step-ratio"`
	KeyRateRankStepRatio  float64 `json:"key-rate-rank-step-ratio"`
	CountRankStepRatio    float64 `json:"count-rank-step-ratio"`
	GreatDecRatio         float64 `json:"great-dec-ratio"`
	MinorDecRatio         float64 `json:"minor-dec-ratio"`
}

func (conf *hotRegionSchedulerConfig) EncodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return schedule.EncodeConfig(conf)
}

func (conf *hotRegionSchedulerConfig) GetMaxZombieDuration() time.Duration {
	conf.RLock()
	defer conf.RUnlock()
	return time.Duration(conf.MaxZombieRounds) * statistics.StoreHeartBeatReportInterval * time.Second
}

func (conf *hotRegionSchedulerConfig) GetMaxPeerNumber() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.MaxPeerNum
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
	data, err := ioutil.ReadAll(r.Body)
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
