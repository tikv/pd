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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/unrolled/render"
)

const (
	roleLeader   = string(placement.Leader)
	roleFollower = string(placement.Follower)
	roleLearner  = string(placement.Learner)
)

var allRoles = []string{roleLeader, roleFollower, roleLearner}

type shuffleRegionSchedulerConfig struct {
	sync.RWMutex
	storage *core.Storage

	Ranges []core.KeyRange `json:"ranges"`
	Roles  []string        `json:"roles"` // can include `leader`, `follower`, `learner`.
}

func (conf *shuffleRegionSchedulerConfig) EncodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return schedule.EncodeConfig(conf)
}

func (conf *shuffleRegionSchedulerConfig) GetRoles() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Roles
}

func (conf *shuffleRegionSchedulerConfig) GetRanges() []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Ranges
}

func (conf *shuffleRegionSchedulerConfig) IsRoleAllow(role string) bool {
	conf.RLock()
	defer conf.RUnlock()
	return slice.AnyOf(conf.Roles, func(i int) bool { return conf.Roles[i] == role })
}

func (conf *shuffleRegionSchedulerConfig) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	router := mux.NewRouter()
	router.HandleFunc("/roles", conf.handleGetRoles).Methods("GET")
	router.HandleFunc("/roles", conf.handleSetRoles).Methods("POST")
	router.ServeHTTP(w, r)
}

func (conf *shuffleRegionSchedulerConfig) handleGetRoles(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{IndentJSON: true})
	rd.JSON(w, http.StatusOK, conf.GetRoles())
}

func (conf *shuffleRegionSchedulerConfig) handleSetRoles(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{IndentJSON: true})
	var roles []string
	if err := apiutil.ReadJSONRespondError(rd, w, r.Body, &roles); err != nil {
		return
	}
	for _, r := range roles {
		if slice.NoneOf(allRoles, func(i int) bool { return allRoles[i] == r }) {
			rd.Text(w, http.StatusBadRequest, "invalid role:"+r)
			return
		}
	}

	conf.Lock()
	defer conf.Unlock()
	old := conf.Roles
	conf.Roles = roles
	if err := conf.persist(); err != nil {
		conf.Roles = old // revert
		rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	rd.Text(w, http.StatusOK, "")
}

func (conf *shuffleRegionSchedulerConfig) persist() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(ShuffleRegionName, data)
}
