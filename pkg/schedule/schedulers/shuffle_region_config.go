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

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	roleLeader   = string(placement.Leader)
	roleFollower = string(placement.Follower)
	roleLearner  = string(placement.Learner)
)

var allRoles = []string{roleLeader, roleFollower, roleLearner}

type shuffleRegionSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	Ranges []core.KeyRange `json:"ranges"`
	Roles  []string        `json:"roles"` // can include `leader`, `follower`, `learner`.
}

func (conf *shuffleRegionSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *shuffleRegionSchedulerConfig) getRoles() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Roles
}

func (conf *shuffleRegionSchedulerConfig) getRanges() []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return ranges
}

func (conf *shuffleRegionSchedulerConfig) isRoleAllow(role string) bool {
	conf.RLock()
	defer conf.RUnlock()
	return slice.AnyOf(conf.Roles, func(i int) bool { return conf.Roles[i] == role })
}

// ServeHTTP implements the http.Handler interface.
func (conf *shuffleRegionSchedulerConfig) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	router := mux.NewRouter()
	router.HandleFunc("/list", conf.handleGetRoles).Methods(http.MethodGet)
	router.HandleFunc("/roles", conf.handleGetRoles).Methods(http.MethodGet)
	router.HandleFunc("/roles", conf.handleSetRoles).Methods(http.MethodPost)
	router.ServeHTTP(w, r)
}

func (conf *shuffleRegionSchedulerConfig) handleGetRoles(w http.ResponseWriter, _ *http.Request) {
	rd := render.New(render.Options{IndentJSON: true})
	rd.JSON(w, http.StatusOK, conf.getRoles())
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
	if err := conf.save(); err != nil {
		conf.Roles = old // revert
		rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	rd.Text(w, http.StatusOK, "Config is updated.")
}
