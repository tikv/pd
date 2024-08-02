// Copyright 2023 TiKV Project Authors.
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

package mockconfig

import (
	sc "github.com/tikv/pd/pkg/schedule/config"
	types "github.com/tikv/pd/pkg/schedule/type"
	"github.com/tikv/pd/server/config"
)

// oldName2NewType is used to convert old scheduler name to new scheduler type.
// It is just used for testing.
var oldName2NewType = map[string]types.CheckerSchedulerType{
	"balance-leader":   types.BalanceLeaderScheduler,
	"balance-region":   types.BalanceRegionScheduler,
	"hot-region":       types.BalanceHotRegionScheduler,
	"evict-slow-store": types.EvictLeaderScheduler,
}

// NewTestOptions creates default options for testing.
func NewTestOptions() *config.PersistOptions {
	// register default schedulers in case config check fail.
	for _, d := range sc.DefaultSchedulers {
		sc.RegisterScheduler(oldName2NewType[d.Type])
	}
	c := config.NewConfig()
	c.Adjust(nil, false)
	return config.NewPersistOptions(c)
}
