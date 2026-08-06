// Copyright 2026 TiKV Project Authors.
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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestSetScheduleConfigDoesNotBlockWhenSchedulersUpdatingNotifierIsFull(t *testing.T) {
	re := require.New(t)
	persistConfig := NewPersistConfig(&Config{}, nil)
	notifier := make(chan struct{}, 1)
	notifier <- struct{}{}
	persistConfig.SetSchedulersUpdatingNotifier(notifier)

	done := make(chan struct{})
	go func() {
		persistConfig.SetScheduleConfig(newScheduleConfig(types.BalanceLeaderScheduler))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		re.Fail("SetScheduleConfig should not block on a full schedulers updating notifier")
	}
}

func TestClearSchedulersUpdatingNotifier(t *testing.T) {
	re := require.New(t)
	persistConfig := NewPersistConfig(&Config{}, nil)
	oldNotifier := make(chan struct{}, 1)
	newNotifier := make(chan struct{}, 1)

	persistConfig.SetSchedulersUpdatingNotifier(oldNotifier)
	persistConfig.SetSchedulersUpdatingNotifier(newNotifier)
	persistConfig.ClearSchedulersUpdatingNotifier(oldNotifier)
	persistConfig.SetScheduleConfig(newScheduleConfig(types.BalanceLeaderScheduler))

	select {
	case <-oldNotifier:
		re.Fail("old notifier should not receive scheduler update after a newer notifier is registered")
	default:
	}
	select {
	case <-newNotifier:
	case <-time.After(time.Second):
		re.Fail("new notifier should receive scheduler update")
	}

	persistConfig.ClearSchedulersUpdatingNotifier(newNotifier)
	persistConfig.SetScheduleConfig(newScheduleConfig(types.BalanceRegionScheduler))
	select {
	case <-newNotifier:
		re.Fail("cleared notifier should not receive scheduler update")
	default:
	}
}

func newScheduleConfig(schedulerType types.CheckerSchedulerType) *sc.ScheduleConfig {
	return &sc.ScheduleConfig{
		Schedulers: []sc.SchedulerConfig{
			{Type: types.SchedulerTypeCompatibleMap[schedulerType]},
		},
	}
}
