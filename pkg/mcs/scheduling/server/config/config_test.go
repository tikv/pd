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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestPeekStoreLimitByType(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		storeLimit map[uint64]sc.StoreLimitConfig
	}{
		{name: "nil store limits"},
		{name: "empty store limits", storeLimit: map[uint64]sc.StoreLimitConfig{}},
		{
			name: "explicit limits including zero",
			storeLimit: map[uint64]sc.StoreLimitConfig{1: {
				AddPeer: 0, RemovePeer: 180, TransferLeaderIn: 240,
			}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			cfg := &Config{Schedule: sc.ScheduleConfig{
				DefaultStoreLimit: sc.StoreLimitConfig{AddPeer: 30, RemovePeer: 60, TransferLeaderIn: 120},
				StoreLimit:        testCase.storeLimit,
			}}
			expected := cfg.Schedule.Clone()
			persistConfig := NewPersistConfig(cfg, nil)
			for _, storeID := range []uint64{1, 2} {
				limit, ok := expected.StoreLimit[storeID]
				if !ok {
					limit = expected.GetDefaultStoreLimit()
				}
				re.Equal(limit.AddPeer, persistConfig.PeekStoreLimitByType(storeID, storelimit.AddPeer))
				re.Equal(limit.RemovePeer, persistConfig.PeekStoreLimitByType(storeID, storelimit.RemovePeer))
				re.Equal(limit.TransferLeaderIn, persistConfig.PeekStoreLimitByType(storeID, storelimit.TransferLeaderIn))
				re.Zero(persistConfig.PeekStoreLimitByType(storeID, storelimit.SendSnapshot))
			}
			re.Same(&cfg.Schedule, persistConfig.GetScheduleConfig())
			re.Equal(expected, persistConfig.GetScheduleConfig())

			updated := expected.Clone()
			updated.DefaultStoreLimit = sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 90, TransferLeaderIn: 150}
			delete(updated.StoreLimit, 1)
			persistConfig.SetScheduleConfig(updated)
			re.Zero(persistConfig.PeekStoreLimitByType(1, storelimit.AddPeer))
			re.Equal(float64(90), persistConfig.PeekStoreLimitByType(1, storelimit.RemovePeer))
			re.Equal(float64(150), persistConfig.PeekStoreLimitByType(1, storelimit.TransferLeaderIn))
			re.Same(updated, persistConfig.GetScheduleConfig())
			re.NotContains(updated.StoreLimit, uint64(1))
		})
	}
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

func TestPersistConfigDefaultStoreLimit(t *testing.T) {
	re := require.New(t)
	previous := sc.DefaultStoreLimitConfig()
	t.Cleanup(func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, previous.AddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, previous.RemovePeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, previous.TransferLeaderIn)
	})
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 25)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, 35)
	cfg := NewConfig()
	re.NoError(cfg.adjust(nil))
	persistConfig := NewPersistConfig(cfg, nil)
	persistConfig.GetScheduleConfig().StoreLimit[1] = sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 30}

	// Each update must preserve the other types in both store and default limits.
	for _, testCase := range []struct {
		typ             storelimit.Type
		rate            float64
		store, defaults sc.StoreLimitConfig
	}{
		{
			typ:      storelimit.AddPeer,
			rate:     40,
			store:    sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 20, TransferLeaderIn: 30},
			defaults: sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 25, TransferLeaderIn: 35},
		},
		{
			typ:      storelimit.RemovePeer,
			rate:     50,
			store:    sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 50, TransferLeaderIn: 30},
			defaults: sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 50, TransferLeaderIn: 35},
		},
		{
			typ:      storelimit.TransferLeaderIn,
			rate:     60,
			store:    sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 50, TransferLeaderIn: 60},
			defaults: sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 50, TransferLeaderIn: 60},
		},
	} {
		persistConfig.SetAllStoresLimit(testCase.typ, testCase.rate)
		re.Equal(testCase.store, persistConfig.GetStoreLimit(1), testCase.typ.String())
		re.Equal(testCase.defaults, persistConfig.GetScheduleConfig().DefaultStoreLimit, testCase.typ.String())
		re.Equal(testCase.defaults, sc.DefaultStoreLimitConfig(), testCase.typ.String())
	}

	data, err := json.Marshal(persistConfig.GetScheduleConfig())
	re.NoError(err)
	var reloadedScheduleConfig sc.ScheduleConfig
	re.NoError(json.Unmarshal(data, &reloadedScheduleConfig))
	restartedConfig := NewConfig()
	restartedConfig.Schedule = reloadedScheduleConfig
	restartedPersistConfig := NewPersistConfig(restartedConfig, nil)

	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 25)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, 35)
	re.Equal(sc.StoreLimitConfig{AddPeer: 40, RemovePeer: 50, TransferLeaderIn: 60}, restartedPersistConfig.GetStoreLimit(2))
}

func TestAdjustScheduleConfigDefaultStoreLimit(t *testing.T) {
	oldAddPeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemovePeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	oldTransferLeaderIn := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.TransferLeaderIn)
	defer func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemovePeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, oldTransferLeaderIn)
	}()

	testCases := []struct {
		name     string
		config   string
		expected sc.StoreLimitConfig
		stores   map[uint64]sc.StoreLimitConfig
	}{
		{
			name:     "case insensitive fields and null leader limit",
			config:   `{"default-store-limit":{"ADD-PEER":0,"remove-peer":70,"TRANSFER-LEADER-IN":120},"store-limit":{"1":{"ADD-PEER":10,"REMOVE-PEER":20,"TRANSFER-LEADER-IN":300},"2":{"transfer-leader-in":null}}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 70, TransferLeaderIn: 120},
			stores: map[uint64]sc.StoreLimitConfig{
				1: {AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 300},
				2: {TransferLeaderIn: 120},
			},
		},
		{
			name:     "per-store leader limits",
			config:   `{"store-limit":{"1":{"add-peer":10,"remove-peer":20},"2":{"transfer-leader-in":30}}}`,
			expected: sc.StoreLimitConfig{AddPeer: 15, RemovePeer: 15, TransferLeaderIn: storelimit.Unlimited},
			stores: map[uint64]sc.StoreLimitConfig{
				1: {AddPeer: 10, RemovePeer: 20, TransferLeaderIn: storelimit.Unlimited},
				2: {TransferLeaderIn: 30},
			},
		},
		{
			name:     "legacy config without store limit default",
			config:   `{"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 15, RemovePeer: 15, TransferLeaderIn: storelimit.Unlimited},
		},
		{
			name:     "legacy store balance rate",
			config:   `{"store-balance-rate":60,"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 60, TransferLeaderIn: storelimit.Unlimited},
		},
		{
			name:     "explicit zero wins over legacy store balance rate",
			config:   `{"store-balance-rate":60,"default-store-limit":{"add-peer":0,"remove-peer":0},"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 0, TransferLeaderIn: storelimit.Unlimited},
		},
		{
			name:     "legacy store balance rate backfills an omitted field",
			config:   `{"store-balance-rate":60,"default-store-limit":{"add-peer":0},"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 60, TransferLeaderIn: storelimit.Unlimited},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 15)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, storelimit.Unlimited)
			watchedConfig := &persistedConfig{
				Schedule: sc.ScheduleConfig{DefaultStoreLimit: sc.DefaultStoreLimitConfig()},
			}
			re.NoError(json.Unmarshal([]byte(`{"schedule":`+testCase.config+`}`), watchedConfig))
			AdjustScheduleCfg(&watchedConfig.Schedule)
			re.Equal(testCase.expected, watchedConfig.Schedule.DefaultStoreLimit)
			re.Zero(watchedConfig.Schedule.StoreBalanceRate)

			cfg := NewConfig()
			cfg.Schedule = watchedConfig.Schedule
			persistConfig := NewPersistConfig(cfg, nil)
			re.Equal(testCase.expected, persistConfig.GetStoreLimit(100))
			for id, expected := range testCase.stores {
				re.Equal(expected, persistConfig.GetStoreLimit(id))
				re.Equal(expected.TransferLeaderIn, persistConfig.GetStoreLimitByType(id, storelimit.TransferLeaderIn))
			}
		})
	}
}
