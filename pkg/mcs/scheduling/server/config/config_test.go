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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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

func TestPersistConfigCheckLabelProperty(t *testing.T) {
	re := require.New(t)
	persistConfig := NewPersistConfig(&Config{}, nil)
	labels := []*metapb.StoreLabel{{Key: "zone", Value: "cn1"}, {Key: "engine", Value: "tiflash"}}

	re.False(persistConfig.CheckLabelProperty(sc.RejectLeader, labels))
	persistConfig.SetLabelPropertyConfig(sc.LabelPropertyConfig{
		sc.RejectLeader: {
			{Key: "zone", Value: "cn2"},
			{Key: "engine", Value: "tiflash"},
		},
		"other": {{Key: "disk", Value: "hdd"}},
	})
	for _, testCase := range []struct {
		name     string
		property string
		labels   []*metapb.StoreLabel
		expected bool
	}{
		{
			name:     "any configured label matches any store label",
			property: sc.RejectLeader,
			labels:   labels,
			expected: true,
		},
		{
			name:     "key matches but value differs",
			property: sc.RejectLeader,
			labels:   []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}},
		},
		{
			name:     "value matches but key differs",
			property: sc.RejectLeader,
			labels:   []*metapb.StoreLabel{{Key: "other", Value: "tiflash"}},
		},
		{
			name:     "key and value match different store labels",
			property: sc.RejectLeader,
			labels:   []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}, {Key: "other", Value: "tiflash"}},
		},
		{
			name:     "case sensitive key",
			property: sc.RejectLeader,
			labels:   []*metapb.StoreLabel{{Key: "Engine", Value: "tiflash"}},
		},
		{
			name:     "case sensitive value",
			property: sc.RejectLeader,
			labels:   []*metapb.StoreLabel{{Key: "engine", Value: "TiFlash"}},
		},
		{
			name:     "unknown property",
			property: "unknown",
			labels:   labels,
		},
		{
			name:     "another property",
			property: "other",
			labels:   []*metapb.StoreLabel{{Key: "disk", Value: "hdd"}},
			expected: true,
		},
		{
			name:     "no store labels",
			property: sc.RejectLeader,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, persistConfig.CheckLabelProperty(testCase.property, testCase.labels))
		})
	}

	persistConfig.SetLabelPropertyConfig(sc.LabelPropertyConfig{})
	re.False(persistConfig.CheckLabelProperty(sc.RejectLeader, labels))
	persistConfig.SetLabelPropertyConfig(nil)
	re.False(persistConfig.CheckLabelProperty(sc.RejectLeader, labels))
}

func TestWatcherConfigLabelProperty(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		config   string
		expected bool
	}{
		{
			name:     "replace property labels",
			config:   `{"label-property":{"reject-leader":[{"key":"zone","value":"cn1"}]}}`,
			expected: true,
		},
		{
			name:   "remove property",
			config: `{"label-property":{"other":[{"key":"engine","value":"tiflash"}]}}`,
		},
		{
			name:   "empty config",
			config: `{"label-property":{}}`,
		},
		{
			name:   "null config",
			config: `{"label-property":null}`,
		},
		{
			name:   "missing config",
			config: `{}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			watcher := &Watcher{PersistConfig: NewPersistConfig(&Config{}, nil)}
			labels := []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}
			re.NoError(watcher.updateConfig(&mvccpb.KeyValue{
				Value: []byte(`{"label-property":{"reject-leader":[{"key":"engine","value":"tiflash"}]}}`),
			}))
			re.True(watcher.CheckLabelProperty(sc.RejectLeader, labels))

			re.NoError(watcher.updateConfig(&mvccpb.KeyValue{Value: []byte(testCase.config)}))
			re.False(watcher.CheckLabelProperty(sc.RejectLeader, labels))
			re.Equal(testCase.expected, watcher.CheckLabelProperty(sc.RejectLeader, []*metapb.StoreLabel{{Key: "zone", Value: "cn1"}}))
		})
	}
}
