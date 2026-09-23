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

package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/config"
)

func TestPartialScheduleConfigUpdatesPreserveLatestFields(t *testing.T) {
	re := require.New(t)
	oldDefaultStoreLimit := sc.DefaultStoreLimitConfig()
	t.Cleanup(func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldDefaultStoreLimit.AddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldDefaultStoreLimit.RemovePeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, oldDefaultStoreLimit.TransferLeaderIn)
	})
	cfg := config.NewConfig()
	re.NoError(cfg.Adjust(nil, false))
	store := storage.NewStorageWithMemoryBackend()
	s := &Server{
		persistOptions: config.NewPersistOptions(cfg),
		storage:        store,
	}

	defaultLimit := s.GetScheduleConfig().DefaultStoreLimit.AddPeer + 30
	re.NoError(s.persistOptions.UpdateScheduleConfig(store, func(_ *sc.ScheduleConfig, next *sc.ScheduleConfig) (bool, error) {
		next.DefaultStoreLimit.AddPeer = defaultLimit
		return true, nil
	}))
	re.NoError(s.PatchScheduleConfig([]byte(`{"max-snapshot-count":99}`)))
	re.NoError(s.SetScheduleConfigItem("max-pending-peer-count", float64(88)))
	re.NoError(s.SetScheduleConfigItem("default-store-limit", map[string]float64{"transfer-leader-in": 30}))
	re.NoError(s.SetScheduleConfigItem("store-limit", map[uint64]map[string]float64{
		1: {"add-peer": 10, "remove-peer": 20},
	}))

	current := s.GetScheduleConfig()
	re.Equal(defaultLimit, current.DefaultStoreLimit.AddPeer)
	re.Equal(uint64(99), current.MaxSnapshotCount)
	re.Equal(uint64(88), current.MaxPendingPeerCount)
	re.Equal(sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 30}, current.StoreLimit[1])

	reloaded := config.NewPersistOptions(config.NewConfig())
	re.NoError(reloaded.Reload(store))
	re.Equal(defaultLimit, reloaded.GetScheduleConfig().DefaultStoreLimit.AddPeer)
	re.Equal(uint64(99), reloaded.GetScheduleConfig().MaxSnapshotCount)
	re.Equal(uint64(88), reloaded.GetScheduleConfig().MaxPendingPeerCount)
	re.Equal(current.StoreLimit[1], reloaded.GetStoreLimit(1))
}

func TestConcurrentStoreLimitPartialUpdates(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	re.NoError(cfg.Adjust(nil, false))
	cfg.Schedule.StoreLimit[1] = sc.StoreLimitConfig{}
	store := storage.NewStorageWithMemoryBackend()
	s := &Server{persistOptions: config.NewPersistOptions(cfg), storage: store}
	patches := []string{
		`{"store-limit":{"1":{"add-peer":10}}}`,
		`{"store-limit":{"1":{"remove-peer":20}}}`,
		`{"store-limit":{"1":{"transfer-leader-in":30}}}`,
	}
	start := make(chan struct{})
	errs := make(chan error, len(patches))
	for _, patch := range patches {
		go func() {
			<-start
			errs <- s.PatchScheduleConfig([]byte(patch))
		}()
	}
	close(start)
	for range patches {
		re.NoError(<-errs)
	}
	expected := sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 30}
	re.Equal(expected, s.GetScheduleConfig().StoreLimit[1])
	reloaded := config.NewPersistOptions(config.NewConfig())
	re.NoError(reloaded.Reload(store))
	re.Equal(expected, reloaded.GetStoreLimit(1))
}

func TestConcurrentPartialScheduleConfigUpdatesDoNotConflict(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	re.NoError(cfg.Adjust(nil, false))
	store := storage.NewStorageWithMemoryBackend()
	s := &Server{
		persistOptions: config.NewPersistOptions(cfg),
		storage:        store,
	}

	const updates = 16
	start := make(chan struct{})
	errCh := make(chan error, updates*2)
	var wg sync.WaitGroup
	for i := range updates {
		wg.Add(2)
		go func(value int) {
			defer wg.Done()
			<-start
			errCh <- s.PatchScheduleConfig([]byte(fmt.Sprintf(`{"max-snapshot-count":%d}`, value+1)))
		}(i)
		go func(value int) {
			defer wg.Done()
			<-start
			errCh <- s.persistOptions.UpdateScheduleConfig(store, func(_ *sc.ScheduleConfig, next *sc.ScheduleConfig) (bool, error) {
				next.DefaultStoreLimit.AddPeer = float64(value + 30)
				return true, nil
			})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		re.NoError(err)
	}

	current := s.GetScheduleConfig()
	reloaded := config.NewPersistOptions(config.NewConfig())
	re.NoError(reloaded.Reload(store))
	re.Equal(current.DefaultStoreLimit, reloaded.GetScheduleConfig().DefaultStoreLimit)
	re.Equal(current.MaxSnapshotCount, reloaded.GetScheduleConfig().MaxSnapshotCount)
}
