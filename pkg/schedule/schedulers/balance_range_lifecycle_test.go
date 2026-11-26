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

package schedulers

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

type balanceRangeLifecycleStorage struct {
	endpoint.ConfigStorage
	saves   atomic.Int32
	removes atomic.Int32
	load    func(string) (string, error)
}

func (s *balanceRangeLifecycleStorage) SaveSchedulerConfig(name string, data []byte) error {
	s.saves.Add(1)
	return s.ConfigStorage.SaveSchedulerConfig(name, data)
}

func (s *balanceRangeLifecycleStorage) RemoveSchedulerConfig(name string) error {
	s.removes.Add(1)
	return s.ConfigStorage.RemoveSchedulerConfig(name)
}

func (s *balanceRangeLifecycleStorage) LoadSchedulerConfig(name string) (string, error) {
	if s.load != nil {
		return s.load(name)
	}
	return s.ConfigStorage.LoadSchedulerConfig(name)
}

func TestBalanceRangeWatchedConfigLifecycle(t *testing.T) {
	for _, configFirst := range []bool{true, false} {
		name := "runtime-first"
		if configFirst {
			name = "config-first"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			cluster := &mockJobStatusUpdater{Cluster: tc}
			store := &balanceRangeLifecycleStorage{ConfigStorage: storage.NewStorageWithMemoryBackend()}
			// Stop the scheduling loop; these tests drive only config lifecycle.
			ctx, stop := context.WithCancel(t.Context())
			stop()
			controller := NewController(ctx, cluster, store, oc)
			defer controller.Wait()
			create := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
			data, err := EncodeConfig([]*balanceRangeSchedulerJob{{JobID: 0, Create: create, Alias: "watched", Status: pending}})
			re.NoError(err)
			if configFirst {
				re.NoError(store.ConfigStorage.SaveSchedulerConfig(types.BalanceRangeScheduler.String(), data))
			}
			// Both startup JSON and args fallback must start empty, even when
			// the args are invalid or the startup snapshot is stale.
			for _, dec := range []ConfigDecoder{
				ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"invalid"}),
				ConfigJSONDecoderForRestore([]byte("invalid")),
			} {
				s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store,
					ConfigDecoderForCluster(cluster, types.BalanceRangeScheduler, dec))
				re.NoError(err)
				re.Empty(s.(*balanceRangeScheduler).conf.jobs)
			}
			s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store,
				ConfigDecoderForCluster(cluster, types.BalanceRangeScheduler,
					ConfigSliceDecoder(types.BalanceRangeScheduler, nil)))
			re.NoError(err)
			re.NoError(controller.AddScheduler(s))
			if !configFirst {
				re.Empty(s.(*balanceRangeScheduler).conf.jobs)
				re.NoError(store.ConfigStorage.SaveSchedulerConfig(s.GetName(), data))
				re.NoError(controller.ReloadSchedulerConfig(s.GetName()))
			}
			jobs := s.(*balanceRangeScheduler).conf.jobs
			re.Len(jobs, 1)
			re.True(create.Equal(jobs[0].Create))
			cached, err := store.LoadSchedulerConfig(s.GetName())
			re.NoError(err)
			re.Equal(string(data), cached)
			re.Zero(store.saves.Load())

			// A replacement snapshot can arrive before the old runtime is removed.
			replacement := []byte("[] ")
			re.NoError(store.ConfigStorage.SaveSchedulerConfig(s.GetName(), replacement))
			re.NoError(controller.RemoveScheduler(s.GetName()))
			cached, err = store.LoadSchedulerConfig(s.GetName())
			re.NoError(err)
			re.Equal(string(replacement), cached)
			re.Zero(store.removes.Load())
		})
	}
}

func TestBalanceRangeWatchDuringRegistration(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc}
	store := &balanceRangeLifecycleStorage{ConfigStorage: storage.NewStorageWithMemoryBackend()}
	ctx, stop := context.WithCancel(t.Context())
	stop()
	controller := NewController(ctx, cluster, store, oc)
	defer controller.Wait()
	s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store,
		ConfigDecoderForCluster(cluster, types.BalanceRangeScheduler, nil))
	re.NoError(err)
	loaded, release := make(chan struct{}), make(chan struct{})
	releaseRegistration := sync.OnceFunc(func() { close(release) })
	// Registered after controller.Wait's defer, so any failed assertion releases
	// the blocked load before cleanup tries to acquire the controller lock.
	defer releaseRegistration()
	var loads atomic.Int32
	store.load = func(name string) (string, error) {
		data, err := store.ConfigStorage.LoadSchedulerConfig(name)
		if loads.Add(1) == 1 {
			close(loaded)
			<-release
		}
		return data, err
	}
	added := make(chan error, 1)
	go func() { added <- controller.AddScheduler(s) }()
	select {
	case <-loaded:
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not reach the config load")
	}
	// Verify lock coverage directly, without relying on the callback goroutine
	// winning a scheduling race before registration is released below.
	readLocked := controller.TryRLock()
	if readLocked {
		controller.RUnlock()
	}
	// Publish after registration has read the old snapshot but before it is
	// visible in the controller. The watch callback must reload after add.
	data, err := EncodeConfig([]*balanceRangeSchedulerJob{{Create: time.Now(), Alias: "new-version", Status: pending}})
	re.NoError(err)
	re.NoError(store.ConfigStorage.SaveSchedulerConfig(s.GetName(), data))
	reloaded := make(chan error, 1)
	go func() { reloaded <- controller.ReloadSchedulerConfig(s.GetName()) }()
	releaseRegistration()
	select {
	case err := <-added:
		re.NoError(err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not finish after releasing the load")
	}
	select {
	case err := <-reloaded:
		re.NoError(err)
	case <-time.After(5 * time.Second):
		t.Fatal("watch callback did not finish after registration")
	}
	re.False(readLocked, "registration must hold the controller lock while loading the watched config")
	jobs := s.(*balanceRangeScheduler).conf.jobs
	re.Len(jobs, 1)
	re.Equal("new-version", jobs[0].Alias)
	re.Zero(store.saves.Load())
}

func TestBalanceRangeWatchedRegistrationErrors(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc}
	store := &balanceRangeLifecycleStorage{ConfigStorage: storage.NewStorageWithMemoryBackend()}
	ctx, stop := context.WithCancel(t.Context())
	stop()
	controller := NewController(ctx, cluster, store, oc)
	defer controller.Wait()
	s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store,
		ConfigDecoderForCluster(cluster, types.BalanceRangeScheduler, nil))
	re.NoError(err)
	store.load = func(string) (string, error) { return "", errors.New("load failed") }
	re.ErrorContains(controller.AddScheduler(s), "load failed")
	store.load = nil
	re.NoError(store.ConfigStorage.SaveSchedulerConfig(s.GetName(), []byte("invalid")))
	re.Error(controller.AddScheduler(s))
	exists, _ := controller.IsSchedulerExisted(s.GetName())
	re.False(exists)
	re.NoError(store.ConfigStorage.SaveSchedulerConfig(s.GetName(), []byte("[]")))
	re.NoError(controller.AddScheduler(s), "a failed registration must be retryable")
	re.Zero(store.saves.Load())
}

func TestBalanceRangePDConfigLifecycle(t *testing.T) {
	for _, handlerOnly := range []bool{false, true} {
		re := require.New(t)
		cancel, _, tc, oc := prepareSchedulersTest()
		store := &balanceRangeLifecycleStorage{ConfigStorage: storage.NewStorageWithMemoryBackend()}
		ctx, stop := context.WithCancel(t.Context())
		stop()
		controller := NewController(ctx, tc, store, oc)
		dec := ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "pd", "100", "200"})
		s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store,
			ConfigDecoderForCluster(tc, types.BalanceRangeScheduler, dec))
		re.NoError(err)
		re.Len(s.(*balanceRangeScheduler).conf.jobs, 1)
		if handlerOnly {
			re.NoError(controller.AddSchedulerHandler(s))
			re.NoError(controller.RemoveSchedulerHandler(s.GetName()))
		} else {
			re.NoError(controller.AddScheduler(s))
			re.NoError(controller.RemoveScheduler(s.GetName()))
		}
		re.Equal(int32(1), store.saves.Load())
		re.Equal(int32(1), store.removes.Load())
		controller.Wait()
		cancel()
	}
}

func TestBalanceRangeWatchedConfigScope(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc}
	called := false
	dec := func(any) error {
		called = true
		return nil
	}
	_, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigDecoderForCluster(cluster, types.BalanceLeaderScheduler, dec))
	re.NoError(err)
	re.True(called, "other MCS schedulers must keep their original config decoder")
	re.False(IsBalanceRangeConfigWatched(cluster, types.BalanceLeaderScheduler))
	re.False(IsBalanceRangeConfigWatched(tc, types.BalanceRangeScheduler))
}
