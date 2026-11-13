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
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

const watchedBalanceRangeJobs = `[{"job-id":0,"alias":"first","status":"cancelled"},{"job-id":1,"alias":"second","status":"pending"}]`

func checkBalanceRangeJobConfig(t *testing.T, expected, actual string) {
	t.Helper()
	re := require.New(t)
	var expectedJobs, actualJobs []*balanceRangeSchedulerJob
	re.NoError(json.Unmarshal([]byte(expected), &expectedJobs))
	re.NoError(json.Unmarshal([]byte(actual), &actualJobs))
	re.Equal(expectedJobs, actualJobs)
}

func TestBalanceRangeLoadsWatchedConfig(t *testing.T) {
	for _, data := range []string{watchedBalanceRangeJobs, "[]", "", "invalid"} {
		t.Run(data, func(t *testing.T) {
			re := require.New(t)
			cleanup, _, cluster, oc := prepareSchedulersTest()
			defer cleanup()
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Keep scheduling from mutating the configuration being checked.
			store := storage.NewStorageWithMemoryBackend()
			name := types.BalanceRangeScheduler.String()
			if data != "" {
				re.NoError(store.SaveSchedulerConfig(name, []byte(data)))
			}
			controller := NewController(ctx, cluster, store, oc)
			controller.UsePersistedBalanceRangeConfig()
			defer controller.Wait()
			s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store, ConfigJSONDecoder([]byte("[]")))
			re.NoError(err)
			err = controller.AddScheduler(s)
			if data == "" || data == "invalid" {
				re.Error(err)
				re.Nil(controller.GetScheduler(name))
			} else {
				re.NoError(err)
				encoded, err := s.EncodeConfig()
				re.NoError(err)
				checkBalanceRangeJobConfig(t, data, string(encoded))
			}
			cached, err := store.LoadSchedulerConfig(name)
			re.NoError(err)
			re.Equal(data, cached, "creation must not rewrite watched jobs")
		})
	}
}

type blockedBalanceRangeStorage struct {
	storage.Storage
	once    sync.Once
	loaded  chan struct{}
	release chan struct{}
}

func (s *blockedBalanceRangeStorage) LoadSchedulerConfig(name string) (string, error) {
	data, err := s.Storage.LoadSchedulerConfig(name)
	s.once.Do(func() {
		close(s.loaded)
		<-s.release
	})
	return data, err
}

func TestBalanceRangeUpdateDuringRegistration(t *testing.T) {
	re := require.New(t)
	cleanup, _, cluster, oc := prepareSchedulersTest()
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &blockedBalanceRangeStorage{
		Storage: storage.NewStorageWithMemoryBackend(),
		loaded:  make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(store.release) })
	name := types.BalanceRangeScheduler.String()
	re.NoError(store.SaveSchedulerConfig(name, []byte("[]")))
	controller := NewController(ctx, cluster, store, oc)
	controller.UsePersistedBalanceRangeConfig()
	defer controller.Wait()
	s, err := CreateScheduler(types.BalanceRangeScheduler, oc, store, ConfigJSONDecoder([]byte("[]")))
	re.NoError(err)
	added := make(chan error, 1)
	go func() { added <- controller.AddScheduler(s) }()
	<-store.loaded
	updated := make(chan error, 1)
	go func() { updated <- controller.UpdateSchedulerConfig(name, []byte(watchedBalanceRangeJobs)) }()
	releaseOnce.Do(func() { close(store.release) })
	re.NoError(<-added)
	re.NoError(<-updated)
	encoded, err := s.EncodeConfig()
	re.NoError(err)
	checkBalanceRangeJobConfig(t, watchedBalanceRangeJobs, string(encoded))
	cached, err := store.LoadSchedulerConfig(name)
	re.NoError(err)
	re.JSONEq(watchedBalanceRangeJobs, cached)
}
