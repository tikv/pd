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
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

type retrySchedulerCluster struct{ *mockcluster.Cluster }

func (*retrySchedulerCluster) UpdateBalanceRangeJobStatus(uint64, time.Time, string) error {
	return schedulers.ErrBalanceRangeJobStatusUnsupported
}

type failFirstSchedulerLoadStorage struct {
	storage.Storage
	loads  atomic.Int32
	failed chan struct{}
}

func (s *failFirstSchedulerLoadStorage) LoadSchedulerConfig(name string) (string, error) {
	if s.loads.Add(1) == 1 {
		close(s.failed)
		return "", errors.New("first scheduler config load failed")
	}
	return s.Storage.LoadSchedulerConfig(name)
}

func TestUpdateSchedulerRetriesBalanceRangeRegistration(t *testing.T) {
	re := require.New(t)
	schedulers.Register()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opts := mockconfig.NewTestOptions()
	cfg := opts.GetScheduleConfig().Clone()
	cfg.Schedulers = nil
	opts.SetScheduleConfig(cfg)
	mock := &retrySchedulerCluster{mockcluster.NewCluster(ctx, opts)}
	store := &failFirstSchedulerLoadStorage{Storage: mock.Storage, failed: make(chan struct{})}
	mock.Storage = store
	coordinator := schedule.NewCoordinator(ctx, mock, nil)
	defer func() {
		coordinator.Stop()
		coordinator.GetSchedulersController().Wait()
	}()
	// Finish initialization with no schedulers, so the injected load failure is
	// consumed by updateScheduler's AddScheduler, not by startup restoration.
	coordinator.InitSchedulers(true)
	re.True(coordinator.AreSchedulersInitialized())
	re.Zero(store.loads.Load())
	create := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	data := []byte(`[{"job-id":0,"alias":"watched","status":"pending","create":"` + create.Format(time.RFC3339Nano) + `"}]`)
	re.NoError(store.SaveSchedulerConfig(types.BalanceRangeScheduler.String(), data))
	cfg.Schedulers = sc.SchedulerConfigs{{Type: types.SchedulerTypeCompatibleMap[types.BalanceRangeScheduler], Args: []string{"ignored"}}}
	c := &Cluster{
		ctx:           ctx,
		storage:       store,
		coordinator:   coordinator,
		persistConfig: config.NewPersistConfig(&config.Config{Schedule: *cfg}, nil),
	}
	c.running.Store(true)
	c.wg.Add(1)
	go c.updateScheduler()
	defer func() {
		cancel()
		c.wg.Wait()
	}()
	select {
	case <-store.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("updateScheduler did not attempt the first config load")
	}
	// No scheduler-list or watcher notification is sent after starting the loop.
	// Only its retry ticker can attempt registration again after the load error.
	controller := coordinator.GetSchedulersController()
	re.Eventually(func() bool {
		exists, _ := controller.IsSchedulerExisted(types.BalanceRangeScheduler.String())
		return exists
	}, 10*time.Second, 10*time.Millisecond)
	re.Equal(int32(2), store.loads.Load())
	encoded, err := controller.GetScheduler(types.BalanceRangeScheduler.String()).EncodeConfig()
	re.NoError(err)
	var jobs []struct {
		Create time.Time `json:"create"`
	}
	re.NoError(json.Unmarshal(encoded, &jobs))
	re.Len(jobs, 1)
	re.True(create.Equal(jobs[0].Create), "ticker retry must load the original watched task identity")
}
