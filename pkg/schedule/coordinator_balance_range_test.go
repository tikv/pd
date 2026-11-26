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

package schedule

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type watchedBalanceRangeCluster struct{ *mockcluster.Cluster }

func (*watchedBalanceRangeCluster) UpdateBalanceRangeJobStatus(uint64, time.Time, string) error {
	return schedulers.ErrBalanceRangeJobStatusUnsupported
}

type watchedBalanceRangeStorage struct {
	storage.Storage
	loadErr error
}

func (s *watchedBalanceRangeStorage) LoadSchedulerConfig(name string) (string, error) {
	if s.loadErr != nil {
		return "", s.loadErr
	}
	return s.Storage.LoadSchedulerConfig(name)
}

func TestInitWatchedBalanceRange(t *testing.T) {
	schedulers.Register()
	for _, cached := range []bool{false, true} {
		for _, loadFails := range []bool{false, true} {
			re := require.New(t)
			opts := mockconfig.NewTestOptions()
			cfg := opts.GetScheduleConfig().Clone()
			cfg.Schedulers = sc.SchedulerConfigs{{Type: types.SchedulerTypeCompatibleMap[types.BalanceRangeScheduler], Args: []string{"invalid"}}}
			opts.SetScheduleConfig(cfg)
			cluster := &watchedBalanceRangeCluster{mockcluster.NewCluster(t.Context(), opts)}
			store := &watchedBalanceRangeStorage{Storage: cluster.Storage}
			cluster.Storage = store
			data := []byte(`[{"job-id":0,"alias":"watched","status":"pending","create":"2026-01-02T03:04:05Z"}]`)
			if cached {
				re.NoError(store.SaveSchedulerConfig(types.BalanceRangeScheduler.String(), data))
			}
			if loadFails {
				store.loadErr = errors.New("load failed")
			}
			c := NewCoordinator(t.Context(), cluster, nil)
			c.InitSchedulers(true)
			exists, _ := c.schedulers.IsSchedulerExisted(types.BalanceRangeScheduler.String())
			re.Equal(!loadFails, exists)
			re.Len(opts.GetScheduleConfig().Schedulers, 1, "failed watched cache loads must remain eligible for MCS retry")
			if exists {
				encoded, err := c.schedulers.GetScheduler(types.BalanceRangeScheduler.String()).EncodeConfig()
				re.NoError(err)
				var jobs []map[string]any
				re.NoError(json.Unmarshal(encoded, &jobs))
				if cached {
					re.Len(jobs, 1)
					re.Equal("watched", jobs[0]["alias"])
					re.Equal("2026-01-02T03:04:05Z", jobs[0]["create"])
				} else {
					re.Empty(jobs)
				}
			}
			c.Stop()
			c.schedulers.Wait()
		}
	}
}
