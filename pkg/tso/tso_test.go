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

package tso

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/election"
)

type MockElection struct{}

func (*MockElection) ID() uint64               { return 0 }
func (*MockElection) Name() string             { return "" }
func (*MockElection) MemberValue() string      { return "" }
func (*MockElection) Client() *clientv3.Client { return nil }
func (*MockElection) IsServing() bool          { return true }
func (*MockElection) PromoteSelf()             {}
func (*MockElection) Campaign(_ context.Context, _ int64) error {
	return nil
}
func (*MockElection) Resign()                             {}
func (*MockElection) GetServingUrls() []string            { return nil }
func (*MockElection) GetElectionPath() string             { return "" }
func (*MockElection) GetLeadership() *election.Leadership { return nil }

func TestGenerateTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := time.Now()
	timestampOracle := &timestampOracle{
		tsoMux: &tsoObject{
			physical: current,
			logical:  maxLogical - 1,
		},
		saveInterval:           50 * time.Millisecond,
		updatePhysicalInterval: 5 * time.Second,
		maxResetTSGap:          func() time.Duration { return time.Hour },
		metrics:                newTSOMetrics("test"),
		member:                 &MockElection{},
	}

	// update physical time interval failed due to reach the lastSavedTime, it needs to save storage first, but this behavior is not allowed.
	_, err := timestampOracle.getTS(ctx, 2)
	re.Error(err)
	re.Equal(current, timestampOracle.tsoMux.physical)

	// simulate the save to storage operation is done.
	timestampOracle.lastSavedTime.Store(current.Add(5 * time.Second))
	_, err = timestampOracle.getTS(ctx, 2)
	re.NoError(err)
	re.NotEqual(current, timestampOracle.tsoMux.physical)
}

func TestCurrentGetTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := time.Now()
	timestampOracle := &timestampOracle{
		tsoMux: &tsoObject{
			physical: current,
			logical:  maxLogical - 1,
		},
		saveInterval:           50 * time.Millisecond,
		updatePhysicalInterval: 5 * time.Second,
		maxResetTSGap:          func() time.Duration { return time.Hour },
		metrics:                newTSOMetrics("test"),
		member:                 &MockElection{},
	}

	runDuration := 5 * time.Second
	timestampOracle.lastSavedTime.Store(current.Add(runDuration))
	runCtx, runCancel := context.WithTimeout(ctx, runDuration-time.Second)
	defer runCancel()
	wg := sync.WaitGroup{}
	wg.Add(10)
	changes := atomic.Int32{}
	totalTso := atomic.Int32{}
	for i := range 10 {
		go func(i int) {
			pre, _ := timestampOracle.getTSO()
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				default:
					_, err := timestampOracle.getTS(runCtx, 1)
					totalTso.Add(1)
					re.NoError(err)
					if i == 0 {
						physical, _ := timestampOracle.getTSO()
						if pre != physical {
							changes.Add(1)
							pre = physical
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
	re.Equal(totalTso.Load()/int32(maxLogical)+1, changes.Load())
}
