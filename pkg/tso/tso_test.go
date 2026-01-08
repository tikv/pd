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

type MokElection struct{}

func (*MokElection) ID() uint64               { return 0 }
func (*MokElection) Name() string             { return "" }
func (*MokElection) MemberValue() string      { return "" }
func (*MokElection) Client() *clientv3.Client { return nil }
func (*MokElection) IsServing() bool          { return true }
func (*MokElection) PromoteSelf()             {}
func (*MokElection) Campaign(_ context.Context, _ int64) error {
	return nil
}
func (*MokElection) Resign()                             {}
func (*MokElection) GetServingUrls() []string            { return nil }
func (*MokElection) GetElectionPath() string             { return "" }
func (*MokElection) GetLeadership() *election.Leadership { return nil }

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
		member:                 &MokElection{},
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
		member:                 &MokElection{},
	}

	runDuration := 5 * time.Second
	timestampOracle.lastSavedTime.Store(current.Add(runDuration))
	runCtx, runCancel := context.WithTimeout(ctx, runDuration-time.Second)
	defer runCancel()
	wg := sync.WaitGroup{}
	wg.Add(100)
	changes := atomic.Int32{}
	totalTso := atomic.Int32{}
	for i := range 100 {
		go func(i int) {
			physical := timestampOracle.tsoMux.physical
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
						if physical != timestampOracle.tsoMux.physical {
							changes.Add(1)
							physical = timestampOracle.tsoMux.physical
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
	re.Equal(totalTso.Load()/int32(maxLogical)+1, changes.Load())
}
