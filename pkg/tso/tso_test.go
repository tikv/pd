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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func mockLeadership() *election.Leadership {
	leadership := &election.Leadership{}
	lease := &election.Lease{}
	expireTime := reflect.ValueOf(lease).Elem().FieldByName("expireTime")
	expireTime = reflect.NewAt(expireTime.Type(), unsafe.Pointer(expireTime.UnsafeAddr())).Elem()
	expireTime.Addr().Interface().(*atomic.Value).Store(time.Now().Add(time.Hour))
	leadership.SetLease(lease)
	return leadership
}

func TestLogicalOverflow(t *testing.T) {
	re := require.New(t)
	re.True(overflowedLogical(maxLogical))
	re.False(overflowedLogical(maxLogical - 1))
}

func TestSetTSO(t *testing.T) {
	re := require.New(t)
	ts := &timestampOracle{
		tsoMux: &tsoObject{
			physical: typeutil.ZeroTime,
			logical:  0,
		},
		metrics: newTSOMetrics("test", "dc"),
	}
	ts.setTSOPhysical(time.Now(), mustInitialized())
	physical, _ := ts.getTSO()
	re.Equal(typeutil.ZeroTime, physical)

	ts.setTSOPhysical(time.Now())
	physical, _ = ts.getTSO()
	re.NotEqual(typeutil.ZeroTime, physical)

	current := time.Now().Add(-time.Second)
	ts = &timestampOracle{
		tsoMux: &tsoObject{
			physical: current,
			logical:  0,
		},
		metrics: newTSOMetrics("test", "dc"),
	}
	ts.setTSOPhysical(time.Now(), mustOverflowed())
	physical, _ = ts.getTSO()
	re.Equal(current, physical)

	ts.setTSOPhysical(time.Now())
	physical, _ = ts.getTSO()
	re.NotEqual(current, physical)
}

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
		saveInterval:           5 * time.Second,
		updatePhysicalInterval: 50 * time.Millisecond,
		maxResetTSGap:          func() time.Duration { return time.Hour },
		metrics:                newTSOMetrics("test", "dc"),
	}
	leadership := mockLeadership()

	// update physical time interval failed due to reach the lastSavedTime, it needs to save storage first, but this behavior is not allowed.
	_, err := timestampOracle.getTS(ctx, leadership, 2, 0)
	re.Error(err)
	physical, _ := timestampOracle.getTSO()
	re.Equal(current, physical)

	// simulate the save to storage operation is done.
	timestampOracle.lastSavedTime.Store(time.Now().Add(5 * time.Second))
	_, err = timestampOracle.getTS(ctx, leadership, 2, 0)
	re.NoError(err)
	physical, _ = timestampOracle.getTSO()
	re.NotEqual(current, physical)
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
		saveInterval:           5 * time.Second,
		updatePhysicalInterval: 50 * time.Millisecond,
		maxResetTSGap:          func() time.Duration { return time.Hour },
		metrics:                newTSOMetrics("test", "dc"),
	}
	leadership := mockLeadership()
	runDuration := 10 * time.Second
	runCtx, runCancel := context.WithTimeout(ctx, runDuration-2*time.Second)
	defer runCancel()

	timestampOracle.lastSavedTime.Store(current.Add(runDuration))

	wg := &sync.WaitGroup{}
	concurrency := 20
	errCh := make(chan error, 1)
	wg.Add(concurrency)
	changes := atomic.Int32{}
	totalTso := atomic.Int32{}
	for range concurrency {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				default:
					ts, err := timestampOracle.getTS(runCtx, leadership, 1, 0)
					totalTso.Add(1)
					if err != nil {
						select {
						case errCh <- err:
							runCancel()
						default:
						}
					}
					if ts.Logical == 1 {
						changes.Add(1)
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		re.NoError(err)
	}
	re.LessOrEqual(changes.Load(), totalTso.Load()/int32(maxLogical)+1)
}
