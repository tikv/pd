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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

type mockElection struct{}

func (*mockElection) ID() uint64               { return 0 }
func (*mockElection) Name() string             { return "" }
func (*mockElection) MemberValue() string      { return "" }
func (*mockElection) Client() *clientv3.Client { return nil }
func (*mockElection) IsServing() bool          { return true }
func (*mockElection) PromoteSelf()             {}
func (*mockElection) Campaign(_ context.Context, _ int64) error {
	return nil
}
func (*mockElection) Resign()                             {}
func (*mockElection) GetServingUrls() []string            { return nil }
func (*mockElection) GetElectionPath() string             { return "" }
func (*mockElection) GetLeadership() *election.Leadership { return nil }

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
		metrics: newTSOMetrics("test"),
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
		metrics: newTSOMetrics("test"),
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
		metrics:                newTSOMetrics("test"),
		member:                 &mockElection{},
		flight:                 syncutil.NewOrderedSingleFlight[bool](),
	}

	// update physical time interval failed due to reach the lastSavedTime, it needs to save storage first, but this behavior is not allowed.
	_, err := timestampOracle.getTS(ctx, 2)
	re.Error(err)
	physical, _ := timestampOracle.getTSO()
	re.Equal(current, physical)

	// simulate the save to storage operation is done.
	timestampOracle.lastSavedTime.Store(time.Now().Add(5 * time.Second))
	_, err = timestampOracle.getTS(ctx, 2)
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
		metrics:                newTSOMetrics("test"),
		member:                 &mockElection{},
		flight:                 syncutil.NewOrderedSingleFlight[bool](),
	}
	runDuration := 10 * time.Second
	runCtx, runCancel := context.WithTimeout(ctx, runDuration-2*time.Second)
	defer runCancel()

	timestampOracle.lastSavedTime.Store(current.Add(runDuration))

	wg := &sync.WaitGroup{}
	concurrency := 200
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
					ts, err := timestampOracle.getTS(runCtx, 1)
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

func BenchmarkGetTSLogicalOverflow(b *testing.B) {
	log.ReplaceGlobals(zap.NewNop(), nil)

	for _, tc := range []struct {
		name  string
		getTS func(context.Context, *timestampOracle, uint32) (pdpb.Timestamp, error)
	}{
		{
			name: "with-ordered-singleflight",
			getTS: func(ctx context.Context, timestampOracle *timestampOracle, count uint32) (pdpb.Timestamp, error) {
				return timestampOracle.getTS(ctx, count)
			},
		},
		{
			name:  "direct-update",
			getTS: getTSWithoutSingleFlight,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			current := time.Now()
			timestampOracle := &timestampOracle{
				tsoMux: &tsoObject{
					physical: current,
					logical:  maxLogical - 1,
				},
				saveInterval:           5 * time.Second,
				updatePhysicalInterval: 50 * time.Millisecond,
				maxResetTSGap:          func() time.Duration { return time.Hour },
				metrics:                newTSOMetrics("bench"),
				member:                 &mockElection{},
				flight:                 syncutil.NewOrderedSingleFlight[bool](),
			}
			timestampOracle.lastSavedTime.Store(current.Add(365 * 24 * time.Hour))

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				timestampOracle.tsoMux.Lock()
				timestampOracle.tsoMux.logical = maxLogical - 1
				timestampOracle.tsoMux.Unlock()

				if _, err := tc.getTS(ctx, timestampOracle, 2); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLogicalOverflowUpdateParallel(b *testing.B) {
	log.ReplaceGlobals(zap.NewNop(), nil)

	for _, tc := range []struct {
		name           string
		handleOverflow func(context.Context, *timestampOracle, pdpb.Timestamp) error
	}{
		{
			name:           "with-ordered-singleflight",
			handleOverflow: handleLogicalOverflowWithSingleFlight,
		},
		{
			name:           "direct-update",
			handleOverflow: handleLogicalOverflowDirectly,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			current := time.Now()
			timestampOracle := &timestampOracle{
				tsoMux: &tsoObject{
					physical: current,
					logical:  maxLogical - 1,
				},
				saveInterval:           5 * time.Second,
				updatePhysicalInterval: 50 * time.Millisecond,
				maxResetTSGap:          func() time.Duration { return time.Hour },
				metrics:                newTSOMetrics("bench-parallel"),
				member:                 &mockElection{},
				flight:                 syncutil.NewOrderedSingleFlight[bool](),
			}
			timestampOracle.lastSavedTime.Store(current.Add(365 * 24 * time.Hour))

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					timestampOracle.tsoMux.Lock()
					timestampOracle.tsoMux.logical = maxLogical - 1
					timestampOracle.tsoMux.Unlock()

					resp := pdpb.Timestamp{Physical: current.UnixNano() / int64(time.Millisecond), Logical: maxLogical + 1}
					if err := tc.handleOverflow(ctx, timestampOracle, resp); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func handleLogicalOverflowWithSingleFlight(ctx context.Context, t *timestampOracle, resp pdpb.Timestamp) error {
	overflowResp := resp
	_, err := t.flight.Do(ctx, func(context.Context) (bool, error) {
		ret, err := t.updateTimestamp(overflowUpdate)
		log.Warn("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`",
			logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
			zap.Reflect("response", overflowResp),
			errs.ZapError(errs.ErrLogicOverflow),
			zap.Bool("overflowed", ret),
			zap.Error(err))
		return ret, err
	})
	return err
}

func handleLogicalOverflowDirectly(_ context.Context, t *timestampOracle, resp pdpb.Timestamp) error {
	overflowed, err := t.updateTimestamp(overflowUpdate)
	log.Warn("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`",
		logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
		zap.Reflect("response", resp),
		errs.ZapError(errs.ErrLogicOverflow),
		zap.Bool("overflowed", overflowed),
		zap.Error(err))
	return err
}

func getTSWithoutSingleFlight(ctx context.Context, t *timestampOracle, count uint32) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp
	if count == 0 {
		return resp, errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
	}
	for i := range maxRetryCount {
		currentPhysical, _ := t.getTSO()
		if currentPhysical.Equal(typeutil.ZeroTime) {
			if t.member.IsServing() {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			t.metrics.notLeaderAnymoreEvent.Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
		}
		resp.Physical, resp.Logical = t.generateTSO(ctx, int64(count))
		if resp.GetPhysical() == 0 {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory has been reset")
		}
		if overflowedLogical(resp.GetLogical()) {
			t.metrics.logicalOverflowEvent.Inc()
			overflowed, err := t.updateTimestamp(overflowUpdate)
			log.Warn("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`",
				logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), errs.ZapError(errs.ErrLogicOverflow),
				zap.Bool("overflowed", overflowed),
				zap.Error(err))
			if err != nil || overflowed {
				time.Sleep(t.updatePhysicalInterval)
			}
			continue
		}
		if !t.member.IsServing() {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested %s anymore", errs.NotLeaderErr))
		}
		return resp, nil
	}
	t.metrics.exceededMaxRetryEvent.Inc()
	return resp, errs.ErrGenerateTimestamp.FastGenByArgs("generate tso maximum number of retries exceeded")
}
