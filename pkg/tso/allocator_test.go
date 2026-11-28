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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tsopb"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestAllocatorCampaignRequiresGroupMembership(t *testing.T) {
	var txnConflicts atomic.Int32
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
		ClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(
				func(
					ctx context.Context, method string, req, reply any,
					cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
				) error {
					err := invoker(ctx, method, req, reply, cc, opts...)
					if response, ok := reply.(*etcdserverpb.TxnResponse); err == nil && ok && !response.Succeeded {
						txnConflicts.Add(1)
					}
					return err
				},
			))
		},
	})
	t.Cleanup(clean)
	store := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)

	tests := []struct {
		name                 string
		groupID              uint32
		deleteBeforeCampaign bool
		deleteWhileServing   bool
		deleteWindow         bool
		transfer             bool
	}{
		{name: "existing group", groupID: 1},
		{name: "existing group with transfer", groupID: 2, transfer: true},
		{name: "deleted group", groupID: 3, deleteBeforeCampaign: true},
		{name: "deleted group with transfer", groupID: 4, deleteBeforeCampaign: true, transfer: true},
		{name: "group deleted while serving", groupID: 5, deleteWhileServing: true},
		{name: "group and window deleted while serving", groupID: 6, deleteWhileServing: true, deleteWindow: true},
		{name: "default group before bootstrap", groupID: constant.DefaultKeyspaceGroupID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			if tt.groupID != constant.DefaultKeyspaceGroupID {
				re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
					return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{ID: tt.groupID})
				}))
			}

			const addr = "http://127.0.0.1:1234"
			msParam := keypath.MsParam{ServiceName: mcs.TSOServiceName, GroupID: tt.groupID}
			participant := member.NewParticipant(client, msParam)
			participant.InitInfo(&tsopb.Participant{
				Name:       "tso",
				Id:         1,
				ListenUrls: []string{addr},
			}, keyspaceGroupPrimaryElectionPurpose(tt.groupID))
			allocator := NewAllocator(ctx, tt.groupID, participant, store, &TestServiceConfig{
				LeaderLease:               mcs.DefaultLease,
				TSOSaveInterval:           time.Minute,
				TSOUpdatePhysicalInterval: 50 * time.Millisecond,
			})
			t.Cleanup(allocator.Close)
			previousWindow := time.Now().Add(-time.Hour).UnixNano()
			re.NoError(store.Save(keypath.TimestampPath(tt.groupID), string(typeutil.Uint64ToBytes(uint64(previousWindow)))))

			var expectedPrimary string
			if tt.transfer {
				expectedPrimary = addr
				_, err := client.Put(ctx, keypath.ExpectedPrimaryPath(&msParam), expectedPrimary)
				re.NoError(err)
			}
			if tt.deleteBeforeCampaign {
				// The watcher has not removed this allocator when the membership is deleted.
				re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
					return store.DeleteKeyspaceGroup(txn, tt.groupID)
				}))
				primary, checkAgain := participant.CheckPrimary()
				re.Nil(primary)
				re.False(checkAgain)
			}

			conflictsBeforeCampaign := txnConflicts.Load()
			campaignDone := make(chan struct{})
			allocator.wg.Go(func() {
				defer close(campaignDone)
				allocator.campaignPrimary(expectedPrimary)
			})
			testutil.Eventually(re, func() bool {
				return isSignaled(campaignDone) || participant.IsServing()
			})

			primary, err := client.Get(ctx, participant.GetElectionPath())
			re.NoError(err)
			window, err := store.LoadTimestamp(tt.groupID)
			re.NoError(err)
			if tt.deleteBeforeCampaign {
				re.Empty(primary.Kvs)
				re.Equal(previousWindow, window.UnixNano())
				re.True(isSignaled(campaignDone))
				re.Equal(conflictsBeforeCampaign+1, txnConflicts.Load(), "etcd must reject the campaign transaction")
			} else {
				re.Len(primary.Kvs, 1)
				re.Positive(primary.Kvs[0].Lease)
				re.True(participant.IsServing())
				re.Greater(window.UnixNano(), previousWindow)
			}

			if tt.deleteWhileServing {
				re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
					if err := store.DeleteKeyspaceGroup(txn, tt.groupID); err != nil {
						return err
					}
					if tt.deleteWindow {
						return txn.Remove(keypath.TimestampPath(tt.groupID))
					}
					return nil
				}))
				re.True(participant.IsServing(), "deleting the group leaves the primary lease intact")
				conflictsBeforeUpdate := txnConflicts.Load()
				// Move the updater's clock past the current window to force a save.
				const fp = "github.com/tikv/pd/pkg/tso/fallBackUpdate"
				re.NoError(failpoint.Enable(fp, "return(true)"))
				t.Cleanup(func() { re.NoError(failpoint.Disable(fp)) })
				testutil.Eventually(re, func() bool { return isSignaled(campaignDone) })
				re.Greater(txnConflicts.Load(), conflictsBeforeUpdate, "etcd must reject the timestamp update")
				re.False(participant.IsServing())
				re.False(allocator.IsInitialize())
				wantWindow := window
				if tt.deleteWindow {
					wantWindow = typeutil.ZeroTime
				}
				window, err = store.LoadTimestamp(tt.groupID)
				re.NoError(err)
				re.Equal(wantWindow, window)
			}

			if tt.deleteBeforeCampaign && !tt.transfer {
				allocator.startPrimaryElectionLoop()
				conflictsBeforeLoop := conflictsBeforeCampaign + 1
				testutil.Eventually(re, func() bool {
					return txnConflicts.Load() > conflictsBeforeLoop
				}, testutil.WithTickInterval(time.Millisecond))
				time.Sleep(250 * time.Millisecond)
				re.LessOrEqual(txnConflicts.Load()-conflictsBeforeLoop, int32(3), "campaign retries must be bounded")
			}
		})
	}
}

func TestWaitPrimaryCampaignRetryCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		allocator := &Allocator{ctx: ctx}
		retryDone := make(chan struct{})
		go func() {
			allocator.waitPrimaryCampaignRetry()
			close(retryDone)
		}()
		synctest.Wait()
		require.False(t, isSignaled(retryDone))

		start := time.Now()
		cancel()
		<-retryDone
		require.Zero(t, time.Since(start), "cancellation must end the retry wait without waiting for the timer")
	})
}

type staleUpdateElection struct {
	mockElection
	serving  atomic.Bool
	resigned chan struct{}
}

func (m *staleUpdateElection) IsServing() bool { return m.serving.Load() }
func (m *staleUpdateElection) PromoteSelf()    { m.serving.Store(true) }
func (m *staleUpdateElection) Resign() {
	m.serving.Store(false)
	close(m.resigned)
}

type staleUpdateStorage struct {
	failing atomic.Bool
	calls   atomic.Int32

	mu                  sync.Mutex
	finalFailureReached chan struct{}
	releaseFinalFailure <-chan struct{}
	nextSuccessfulSave  chan struct{}
}

func (*staleUpdateStorage) LoadTimestamp(uint32) (time.Time, error) {
	return typeutil.ZeroTime, nil
}

func (s *staleUpdateStorage) SaveTimestamp(context.Context, uint32, time.Time, *election.Leadership) error {
	if !s.failing.Load() {
		s.mu.Lock()
		nextSuccessfulSave := s.nextSuccessfulSave
		s.nextSuccessfulSave = nil
		s.mu.Unlock()
		if nextSuccessfulSave != nil {
			close(nextSuccessfulSave)
		}
		return nil
	}
	if s.calls.Add(1) == maxUpdateTSORetryCount {
		s.mu.Lock()
		finalFailureReached := s.finalFailureReached
		releaseFinalFailure := s.releaseFinalFailure
		s.mu.Unlock()
		close(finalFailureReached)
		if releaseFinalFailure != nil {
			<-releaseFinalFailure
		}
	}
	return errors.New("tso update failed")
}

func (*staleUpdateStorage) DeleteTimestamp(context.Context, uint32) error { return nil }

func (s *staleUpdateStorage) startFailures(releaseFinalFailure <-chan struct{}) <-chan struct{} {
	s.mu.Lock()
	s.finalFailureReached = make(chan struct{})
	s.releaseFinalFailure = releaseFinalFailure
	finalFailureReached := s.finalFailureReached
	s.mu.Unlock()
	s.calls.Store(0)
	s.failing.Store(true)
	return finalFailureReached
}

func (s *staleUpdateStorage) stopFailures() {
	s.failing.Store(false)
}

func (s *staleUpdateStorage) notifyOnNextSuccessfulSave() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSuccessfulSave = make(chan struct{})
	return s.nextSuccessfulSave
}

func isSignaled(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

// TestAllocatorIgnoresStaleUpdateFailure is a regression test for #11086.
func TestAllocatorIgnoresStaleUpdateFailure(t *testing.T) {
	re := require.New(t)

	member := &staleUpdateElection{
		resigned: make(chan struct{}),
	}
	member.serving.Store(true)
	storage := &staleUpdateStorage{}
	allocator := NewAllocator(context.Background(), 11086, member, storage, &TestServiceConfig{
		TSOUpdatePhysicalInterval: time.Millisecond,
		TSOSaveInterval:           2 * time.Millisecond,
		MaxResetTSGap:             time.Hour,
	})
	defer allocator.Close()

	re.NoError(allocator.Initialize())
	releaseStaleFailure := make(chan struct{}, 1)
	defer close(releaseStaleFailure)
	staleFailure := storage.startFailures(releaseStaleFailure)
	testutil.Eventually(re, func() bool { return isSignaled(staleFailure) })

	// Reinitialize and promote before the old UpdateTSO returns its final error.
	member.serving.Store(false)
	storage.stopFailures()
	re.NoError(allocator.Initialize())
	nextSuccessfulSave := storage.notifyOnNextSuccessfulSave()
	member.PromoteSelf()
	releaseStaleFailure <- struct{}{}
	// The updater must handle the stale error before it can perform this save.
	testutil.Eventually(re, func() bool {
		return isSignaled(nextSuccessfulSave) || isSignaled(member.resigned)
	})
	re.False(isSignaled(member.resigned), "a stale update failure resigned the reinitialized allocator")
	re.True(allocator.IsInitialize(), "a stale update failure reset the reinitialized timestamp")

	_, err := allocator.GenerateTSO(context.Background(), 1)
	re.NoError(err)

	currentFailure := storage.startFailures(nil)
	testutil.Eventually(re, func() bool { return isSignaled(currentFailure) })
	testutil.Eventually(re, func() bool { return isSignaled(member.resigned) })
	re.False(allocator.IsInitialize())
}

// TestClosingAllocatorReturnsNotLeader checks that a closing allocator, which resets its timestamp
// before it resigns, fails the requests with the leader-change error, whether it starts to close
// before or while a request generates the TSO, and refuses to be initialized again.
func TestClosingAllocatorReturnsNotLeader(t *testing.T) {
	re := require.New(t)
	newAllocator := func() (*Allocator, *staleUpdateElection) {
		member := &staleUpdateElection{resigned: make(chan struct{})}
		member.serving.Store(true)
		allocator := NewAllocator(context.Background(), 1, member, &staleUpdateStorage{}, &TestServiceConfig{
			TSOUpdatePhysicalInterval: time.Millisecond,
			TSOSaveInterval:           2 * time.Millisecond,
			MaxResetTSGap:             time.Hour,
		})
		re.NoError(allocator.Initialize())
		_, err := allocator.GenerateTSO(context.Background(), 1)
		re.NoError(err)
		return allocator, member
	}

	// Closed before the request: the timestamp is reset while the member is still the primary.
	allocator, _ := newAllocator()
	allocator.Close()
	re.False(allocator.IsInitialize())
	start := time.Now()
	_, err := allocator.GenerateTSO(context.Background(), 1)
	re.ErrorContains(err, "not leader")
	// It fails at once instead of waiting for the timestamp to be initialized again.
	re.Less(time.Since(start), 200*time.Millisecond)
	// The first request of a keyspace group initializes the allocator, which a closed
	// allocator must refuse even though the member is still the primary.
	re.ErrorContains(allocator.Initialize(), "not leader")
	re.False(allocator.IsInitialize())
	re.Zero(promtestutil.ToFloat64(allocator.tsoAllocatorRoleGauge))

	// Closed and resigned after the serving check of the request.
	allocator, member := newAllocator()
	defer allocator.Close()
	fp := "github.com/tikv/pd/pkg/tso/afterGenerateTSOServingCheck"
	re.NoError(failpoint.EnableCall(fp, func() {
		allocator.Close()
		member.Resign()
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()
	_, err = allocator.GenerateTSO(context.Background(), 1)
	re.ErrorContains(err, "not leader")
}
