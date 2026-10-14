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

package affinity

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
)

func TestRebalanceAffinityGroupRequiresRebalancer(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	groupID := seedBalanceGroups(t, manager, []uint64{1})[0]

	plans, err := manager.RebalanceAffinityGroup(t.Context(), groupID, false)

	require.ErrorIs(t, err, ErrBalanceNotReady)
	require.Nil(t, plans)
	require.False(t, manager.IsManualBalanceRunning(groupID))
}

func TestRebalanceAffinityGroupRejectsConcurrentRequestForTable(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
	entered := make(chan struct{})
	release := make(chan struct{})
	manager.SetBalanceRebalancer(func(ctx context.Context, id string, _ bool) ([]BalancePlan, error) {
		close(entered)
		select {
		case <-release:
			return []BalancePlan{{GroupID: id}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	type result struct {
		plans []BalancePlan
		err   error
	}
	first := make(chan result, 1)
	go func() {
		plans, err := manager.RebalanceAffinityGroup(t.Context(), groupID, true)
		first <- result{plans: plans, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first rebalance did not start")
	}
	require.True(t, manager.IsManualBalanceRunning(groupID))
	plans, err := manager.RebalanceAffinityGroup(t.Context(), groupID, false)
	require.ErrorIs(t, err, ErrBalanceBusy)
	require.Nil(t, plans)

	close(release)
	select {
	case got := <-first:
		require.NoError(t, got.err)
		require.Len(t, got.plans, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("first rebalance did not finish")
	}
	require.False(t, manager.IsManualBalanceRunning(groupID))
}

func TestRebalanceAffinityGroupReleasesTableAfterPreviewOrError(t *testing.T) {
	failure := errors.New("manual rebalance failed")
	for _, tc := range []struct {
		name      string
		apply     bool
		callback  BalanceRebalancer
		wantErr   error
		wantPlans int
	}{
		{
			name:  "preview",
			apply: false,
			callback: func(_ context.Context, id string, _ bool) ([]BalancePlan, error) {
				return []BalancePlan{{GroupID: id}}, nil
			},
			wantPlans: 1,
		},
		{
			name:  "error",
			apply: true,
			callback: func(context.Context, string, bool) ([]BalancePlan, error) {
				return nil, failure
			},
			wantErr: failure,
		},
		{
			name:  "wrapped conflict retains partial result",
			apply: true,
			callback: func(_ context.Context, id string, _ bool) ([]BalancePlan, error) {
				return []BalancePlan{{GroupID: id, Status: "submitted"}}, fmt.Errorf("already committed 1 group: %w", errs.ErrEtcdTxnConflict.FastGenByArgs())
			},
			wantErr:   ErrBalanceConflict,
			wantPlans: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			manager.SetBalanceRebalancer(tc.callback)

			plans, err := manager.RebalanceAffinityGroup(t.Context(), groupID, tc.apply)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, plans, tc.wantPlans)
			require.False(t, manager.IsManualBalanceRunning(groupID))
		})
	}
}

func TestRebalanceAffinityGroupCancellationReleasesTable(t *testing.T) {
	for _, lifecycle := range []bool{false, true} {
		name := "caller-context"
		if lifecycle {
			name = "manager-lifecycle"
		}
		t.Run(name, func(t *testing.T) {
			managerCtx, cancelManager := context.WithCancel(t.Context())
			defer cancelManager()
			manager := newBalanceRuntimeManagerWithContext(t, managerCtx)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			requestCtx := t.Context()
			var cancel context.CancelFunc
			if lifecycle {
				cancel = cancelManager
			} else {
				var cancelRequest context.CancelFunc
				requestCtx, cancelRequest = context.WithCancel(t.Context())
				cancel = cancelRequest
			}
			defer cancel()
			entered := make(chan struct{})
			manager.SetBalanceRebalancer(func(ctx context.Context, _ string, _ bool) ([]BalancePlan, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			type result struct{ err error }
			done := make(chan result, 1)
			go func() {
				_, err := manager.RebalanceAffinityGroup(requestCtx, groupID, true)
				done <- result{err: err}
			}()

			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("rebalance callback did not start")
			}
			require.True(t, manager.IsManualBalanceRunning(groupID))
			cancel()
			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("rebalance did not stop after cancellation")
			}
			require.False(t, manager.IsManualBalanceRunning(groupID))
		})
	}
}
