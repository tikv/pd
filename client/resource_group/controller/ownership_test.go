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

package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

// TestControllerOwnershipExclusive verifies that acquiring a second controller
// fails while the first one owns the process-wide slot.
func TestControllerOwnershipExclusive(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()

	c1, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	defer func() {
		re.NoError(c1.Stop())
	}()

	// A second acquisition must fail deterministically, both before and
	// after the first controller is started.
	_, err = NewResourceGroupController(ctx, 2, mockProvider, nil, constants.NullKeyspaceID)
	re.ErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)
	// The rejection happens before any side effect: the provider has served
	// exactly one config load (from the first acquisition).
	mockProvider.AssertNumberOfCalls(t, "Get", 1)
	c1.Start(ctx)
	_, err = NewResourceGroupController(ctx, 2, mockProvider, nil, constants.NullKeyspaceID)
	re.ErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)
}

// TestControllerOwnershipReleaseOnStop verifies that stopping a started or an
// unstarted controller releases ownership exactly once, and that a replacement
// can be acquired after a complete shutdown.
func TestControllerOwnershipReleaseOnStop(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()

	// Stopping an unstarted controller releases ownership.
	c1, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(c1.Stop())

	// A replacement can be acquired, and stopping a started controller
	// releases ownership as well.
	c2, err := NewResourceGroupController(ctx, 2, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c2.Start(ctx)
	re.NoError(c2.Stop())
	// Stop is idempotent.
	re.NoError(c2.Stop())

	// A stale controller's Stop must not release the current owner's slot.
	c3, err := NewResourceGroupController(ctx, 3, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(c1.Stop())
	re.NoError(c2.Stop())
	_, err = NewResourceGroupController(ctx, 4, mockProvider, nil, constants.NullKeyspaceID)
	re.ErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)
	re.NoError(c3.Stop())

	// A stopped controller cannot be started again.
	c3.Start(ctx)
	re.Nil(c3.loopCtx)
}

// TestControllerOwnershipNotLeakedOnInitFailure verifies that a failed
// construction does not leak the ownership slot.
func TestControllerOwnershipNotLeakedOnInitFailure(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failingProvider := &MockResourceGroupProvider{}
	failingProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(
		(*meta_storagepb.GetResponse)(nil), errors.New("mock load config error"))
	_, err := NewResourceGroupController(ctx, 1, failingProvider, nil, constants.NullKeyspaceID)
	re.Error(err)
	re.NotErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)

	// The slot must be available again after the failure.
	mockProvider := newMockResourceGroupProvider()
	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(c.Stop())
}

// TestControllerOwnershipConcurrent verifies that concurrent acquire/stop
// attempts never let two controllers own the slot at the same time.
func TestControllerOwnershipConcurrent(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()

	var (
		wg        sync.WaitGroup
		active    atomic.Int32
		successes atomic.Int32
		overlaps  atomic.Int32
		stopErrs  atomic.Int32
	)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 8 {
				c, err := NewResourceGroupController(ctx, uint64(i), mockProvider, nil, constants.NullKeyspaceID)
				if err != nil {
					continue
				}
				successes.Add(1)
				if active.Add(1) > 1 {
					overlaps.Add(1)
				}
				// Decrement before Stop: ownership is still held here, so
				// no other goroutine can acquire until Stop releases it.
				active.Add(-1)
				if err := c.Stop(); err != nil {
					stopErrs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	re.Zero(overlaps.Load())
	re.Zero(stopErrs.Load())
	re.Positive(successes.Load())

	// The slot must end up free.
	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(c.Stop())
}

// TestControllerOwnershipHeldAfterContextCancel verifies that canceling the
// context passed to Start stops the run loop but does not release the
// process-wide ownership slot; only an explicit Stop does.
func TestControllerOwnershipHeldAfterContextCancel(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	mockProvider := newMockResourceGroupProvider()

	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c.Start(ctx)
	cancel()
	// Wait for the run loop to exit; the slot must still be held.
	c.wg.Wait()
	_, err = NewResourceGroupController(context.Background(), 2, mockProvider, nil, constants.NullKeyspaceID)
	re.ErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)

	re.NoError(c.Stop())
	replacement, err := NewResourceGroupController(context.Background(), 3, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(replacement.Stop())
}

// TestControllerConcurrentStartStop verifies that a Stop racing a Start can
// neither leak a run loop that no longer owns the slot nor leave the slot
// held after both return.
func TestControllerConcurrentStartStop(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()

	for range 50 {
		c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		var (
			wg      sync.WaitGroup
			stopErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Start(ctx)
		}()
		go func() {
			defer wg.Done()
			stopErr = c.Stop()
		}()
		wg.Wait()
		re.NoError(stopErr)
		// Whichever side won, no run loop may survive Stop: either Start
		// was refused, or Stop canceled the started loop and waited for it.
		c.wg.Wait()
		// The slot must be free for a replacement.
		replacement, err := NewResourceGroupController(ctx, 2, mockProvider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		re.NoError(replacement.Stop())
	}
}

// TestStopUnstartedControllerCleansMetrics verifies that stopping a
// controller that was used but never started still cleans the process-global
// metric state before releasing the slot.
func TestStopUnstartedControllerCleansMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()
	group := &rmpb.ResourceGroup{
		Name: "unstarted_cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, "unstarted_cleanup", mock.Anything).Return(group, nil)

	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	// Controller methods are usable before Start and populate the
	// process-global status gauge.
	_, err = c.tryGetResourceGroupController(ctx, "unstarted_cleanup", false)
	re.NoError(err)
	re.NotZero(gaugeSeriesCount(metrics.ResourceGroupStatusGauge))

	// Stop on the unstarted controller must clean the process-global state
	// before handing the slot to a replacement.
	re.NoError(c.Stop())
	re.Zero(gaugeSeriesCount(metrics.ResourceGroupStatusGauge))
}

// TestControllerDoubleStart verifies that a second Start is refused instead
// of launching a duplicate run loop whose context could never be canceled.
func TestControllerDoubleStart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()

	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c.Start(ctx)
	loopCtx := c.loopCtx
	c.Start(ctx)
	re.Same(loopCtx, c.loopCtx, "second Start must not replace the run loop context")
	re.NoError(c.Stop())
}

// TestForeignControllerStopDoesNotDisturbOwner verifies that stopping a
// controller allocated outside the supported API neither releases the
// owner's slot nor clears the owner's process-global metric state.
func TestForeignControllerStopDoesNotDisturbOwner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := newMockResourceGroupProvider()
	group := &rmpb.ResourceGroup{
		Name: "owner_group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, "owner_group", mock.Anything).Return(group, nil)

	owner, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	_, err = owner.tryGetResourceGroupController(ctx, "owner_group", false)
	re.NoError(err)
	re.NotZero(gaugeSeriesCount(metrics.ResourceGroupStatusGauge))

	foreign := &ResourceGroupsController{}
	re.NoError(foreign.Stop())
	re.NotZero(gaugeSeriesCount(metrics.ResourceGroupStatusGauge))
	_, err = NewResourceGroupController(ctx, 2, mockProvider, nil, constants.NullKeyspaceID)
	re.ErrorIs(err, errs.ErrClientResourceGroupControllerAlreadyExists)

	re.NoError(owner.Stop())
	re.Zero(gaugeSeriesCount(metrics.ResourceGroupStatusGauge))
}

// gaugeSeriesCount returns the number of series currently exported by vec.
func gaugeSeriesCount(vec *prometheus.GaugeVec) int {
	ch := make(chan prometheus.Metric)
	go func() {
		vec.Collect(ch)
		close(ch)
	}()
	count := 0
	for range ch {
		count++
	}
	return count
}
