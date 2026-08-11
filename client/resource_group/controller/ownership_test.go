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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
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
					overlaps.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	re.Zero(overlaps.Load())
	re.Positive(successes.Load())

	// The slot must end up free.
	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.NoError(c.Stop())
}
