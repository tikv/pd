// Copyright 2025 TiKV Project Authors.
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

package gc

import (
	"context"
	"math"
	"time"
)

// Client is the interface for GC client.
type Client interface {
	// GetGCInternalController returns the interface for controlling GC execution.
	//
	// WARNING: This is only for internal use. The only possible place to use this is the `GCWorker` in TiDB, or
	// other possible components that are responsible for being the center of controlling GC of the cluster.
	// In most cases, you don't need this and all you need is the `GetGCStatesClient`.
	GetGCInternalController(keyspaceID uint32) InternalController
	// GetGCStatesClient returns the interface for users to access GC states.
	GetGCStatesClient(keyspaceID uint32) GCStatesClient
}

// GCStatesClient is the interface for users to access GC states.
// KeyspaceID is already bound to this type when created.
//
//nolint:revive
type GCStatesClient interface {
	// SetGCBarrier sets (creates or updates) a GC barrier.
	SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*GCBarrierInfo, error)
	// DeleteGCBarrier deletes a GC barrier.
	DeleteGCBarrier(ctx context.Context, barrierID string) (*GCBarrierInfo, error)
	// GetGCState gets the current GC state.
	GetGCState(ctx context.Context) (GCStateInfo, error)
}

// InternalController is the interface for controlling GC execution.
// KeyspaceID is already bound to this type when created.
//
// WARNING: This is only for internal use. The only possible place to use this is the `GCWorker` in TiDB, or
// other possible components that are responsible for being the center of controlling GC of the cluster.
type InternalController interface {
	// AdvanceTxnSafePoint tries to advance the transaction safe point to the target value.
	AdvanceTxnSafePoint(ctx context.Context, target uint64) (AdvanceTxnSafePointResult, error)
	// AdvanceGCSafePoint tries to advance the GC safe point to the target value.
	AdvanceGCSafePoint(ctx context.Context, target uint64) (AdvanceGCSafePointResult, error)
}

// AdvanceTxnSafePointResult represents the result of advancing transaction safe point.
type AdvanceTxnSafePointResult struct {
	OldTxnSafePoint    uint64
	Target             uint64
	NewTxnSafePoint    uint64
	BlockerDescription string
}

// AdvanceGCSafePointResult represents the result of advancing GC safe point.
type AdvanceGCSafePointResult struct {
	OldGCSafePoint uint64
	Target         uint64
	NewGCSafePoint uint64
}

// GCBarrierInfo represents the information of a GC barrier.
//
//nolint:revive
type GCBarrierInfo struct {
	BarrierID string
	BarrierTS uint64
	TTL       time.Duration
	// The time when the RPC that fetches the GC barrier info.
	// It will be used as the basis for determining whether the barrier is expired.
	getReqStartTime time.Time
}

// TTLNeverExpire is a special value for TTL that indicates the barrier never expires.
const TTLNeverExpire = time.Duration(math.MaxInt64)

// NewGCBarrierInfo creates a new GCBarrierInfo instance.
func NewGCBarrierInfo(barrierID string, barrierTS uint64, ttl time.Duration, getReqStartTime time.Time) *GCBarrierInfo {
	return &GCBarrierInfo{
		BarrierID:       barrierID,
		BarrierTS:       barrierTS,
		TTL:             ttl,
		getReqStartTime: getReqStartTime,
	}
}

// IsExpired checks whether the barrier is expired.
func (b *GCBarrierInfo) IsExpired() bool {
	return b.isExpiredImpl(time.Now())
}

// isExpiredImpl is the internal implementation of IsExpired that accepts caller-specified current time for the
// convenience of testing.
func (b *GCBarrierInfo) isExpiredImpl(now time.Time) bool {
	if b.TTL == TTLNeverExpire {
		return false
	}
	return now.Sub(b.getReqStartTime) > b.TTL
}

// GCStateInfo represents the information of the GC state.
//
//nolint:revive
type GCStateInfo struct {
	KeyspaceID   uint32
	TxnSafePoint uint64
	GCSafePoint  uint64
	GCBarriers   []*GCBarrierInfo
}
