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
// KeyspaceID is bound to this type when created.
//
//nolint:revive
type GCStatesClient interface {
	SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*GCBarrierInfo, error)
	DeleteGCBarrier(ctx context.Context, barrierID string) (*GCBarrierInfo, error)
	GetGCState(ctx context.Context) (GCStateInfo, error)
}

// InternalController is the interface for controlling GC execution.
// KeyspaceID is bound to this type when created.
//
// WARNING: This is only for internal use. The only possible place to use this is the `GCWorker` in TiDB, or
// other possible components that are responsible for being the center of controlling GC of the cluster.
type InternalController interface {
	AdvanceTxnSafePoint(ctx context.Context, target uint64) (AdvanceTxnSafePointResult, error)
	AdvanceGCSafePoint(ctx context.Context, target uint64) (AdvanceGCSafePointResult, error)
}

type AdvanceTxnSafePointResult struct {
	OldTxnSafePoint    uint64
	Target             uint64
	NewTxnSafePoint    uint64
	BlockerDescription string
}

type AdvanceGCSafePointResult struct {
	OldGCSafePoint uint64
	Target         uint64
	NewGCSafePoint uint64
}

//nolint:revive
type GCBarrierInfo struct {
	BarrierID string
	BarrierTS uint64
	TTL       time.Duration
	// The time when the RPC that fetches the GC barrier info.
	// It will be used as the basis for determining whether the barrier is expired.
	getReqStartTime time.Time
}

const TTLNeverExpire = time.Duration(math.MaxInt64)

func NewGCBarrierInfo(barrierID string, barrierTS uint64, ttl time.Duration, getReqStartTime time.Time) *GCBarrierInfo {
	return &GCBarrierInfo{
		BarrierID:       barrierID,
		BarrierTS:       barrierTS,
		TTL:             ttl,
		getReqStartTime: getReqStartTime,
	}
}

func (b *GCBarrierInfo) IsExpired() bool {
	return b.isExpired(time.Now())
}

func (b *GCBarrierInfo) isExpired(now time.Time) bool {
	if b.TTL == TTLNeverExpire {
		return false
	}
	return now.Sub(b.getReqStartTime) > b.TTL
}

//nolint:revive
type GCStateInfo struct {
	KeyspaceID   uint32
	TxnSafePoint uint64
	GCSafePoint  uint64
	GCBarriers   []*GCBarrierInfo
}
