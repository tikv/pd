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

	"github.com/pingcap/errors"
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

// GCStatesAPIOptions represents all options for GC states API.
//
//nolint:revive
type GCStatesAPIOptions struct {
	ExcludeGCBarriers       bool
	ExcludeGlobalGCBarriers bool
}

// DefaultGCStatesAPIOptions returns the default options for GC states API.
func DefaultGCStatesAPIOptions() GCStatesAPIOptions {
	return GCStatesAPIOptions{
		ExcludeGCBarriers:       true,
		ExcludeGlobalGCBarriers: true,
	}
}

// GCStatesAPIOption is the type of option for GC states API.
//
//nolint:revive
type GCStatesAPIOption func(*GCStatesAPIOptions)

// ExcludeGCBarriers controls whether GetGCState and GetAllKeyspacesGCStates should exclude GC barriers.
// Enabling this can reduce the cost of reading GC states, and is recommended for most use cases.
// When GC barriers are needed, explicitly set false to this option.
func ExcludeGCBarriers(whetherToExclude bool) GCStatesAPIOption {
	return func(opts *GCStatesAPIOptions) {
		opts.ExcludeGCBarriers = whetherToExclude
	}
}

// ExcludeGlobalGCBarriers controls whether GetAllKeyspacesGCStates should exclude global GC barriers.
// Enabling this can reduce the cost of reading GC states, and is recommended for most use cases.
// When global GC barriers are needed, explicitly set false to this option.
func ExcludeGlobalGCBarriers(whetherToExclude bool) GCStatesAPIOption {
	return func(opts *GCStatesAPIOptions) {
		opts.ExcludeGlobalGCBarriers = whetherToExclude
	}
}

// GCStatesClient is the interface for users to access GC states.
// KeyspaceID is already bound to this type when created.
//
//nolint:revive
type GCStatesClient interface {
	// SetGCBarrier sets a GC barrier, which blocks GC from being advanced over the given barrierTS for at most a duration
	// specified by ttl. This method either adds a new GC barrier or updates an existing one. Returns the information of the
	// new GC barrier.
	//
	// A GC barrier is uniquely identified by the given barrierID in the keyspace scope for NullKeyspace or keyspaces
	// with keyspace-level GC enabled. When this method is called on keyspaces without keyspace-level GC enabled, it will
	// be equivalent to calling it on the NullKeyspace.
	//
	// Once a GC barrier is set, it will block the txn safe point from being advanced over the barrierTS, until the GC
	// barrier is expired (defined by ttl) or manually deleted (by calling DeleteGCBarrier).
	//
	// When this method is called on an existing GC barrier, it updates the barrierTS and ttl of the existing GC barrier and
	// the expiration time will become the current time plus the ttl. This means that calling this method on an existing
	// GC barrier can extend its lifetime arbitrarily.
	//
	// Passing non-positive value to ttl is not allowed. Passing `time.Duration(math.MaxInt64)` to ttl indicates that the
	// GC barrier should never expire. The ttl might be rounded up, and the actual ttl is guaranteed no less than the
	// specified duration.
	//
	// The barrierID must be non-empty. "gc_worker" is a reserved name and cannot be used as a barrierID.
	//
	// The given barrierTS must be greater than or equal to the current txn safe point, or an error will be returned.
	//
	// When this function executes successfully, its result is never nil.
	SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*GCBarrierInfo, error)
	// DeleteGCBarrier deletes a GC barrier by the given barrierID. Returns the information of the deleted GC barrier, or
	// nil if the barrier does not exist.
	//
	// When this method is called on a keyspace without keyspace-level GC enabled, it will be equivalent to calling it on
	// the NullKeyspace.
	DeleteGCBarrier(ctx context.Context, barrierID string) (*GCBarrierInfo, error)
	// GetGCState returns the GC state of the given keyspace.
	//
	// When this method is called on a keyspace without keyspace-level GC enabled, it will be equivalent to calling it on
	// the NullKeyspace.
	GetGCState(ctx context.Context, opts ...GCStatesAPIOption) (GCState, error)
	// SetGlobalGCBarrier sets a global GC barrier, which blocks GC like how GC barriers do, but is effective for all
	// keyspaces. This API is designed for some special needs to block GC of all keyspaces.
	//
	// The usage is the similar to SetGCBarrier, but is not affected by the keyspace context of the current GCStatesClient
	// instance. Note that normal GC barriers and global GC barriers are separated.
	// One can not use SetGCBarrier and DeleteGCBarrier to operate a global GC barrier set by SetGlobalGCBarrier, and vice
	// versa.
	//
	// Once a global GC barrier is set, it will block the txn safe points of all keyspaces from being advanced over the
	// barrierTS, until the global GC barrier is expired (defined by ttl) or manually deleted (by calling
	// DeleteGlobalGCBarrier).
	//
	// When this method is called on an existing global GC barrier, it updates the barrierTS and ttl of the existing global
	// GC barrier and the expiration time will become the current time plus the ttl.
	// This means that calling this method on an existing global GC barrier can extend its lifetime arbitrarily.
	//
	// Passing non-positive value to ttl is not allowed. Passing `time.Duration(math.MaxInt64)` to ttl indicates that the
	// global GC barrier should never expire. The ttl might be rounded up, and the actual ttl is guaranteed no less than the
	// specified duration.
	//
	// The barrierID must be non-empty.
	//
	// The given barrierTS must be greater than or equal to the current txn safe points of all keyspaces,
	// otherwise an error will be returned.
	//
	// Currently, the caller is responsible for guaranteeing the given barrierTS does not exceed any of the max allocated
	// timestamps of all TSOs in the cluster. Note that a cluster might have multiple TSOs for different keyspaces.
	//
	// When this function executes successfully, its result is never nil.
	SetGlobalGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*GlobalGCBarrierInfo, error)
	// DeleteGlobalGCBarrier deletes a global GC barrier.
	DeleteGlobalGCBarrier(ctx context.Context, barrierID string) (*GlobalGCBarrierInfo, error)
	// GetAllKeyspacesGCStates gets GC states from all keyspaces.
	// The return value includes both GC states and global GC barriers information.
	// If a keyspace's state is not ENABLED(like DISABLE/ARCHIVED/TOMBSTONE), that keyspace is skipped.
	// If a keyspace is not configured with keyspace level GC, its GCState data is missing.
	GetAllKeyspacesGCStates(ctx context.Context, opts ...GCStatesAPIOption) (ClusterGCStates, error)
}

// InternalController is the interface for controlling GC execution.
// KeyspaceID is already bound to this type when created.
//
// WARNING: This is only for internal use. The only possible place to use this is the `GCWorker` in TiDB, or
// other possible components that are responsible for being the center of controlling GC of the cluster.
type InternalController interface {
	// AdvanceTxnSafePoint tries to advance the txn safe point to the given target.
	//
	// Returns a struct AdvanceTxnSafePointResult, which contains the old txn safe point, the target, and the new
	// txn safe point it finally made it to advance to. If there's something blocking the txn safe point from being
	// advanced to the given target, it may finally be advanced to a smaller value or remains the previous value, in which
	// case the BlockerDescription field of the AdvanceTxnSafePointResult will be set to a non-empty string describing
	// the reason.
	//
	// Txn safe point of a single keyspace should never decrease. If the given target is smaller than the previous value,
	// it returns an error.
	//
	// WARNING: This method is only used to manage the GC procedure, and should never be called by code that doesn't
	// have the responsibility to manage GC. It can only be called on NullKeyspace or keyspaces with keyspace level GC
	// enabled.
	AdvanceTxnSafePoint(ctx context.Context, target uint64) (AdvanceTxnSafePointResult, error)
	// AdvanceGCSafePoint tries to advance the GC safe point to the given target. If the target is less than the current
	// value or greater than the txn safe point, it returns an error.
	//
	// WARNING: This method is only used to manage the GC procedure, and should never be called by code that doesn't
	// have the responsibility to manage GC. It can only be called on NullKeyspace or keyspaces with keyspace level GC
	// enabled.
	AdvanceGCSafePoint(ctx context.Context, target uint64) (AdvanceGCSafePointResult, error)
}

// AdvanceTxnSafePointResult represents the result of advancing transaction safe point.
type AdvanceTxnSafePointResult struct {
	// The old txn safe point before the advancement operation.
	OldTxnSafePoint uint64
	// The target to which the current advancement operation tried to advance the txn safe point. It contains the
	Target uint64
	// same value as the `target` argument passed to the AdvanceTxnSafePoint method.
	NewTxnSafePoint uint64
	// When the txn safe point is blocked and is unable to be advanced to exactly the target, this field will contains
	// a non-empty string describing the reason why it is blocked.
	BlockerDescription string
}

// AdvanceGCSafePointResult represents the result of advancing GC safe point.
type AdvanceGCSafePointResult struct {
	// The old GC safe point before the advancement operation.
	OldGCSafePoint uint64
	// The target to which the current advancement operation tried to advance the GC safe point. It contains the
	// same value as the `target` argument passed to the AdvanceGCSafePoint method.
	Target uint64
	// The new GC safe point after the advancement operation.
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

// GlobalGCBarrierInfo represents the information of a global GC barrier.
type GlobalGCBarrierInfo struct {
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

// IsExpired checks whether the barrier is expired by the local time. The check is done by checking the local time.
// Note that the result is unreliable in case there is significant time drift between the client and the PD server.
// As the TTL is round down when returning from the server, this method may give an expired result slightly earlier
// than it actually expires in PD server.
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

// NewGlobalGCBarrierInfo creates a new GCBarrierInfo instance.
func NewGlobalGCBarrierInfo(barrierID string, barrierTS uint64, ttl time.Duration, getReqStartTime time.Time) *GlobalGCBarrierInfo {
	return &GlobalGCBarrierInfo{
		BarrierID:       barrierID,
		BarrierTS:       barrierTS,
		TTL:             ttl,
		getReqStartTime: getReqStartTime,
	}
}

// IsExpired checks whether the barrier is expired.
func (b *GlobalGCBarrierInfo) IsExpired() bool {
	return b.isExpiredImpl(time.Now())
}

func (b *GlobalGCBarrierInfo) isExpiredImpl(now time.Time) bool {
	if b.TTL == TTLNeverExpire {
		return false
	}
	return now.Sub(b.getReqStartTime) > b.TTL
}

// GCState represents the information of the GC state.
//
//nolint:revive
type GCState struct {
	// The ID of the keyspace this GC state belongs to.
	KeyspaceID    uint32
	TxnSafePoint  uint64
	GCSafePoint   uint64
	hasGCBarriers bool
	gcBarriers    []*GCBarrierInfo
}

// NewGCStateWithoutGCBarriers creates a GCState instance without GC barriers info.
func NewGCStateWithoutGCBarriers(keyspaceID uint32, txnSafePoint uint64, gcSafePoint uint64) GCState {
	return GCState{
		KeyspaceID:    keyspaceID,
		TxnSafePoint:  txnSafePoint,
		GCSafePoint:   gcSafePoint,
		hasGCBarriers: false,
		gcBarriers:    nil,
	}
}

// NewGCStateWithGCBarriers creates a GCState instance with GC barriers info.
func NewGCStateWithGCBarriers(keyspaceID uint32, txnSafePoint uint64, gcSafePoint uint64, gcBarriers []*GCBarrierInfo) GCState {
	return GCState{
		KeyspaceID:    keyspaceID,
		TxnSafePoint:  txnSafePoint,
		GCSafePoint:   gcSafePoint,
		hasGCBarriers: true,
		gcBarriers:    gcBarriers,
	}
}

// HasGCBarriers returns whether the GCState instance carries GC barriers info. Note that valid GC barriers info
// can be empty.
func (s GCState) HasGCBarriers() bool {
	return s.hasGCBarriers
}

// GetGCBarriers retrieves GC barriers from the GCState instance, or returns an error if it doesn't carry any GC barrier
// info.
func (s GCState) GetGCBarriers() ([]*GCBarrierInfo, error) {
	if !s.HasGCBarriers() {
		return nil, errors.New("trying to get GC barriers from GCState that doesn't provide GC barriers info. " +
			"to retrieve GC barriers, pass false to excludeGCBarriers parameter to GC APIs")
	}
	return s.gcBarriers, nil
}

// ClusterGCStates represents the information of the GC state for all keyspaces.
type ClusterGCStates struct {
	// Maps from keyspace id to GC state of that keyspace.
	GCStates            map[uint32]GCState
	hasGlobalGCBarriers bool
	globalGCBarriers    []*GlobalGCBarrierInfo
}

// NewClusterGCStatesWithoutGlobalGCBarriers creates a ClusterGCStates instance without global GC barriers info.
func NewClusterGCStatesWithoutGlobalGCBarriers(gcStates map[uint32]GCState) ClusterGCStates {
	return ClusterGCStates{
		GCStates:            gcStates,
		hasGlobalGCBarriers: false,
		globalGCBarriers:    nil,
	}
}

// NewClusterGCStatesWithGlobalGCBarriers creates a ClusterGCStates instance with global GC barriers info.
func NewClusterGCStatesWithGlobalGCBarriers(gcStates map[uint32]GCState, globalGCBarriers []*GlobalGCBarrierInfo) ClusterGCStates {
	return ClusterGCStates{
		GCStates:            gcStates,
		hasGlobalGCBarriers: true,
		globalGCBarriers:    globalGCBarriers,
	}
}

// HasGlobalGCBarriers returns whether the ClusterGCStates instance carries global GC barriers info. Note that valid
// global GC barriers info can be empty.
func (s ClusterGCStates) HasGlobalGCBarriers() bool {
	return s.hasGlobalGCBarriers
}

// GetGlobalGCBarriers retrieves global GC barriers from the ClusterGCStates instance, or returns an error if it doesn't
// carry any global GC barrier info.
func (s ClusterGCStates) GetGlobalGCBarriers() ([]*GlobalGCBarrierInfo, error) {
	if !s.HasGlobalGCBarriers() {
		return nil, errors.New("trying to get global GC barriers from ClusterGCStates that doesn't provide global GC barriers info. " +
			"to retrieve global GC barriers, pass false to excludeGlobalGCBarriers parameter to GC APIs")
	}
	return s.globalGCBarriers, nil
}

// LegacyClientV2 is the GC client interface for legacy PD servers using (old) GC API V2.
// Used to migrate legacy PD servers which do not support the "new GC API" for multi-tenant usage (e.g. TiCDC nextgen).
// This interface is intentionally not added to RPCClient to avoid misuse or breaking existing stub and mock implementations.
// Will be removed after the migration is done.
type LegacyClientV2 interface {
	// GetMinServiceSafePointV2 returns the current minimum service GC safe point for the given keyspace.
	GetMinServiceSafePointV2(ctx context.Context, keyspaceID uint32) (uint64, error)
	// SetServiceSafePointV2 updates a service GC safe point for the given keyspace and returns the new minimum safe point.
	SetServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	// DeleteServiceSafePointV2 deletes a service GC safe point for the given keyspace and returns the new minimum safe point.
	DeleteServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string) (uint64, error)
}
