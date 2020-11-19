// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/member"
	"go.uber.org/zap"
)

// LocalTSOAllocator is the DC-level local TSO allocator,
// which is only used to allocate TSO in one DC each.
// One PD server may hold multiple Local TSO Allocators.
type LocalTSOAllocator struct {
	// leadership is used to campaign the corresponding DC's Local TSO Allocator.
	leadership      *election.Leadership
	timestampOracle *timestampOracle
	// for election use, notice that the leadership that member holds is
	// the leadership for PD leader. Local TSO Allocator's leadership is for the
	// election of Local TSO Allocator leader among several PD servers and
	// Local TSO Allocator only use member's some etcd and pbpd.Member info.
	// So it's not conflicted.
	member          *member.Member
	rootPath        string
	dcLocation      string
	allocatorLeader atomic.Value // stored as *pdpb.Member
}

// NewLocalTSOAllocator creates a new local TSO allocator.
func NewLocalTSOAllocator(
	member *member.Member,
	leadership *election.Leadership,
	dcLocation string,
	saveInterval time.Duration,
	updatePhysicalInterval time.Duration,
	maxResetTSGap func() time.Duration,
) Allocator {
	return &LocalTSOAllocator{
		leadership: leadership,
		timestampOracle: &timestampOracle{
			client:                 leadership.GetClient(),
			rootPath:               leadership.GetLeaderKey(),
			saveInterval:           saveInterval,
			updatePhysicalInterval: updatePhysicalInterval,
			maxResetTSGap:          maxResetTSGap,
		},
		member:     member,
		rootPath:   leadership.GetLeaderKey(),
		dcLocation: dcLocation,
	}
}

// GetDCLocation returns the local allocator's dc-location.
func (lta *LocalTSOAllocator) GetDCLocation() string {
	return lta.dcLocation
}

// Initialize will initialize the created local TSO allocator.
func (lta *LocalTSOAllocator) Initialize() error {
	return lta.timestampOracle.SyncTimestamp(lta.leadership)
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (lta *LocalTSOAllocator) IsInitialize() bool {
	return lta.timestampOracle.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd
// for all local TSO allocators this PD server hold.
func (lta *LocalTSOAllocator) UpdateTSO() error {
	return lta.timestampOracle.UpdateTimestamp(lta.leadership)
}

// SetTSO sets the physical part with given TSO.
func (lta *LocalTSOAllocator) SetTSO(tso uint64) error {
	return lta.timestampOracle.resetUserTimestamp(lta.leadership, tso, false)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (lta *LocalTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	// Todo: use the low bits of TSO's logical part to distinguish the different local TSO
	return lta.timestampOracle.getTS(lta.leadership, count)
}

// Reset is used to reset the TSO allocator.
func (lta *LocalTSOAllocator) Reset() {
	lta.timestampOracle.ResetTimestamp()
}

// setAllocatorLeader sets the current Local TSO Allocator leader.
func (lta *LocalTSOAllocator) setAllocatorLeader(member *pdpb.Member) {
	lta.allocatorLeader.Store(member)
}

// unsetAllocatorLeader unsets the current Local TSO Allocator leader.
func (lta *LocalTSOAllocator) unsetAllocatorLeader() {
	lta.allocatorLeader.Store(&pdpb.Member{})
}

// GetAllocatorLeader returns the Local TSO Allocator leader.
func (lta *LocalTSOAllocator) GetAllocatorLeader() *pdpb.Member {
	allocatorLeader := lta.allocatorLeader.Load()
	if allocatorLeader == nil {
		return nil
	}
	return allocatorLeader.(*pdpb.Member)
}

// GetMember returns the Local TSO Allocator's member value.
func (lta *LocalTSOAllocator) GetMember() *pdpb.Member {
	return lta.member.Member()
}

// GetCurrentTSO returns current TSO in memory.
func (lta *LocalTSOAllocator) GetCurrentTSO() (pdpb.Timestamp, error) {
	currentPhysical, currentLogical := lta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return *tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// WriteTSO is used to set the maxTS as current TSO in memory.
func (lta *LocalTSOAllocator) WriteTSO(maxTS *pdpb.Timestamp) error {
	currentTSO, err := lta.GetCurrentTSO()
	if err != nil {
		return err
	}
	// If current local TSO has already been greater or equal to maxTS, then do not update it.
	if tsoutil.CompareTimestamp(&currentTSO, maxTS) >= 0 {
		return nil
	}
	return lta.timestampOracle.resetUserTimestamp(lta.leadership, tsoutil.GenerateTS(maxTS), true)
}

// EnableAllocatorLeader sets the Local TSO Allocator itself to a leader.
func (lta *LocalTSOAllocator) EnableAllocatorLeader() {
	lta.setAllocatorLeader(lta.member.Member())
}

// CampaignAllocatorLeader is used to campaign a Local TSO Allocator's leadership.
func (lta *LocalTSOAllocator) CampaignAllocatorLeader(leaseTimeout int64) error {
	return lta.leadership.Campaign(leaseTimeout, lta.member.MemberValue())
}

// KeepAllocatorLeader is used to keep the PD leader's leadership.
func (lta *LocalTSOAllocator) KeepAllocatorLeader(ctx context.Context) {
	lta.leadership.Keep(ctx)
}

// IsStillAllocatorLeader returns whether the allocator is still a
// Local TSO Allocator leader by checking its leadership's lease.
func (lta *LocalTSOAllocator) IsStillAllocatorLeader() bool {
	return lta.leadership.Check()
}

// isSameLeader checks whether a server is the leader itself.
func (lta *LocalTSOAllocator) isSameAllocatorLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == lta.member.Member().MemberId
}

// CheckAllocatorLeader checks who is the current Local TSO Allocator leader, and returns true if it is needed to check later.
func (lta *LocalTSOAllocator) CheckAllocatorLeader() (*pdpb.Member, int64, bool) {
	if lta.member.GetEtcdLeader() == 0 {
		log.Error("no etcd leader, check local tso allocator leader later",
			zap.String("dc-location", lta.dcLocation), errs.ZapError(errs.ErrEtcdLeaderNotFound))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	allocatorLeader, rev, err := election.GetLeader(lta.leadership.GetClient(), lta.rootPath)
	if err != nil {
		log.Error("getting local tso allocator leader meets error",
			zap.String("dc-location", lta.dcLocation), errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}
	if allocatorLeader != nil {
		if lta.isSameAllocatorLeader(allocatorLeader) {
			// oh, we are already a Local TSO Allocator leader, which indicates we may meet something wrong
			// in previous CampaignAllocatorLeader. We should delete the leadership and campaign again.
			// In normal case, if a Local TSO Allocator become an allocator leader, it will keep looping
			// in the campaignAllocatorLeader to maintain its leadership. However, the potential failure
			// may occur after an allocator get the leadership and it will return from the campaignAllocatorLeader,
			// which means the election and initialization are not completed fully. By this mean, we should
			// re-campaign by deleting the current allocator leader.
			log.Warn("the local tso allocator leader has not changed, delete and campaign again",
				zap.String("dc-location", lta.dcLocation), zap.Stringer("old-pd-leader", allocatorLeader))
			if err = lta.leadership.DeleteLeader(); err != nil {
				log.Error("deleting local tso allocator leader key meets error", errs.ZapError(err))
				time.Sleep(200 * time.Millisecond)
				return nil, 0, true
			}
		}
	}
	return allocatorLeader, rev, false
}

// WatchAllocatorLeader is used to watch the changes of the Local TSO Allocator leader.
func (lta *LocalTSOAllocator) WatchAllocatorLeader(serverCtx context.Context, allocatorLeader *pdpb.Member, revision int64) {
	lta.setAllocatorLeader(allocatorLeader)
	lta.leadership.Watch(serverCtx, revision)
	lta.unsetAllocatorLeader()
}
