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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// LocalTSOAllocator is the DC-level local TSO allocator,
// which is only used to allocate TSO in one DC each.
// One PD server may hold multiple Local TSO Allocators.
type LocalTSOAllocator struct {
	allocatorManager *AllocatorManager
	// leadership is used to campaign the corresponding DC's Local TSO Allocator.
	leadership      *election.Leadership
	timestampOracle *timestampOracle
	// for election use, notice that the leadership that member holds is
	// the leadership for PD leader. Local TSO Allocator's leadership is for the
	// election of Local TSO Allocator leader among several PD servers and
	// Local TSO Allocator only use member's some etcd and pdpb.Member info.
	// So it's not conflicted.
	rootPath        string
	allocatorLeader atomic.Value // stored as *pdpb.Member
	// pre-initialized metrics
	tsoAllocatorRoleGauge prometheus.Gauge
}

// NewLocalTSOAllocator creates a new local TSO allocator.
func NewLocalTSOAllocator(
	am *AllocatorManager,
	leadership *election.Leadership,
	dcLocation string,
) Allocator {
	return &LocalTSOAllocator{
		allocatorManager:      am,
		leadership:            leadership,
		timestampOracle:       newLocalTimestampOracle(am, leadership, dcLocation),
		rootPath:              leadership.GetLeaderKey(),
		tsoAllocatorRoleGauge: tsoAllocatorRole.WithLabelValues(am.getGroupIDStr(), dcLocation),
	}
}

func newLocalTimestampOracle(am *AllocatorManager, leadership *election.Leadership, dcLocation string) *timestampOracle {
	oracle := &timestampOracle{
		client:                 leadership.GetClient(),
		keyspaceGroupID:        am.kgID,
		tsPath:                 keypath.KeyspaceGroupLocalTSPath(localTSOAllocatorEtcdPrefix, am.kgID, dcLocation),
		storage:                am.storage,
		saveInterval:           am.saveInterval,
		updatePhysicalInterval: am.updatePhysicalInterval,
		maxResetTSGap:          am.maxResetTSGap,
		dcLocation:             dcLocation,
		tsoMux:                 &tsoObject{},
		metrics:                newTSOMetrics(am.getGroupIDStr(), dcLocation),
	}
	return oracle
}

// GetTimestampPath returns the timestamp path in etcd.
func (lta *LocalTSOAllocator) GetTimestampPath() string {
	if lta == nil || lta.timestampOracle == nil {
		return ""
	}
	return lta.timestampOracle.GetTimestampPath()
}

// GetDCLocation returns the local allocator's dc-location.
func (lta *LocalTSOAllocator) GetDCLocation() string {
	return lta.timestampOracle.dcLocation
}

// Initialize will initialize the created local TSO allocator.
func (lta *LocalTSOAllocator) Initialize(suffix int) error {
	lta.tsoAllocatorRoleGauge.Set(1)
	lta.timestampOracle.suffix = suffix
	return lta.timestampOracle.SyncTimestamp()
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (lta *LocalTSOAllocator) IsInitialize() bool {
	return lta.timestampOracle.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd
// for all local TSO allocators this PD server hold.
func (lta *LocalTSOAllocator) UpdateTSO() error {
	return lta.timestampOracle.UpdateTimestamp()
}

// SetTSO sets the physical part with given TSO.
func (lta *LocalTSOAllocator) SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	return lta.timestampOracle.resetUserTimestampInner(lta.leadership, tso, ignoreSmaller, skipUpperBoundCheck)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (lta *LocalTSOAllocator) GenerateTSO(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "LocalTSOAllocator.GenerateTSO").End()
	if !lta.leadership.Check() {
		lta.getMetrics().notLeaderEvent.Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(
			fmt.Sprintf("requested pd %s of %s allocator", errs.NotLeaderErr, lta.timestampOracle.dcLocation))
	}
	return lta.timestampOracle.getTS(ctx, lta.leadership, count, lta.allocatorManager.GetSuffixBits())
}

// Reset is used to reset the TSO allocator.
func (lta *LocalTSOAllocator) Reset() {
	lta.tsoAllocatorRoleGauge.Set(0)
	lta.timestampOracle.ResetTimestamp()
}

// setAllocatorLeader sets the current Local TSO Allocator leader.
func (lta *LocalTSOAllocator) setAllocatorLeader(member any) {
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
func (lta *LocalTSOAllocator) GetMember() ElectionMember {
	return lta.allocatorManager.member
}

// GetCurrentTSO returns current TSO in memory.
func (lta *LocalTSOAllocator) GetCurrentTSO() (*pdpb.Timestamp, error) {
	currentPhysical, currentLogical := lta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return &pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// WriteTSO is used to set the maxTS as current TSO in memory.
func (lta *LocalTSOAllocator) WriteTSO(maxTS *pdpb.Timestamp) error {
	currentTSO, err := lta.GetCurrentTSO()
	if err != nil {
		return err
	}
	// If current local TSO has already been greater or equal to maxTS, then do not update it.
	if tsoutil.CompareTimestamp(currentTSO, maxTS) >= 0 {
		return nil
	}
	return lta.timestampOracle.resetUserTimestamp(context.Background(), lta.leadership, tsoutil.GenerateTS(maxTS), true)
}

// EnableAllocatorLeader sets the Local TSO Allocator itself to a leader.
func (lta *LocalTSOAllocator) EnableAllocatorLeader() {
	lta.setAllocatorLeader(lta.allocatorManager.member.GetMember())
}

// CampaignAllocatorLeader is used to campaign a Local TSO Allocator's leadership.
func (lta *LocalTSOAllocator) CampaignAllocatorLeader(leaseTimeout int64, cmps ...clientv3.Cmp) error {
	return lta.leadership.Campaign(leaseTimeout, lta.allocatorManager.member.MemberValue(), cmps...)
}

// KeepAllocatorLeader is used to keep the PD leader's leadership.
func (lta *LocalTSOAllocator) KeepAllocatorLeader(ctx context.Context) {
	defer logutil.LogPanic()
	lta.leadership.Keep(ctx)
}

// IsAllocatorLeader returns whether the allocator is still a
// Local TSO Allocator leader by checking its leadership's lease and leader info.
func (lta *LocalTSOAllocator) IsAllocatorLeader() bool {
	return lta.leadership.Check() && lta.GetAllocatorLeader().GetMemberId() == lta.GetMember().ID()
}

// isSameAllocatorLeader checks whether a server is the leader itself.
func (lta *LocalTSOAllocator) isSameAllocatorLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == lta.allocatorManager.member.ID()
}

// CheckAllocatorLeader checks who is the current Local TSO Allocator leader, and returns true if it is needed to check later.
func (lta *LocalTSOAllocator) CheckAllocatorLeader() (*pdpb.Member, int64, bool) {
	if err := lta.allocatorManager.member.PreCheckLeader(); err != nil {
		log.Error("no etcd leader, check local tso allocator leader later",
			zap.String("dc-location", lta.timestampOracle.dcLocation), errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	allocatorLeader, rev, err := election.GetLeader(lta.leadership.GetClient(), lta.rootPath)
	if err != nil {
		log.Error("getting local tso allocator leader meets error",
			zap.String("dc-location", lta.timestampOracle.dcLocation), errs.ZapError(err))
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
				zap.String("dc-location", lta.timestampOracle.dcLocation), zap.Stringer("old-pd-leader", allocatorLeader))
			// Delete the leader itself and let others start a new election again.
			if err = lta.leadership.DeleteLeaderKey(); err != nil {
				log.Error("deleting local tso allocator leader key meets error", errs.ZapError(err))
				time.Sleep(200 * time.Millisecond)
				return nil, 0, true
			}
			// Return nil and false to make sure the campaign will start immediately.
			return nil, 0, false
		}
	}
	return allocatorLeader, rev, false
}

// WatchAllocatorLeader is used to watch the changes of the Local TSO Allocator leader.
func (lta *LocalTSOAllocator) WatchAllocatorLeader(serverCtx context.Context, allocatorLeader *pdpb.Member, revision int64) {
	lta.setAllocatorLeader(allocatorLeader)
	// Check the cluster dc-locations to update the max suffix bits
	go lta.allocatorManager.ClusterDCLocationChecker()
	lta.leadership.Watch(serverCtx, revision)
	lta.unsetAllocatorLeader()
}

func (lta *LocalTSOAllocator) getMetrics() *tsoMetrics {
	return lta.timestampOracle.metrics
}
