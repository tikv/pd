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
	"errors"
	"fmt"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// Allocator is a Timestamp Oracle allocator.
type Allocator interface {
	// Initialize is used to initialize a TSO allocator.
	// It will synchronize TSO with etcd and initialize the
	// memory for later allocation work.
	Initialize(suffix int) error
	// IsInitialize is used to indicates whether this allocator is initialized.
	IsInitialize() bool
	// UpdateTSO is used to update the TSO in memory and the time window in etcd.
	UpdateTSO() error
	// SetTSO sets the physical part with given TSO. It's mainly used for BR restore.
	// Cannot set the TSO smaller than now in any case.
	// if ignoreSmaller=true, if input ts is smaller than current, ignore silently, else return error
	// if skipUpperBoundCheck=true, skip tso upper bound check
	SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error
	// GenerateTSO is used to generate a given number of TSOs.
	// Make sure you have initialized the TSO allocator before calling.
	GenerateTSO(ctx context.Context, count uint32) (pdpb.Timestamp, error)
	// Reset is used to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
// TODO: Local TSO allocator is deprecated now, we can update the name to
// TSOAllocator and remove the `Global` concept.
type GlobalTSOAllocator struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// for global TSO synchronization
	am *AllocatorManager
	// for election use
	member ElectionMember
	// expectedPrimaryLease is used to store the expected primary lease.
	expectedPrimaryLease atomic.Value // store as *election.LeaderLease
	timestampOracle      *timestampOracle
	// pre-initialized metrics
	tsoAllocatorRoleGauge prometheus.Gauge
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(
	ctx context.Context,
	am *AllocatorManager,
) Allocator {
	ctx, cancel := context.WithCancel(ctx)
	gta := &GlobalTSOAllocator{
		ctx:                   ctx,
		cancel:                cancel,
		am:                    am,
		member:                am.member,
		timestampOracle:       newGlobalTimestampOracle(am),
		tsoAllocatorRoleGauge: tsoAllocatorRole.WithLabelValues(am.getGroupIDStr(), GlobalDCLocation),
	}

	return gta
}

func newGlobalTimestampOracle(am *AllocatorManager) *timestampOracle {
	oracle := &timestampOracle{
		client:                 am.member.GetLeadership().GetClient(),
		keyspaceGroupID:        am.kgID,
		tsPath:                 keypath.KeyspaceGroupGlobalTSPath(am.kgID),
		storage:                am.storage,
		saveInterval:           am.saveInterval,
		updatePhysicalInterval: am.updatePhysicalInterval,
		maxResetTSGap:          am.maxResetTSGap,
		tsoMux:                 &tsoObject{},
		metrics:                newTSOMetrics(am.getGroupIDStr(), GlobalDCLocation),
	}
	return oracle
}

// close is used to shutdown the primary election loop.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (gta *GlobalTSOAllocator) close() {
	gta.cancel()
	gta.wg.Wait()
}

// getGroupID returns the keyspace group ID of the allocator.
func (gta *GlobalTSOAllocator) getGroupID() uint32 {
	if gta.am == nil {
		return 0
	}
	return gta.am.getGroupID()
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize(int) error {
	gta.tsoAllocatorRoleGauge.Set(1)
	return gta.timestampOracle.SyncTimestamp()
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (gta *GlobalTSOAllocator) IsInitialize() bool {
	return gta.timestampOracle.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.timestampOracle.UpdateTimestamp()
}

// SetTSO sets the physical part with given TSO.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	return gta.timestampOracle.resetUserTimestamp(gta.member.GetLeadership(), tso, ignoreSmaller, skipUpperBoundCheck)
}

// GenerateTSO is used to generate the given number of TSOs.
// Make sure you have initialized the TSO allocator before calling this method.
// Basically, there are two ways to generate a Global TSO:
//  1. The old way to generate a normal TSO from memory directly, which makes the TSO service node become single point.
//  2. Deprecated: The new way to generate a Global TSO by synchronizing with all other Local TSO Allocators.
func (gta *GlobalTSOAllocator) GenerateTSO(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "GlobalTSOAllocator.GenerateTSO").End()
	if !gta.member.GetLeadership().Check() {
		gta.getMetrics().notLeaderEvent.Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}

	return gta.timestampOracle.getTS(ctx, gta.member.GetLeadership(), count)
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.tsoAllocatorRoleGauge.Set(0)
	gta.timestampOracle.ResetTimestamp()
}

// primaryElectionLoop is used to maintain the TSO primary election and TSO's
// running allocator. It is only used in microservice env.
func (gta *GlobalTSOAllocator) primaryElectionLoop() {
	defer logutil.LogPanic()
	defer gta.wg.Done()

	for {
		select {
		case <-gta.ctx.Done():
			log.Info("exit the global tso primary election loop",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0))
			return
		default:
		}

		primary, checkAgain := gta.member.CheckLeader()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				zap.String("campaign-tso-primary-name", gta.member.Name()),
				zap.Stringer("tso-primary", primary))
			// Watch will keep looping and never return unless the primary has changed.
			primary.Watch(gta.ctx)
			log.Info("the tso primary has changed, try to re-campaign a primary",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0))
		}

		// To make sure the expected primary(if existed) and new primary are on the same server.
		expectedPrimary := mcsutils.GetExpectedPrimaryFlag(gta.member.Client(), &keypath.MsParam{
			ServiceName: constant.TSOServiceName,
			GroupID:     gta.getGroupID(),
		})
		// skip campaign the primary if the expected primary is not empty and not equal to the current memberValue.
		// expected primary ONLY SET BY `{service}/primary/transfer` API.
		if len(expectedPrimary) > 0 && !strings.Contains(gta.member.MemberValue(), expectedPrimary) {
			log.Info("skip campaigning of tso primary and check later",
				zap.String("server-name", gta.member.Name()),
				zap.String("expected-primary-id", expectedPrimary),
				zap.Uint64("member-id", gta.member.ID()),
				zap.String("cur-member-value", gta.member.MemberValue()))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		gta.campaignLeader()
	}
}

func (gta *GlobalTSOAllocator) campaignLeader() {
	log.Info("start to campaign the primary",
		logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
		zap.String("campaign-tso-primary-name", gta.member.Name()))
	if err := gta.am.member.CampaignLeader(gta.ctx, gta.am.leaderLease); err != nil {
		if errors.Is(err, errs.ErrEtcdTxnConflict) {
			log.Info("campaign tso primary meets error due to txn conflict, another tso server may campaign successfully",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				zap.String("campaign-tso-primary-name", gta.member.Name()))
		} else if errors.Is(err, errs.ErrCheckCampaign) {
			log.Info("campaign tso primary meets error due to pre-check campaign failed, the tso keyspace group may be in split",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				zap.String("campaign-tso-primary-name", gta.member.Name()))
		} else {
			log.Error("campaign tso primary meets error due to etcd error",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				zap.String("campaign-tso-primary-name", gta.member.Name()), errs.ZapError(err))
		}
		return
	}

	// Start keepalive the leadership and enable TSO service.
	// TSO service is strictly enabled/disabled by the leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.
	ctx, cancel := context.WithCancel(gta.ctx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		gta.member.ResetLeader()
	})

	// maintain the leadership, after this, TSO can be service.
	gta.member.KeepLeader(ctx)
	log.Info("campaign tso primary ok",
		logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
		zap.String("campaign-tso-primary-name", gta.member.Name()))

	log.Info("initializing the global tso allocator")
	if err := gta.am.GetAllocator().Initialize(0); err != nil {
		log.Error("failed to initialize the global tso allocator",
			logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
			errs.ZapError(err))
		return
	}
	defer func() {
		gta.am.ResetAllocatorGroup(false)
	}()

	// check expected primary and watch the primary.
	exitPrimary := make(chan struct{})
	lease, err := mcsutils.KeepExpectedPrimaryAlive(ctx, gta.member.Client(), exitPrimary,
		gta.am.leaderLease, &keypath.MsParam{
			ServiceName: constant.TSOServiceName,
			GroupID:     gta.getGroupID(),
		}, gta.member.MemberValue())
	if err != nil {
		log.Error("prepare tso primary watch error", errs.ZapError(err))
		return
	}
	gta.expectedPrimaryLease.Store(lease)
	gta.member.EnableLeader()

	tsoLabel := fmt.Sprintf("TSO Service Group %d", gta.getGroupID())
	member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(1)
	defer resetLeaderOnce.Do(func() {
		cancel()
		gta.member.ResetLeader()
		member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(0)
	})

	log.Info("tso primary is ready to serve",
		logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
		zap.String("tso-primary-name", gta.member.Name()))

	leaderTicker := time.NewTicker(constant.LeaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !gta.member.IsLeader() {
				log.Info("no longer a primary because lease has expired, the tso primary will step down",
					logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("exit leader campaign",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0))
			return
		case <-exitPrimary:
			log.Info("no longer be primary because primary have been updated, the TSO primary will step down")
			return
		}
	}
}

// GetExpectedPrimaryLease returns the expected primary lease.
func (gta *GlobalTSOAllocator) GetExpectedPrimaryLease() *election.Lease {
	l := gta.expectedPrimaryLease.Load()
	if l == nil {
		return nil
	}
	return l.(*election.Lease)
}

func (gta *GlobalTSOAllocator) getMetrics() *tsoMetrics {
	return gta.timestampOracle.metrics
}
