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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	// Deprecated: This is a legacy label, it should be removed in the future.
	GlobalDCLocation = "global"
	// maxUpdateTSORetryCount is the max retry count for updating TSO.
	// When encountering a network partition, manually retrying may help the next request succeed with the new endpoint according to https://github.com/etcd-io/etcd/issues/8711
	maxUpdateTSORetryCount = 3
	// Etcd client retry with `roundRobinQuorumBackoff` (https://github.com/etcd-io/etcd/blob/d62cdeee4863001b09e772ed013eb1342a1d0f89/client/v3/client.go#L488),
	// whose default interval is 25ms, so we sleep 50ms here. (https://github.com/etcd-io/etcd/blob/d62cdeee4863001b09e772ed013eb1342a1d0f89/client/v3/options.go#L53)
	updateTSORetryInterval = 50 * time.Millisecond
)

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// IsLeaderElected returns true if the leader exists; otherwise false.
	IsLeaderElected() bool
	// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
	// otherwise returns a bool which indicates if it is needed to check later.
	CheckLeader() (leader member.ElectionLeader, checkAgain bool)
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// KeepLeader is used to keep the leader's leadership.
	KeepLeader(ctx context.Context)
	// CampaignLeader is used to campaign the leadership and make it become a leader in an election group.
	CampaignLeader(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetLeaderID returns current leader's member ID.
	GetLeaderID() uint64
	// GetLeaderPath returns the path of the leader.
	GetLeaderPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
	// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
	GetLastLeaderUpdatedTime() time.Time
	// PreCheckLeader does some pre-check before checking whether it's the leader.
	PreCheckLeader() error
}

// Allocator is the global single point TSO allocator.
type Allocator struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cfg Config
	// keyspaceGroupID is the keyspace group ID of the allocator.
	keyspaceGroupID uint32
	// for election use
	member ElectionMember
	// expectedPrimaryLease is used to store the expected primary lease.
	expectedPrimaryLease atomic.Value // store as *election.LeaderLease
	timestampOracle      *timestampOracle

	// observability
	tsoAllocatorRoleGauge prometheus.Gauge
	logFields             []zap.Field
}

// NewAllocator creates a new TSO allocator.
func NewAllocator(
	ctx context.Context,
	keyspaceGroupID uint32,
	member ElectionMember,
	storage endpoint.TSOStorage,
	cfg Config,
) *Allocator {
	ctx, cancel := context.WithCancel(ctx)
	keyspaceGroupIDStr := strconv.FormatUint(uint64(keyspaceGroupID), 10)
	a := &Allocator{
		ctx:             ctx,
		cancel:          cancel,
		cfg:             cfg,
		keyspaceGroupID: keyspaceGroupID,
		member:          member,
		timestampOracle: &timestampOracle{
			keyspaceGroupID:        keyspaceGroupID,
			member:                 member,
			storage:                storage,
			saveInterval:           cfg.GetTSOSaveInterval(),
			updatePhysicalInterval: cfg.GetTSOUpdatePhysicalInterval(),
			maxResetTSGap:          cfg.GetMaxResetTSGap,
			tsoMux:                 &tsoObject{},
			metrics:                newTSOMetrics(keyspaceGroupIDStr, GlobalDCLocation),
		},
		tsoAllocatorRoleGauge: tsoAllocatorRole.WithLabelValues(keyspaceGroupIDStr, GlobalDCLocation),
		logFields: []zap.Field{
			logutil.CondUint32("keyspace-group-id", keyspaceGroupID, keyspaceGroupID > 0),
			zap.String("name", member.Name()),
			zap.Uint64("id", member.ID()),
		},
	}

	a.wg.Add(1)
	go a.allocatorUpdater()

	return a
}

// allocatorUpdater is used to run the TSO Allocator updating daemon.
func (a *Allocator) allocatorUpdater() {
	defer logutil.LogPanic()
	defer a.wg.Done()

	tsTicker := time.NewTicker(a.cfg.GetTSOUpdatePhysicalInterval())
	failpoint.Inject("fastUpdatePhysicalInterval", func() {
		tsTicker.Reset(time.Millisecond)
	})
	defer tsTicker.Stop()

	log.Info("entering into allocator update loop", a.logFields...)
	for {
		// Only try to update when the member is leader/primary and the allocator is initialized.
		if !a.isPrimary() || !a.IsInitialize() {
			continue
		}
		if err := a.UpdateTSO(); err != nil {
			log.Warn("failed to update allocator's timestamp", append(a.logFields, errs.ZapError(err))...)
			a.Reset(true)
			// To wait for the allocator to be re-initialized next time.
			continue
		}
		select {
		case <-a.ctx.Done():
			a.Reset(false)
			log.Info("exit the allocator update loop", a.logFields...)
			return
		case <-tsTicker.C:
		}
	}
}

// close is used to shutdown the primary election loop.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (a *Allocator) close() {
	log.Info("closing the allocator", a.logFields...)
	a.cancel()
	a.wg.Wait()
	log.Info("closed the allocator", a.logFields...)
}

// Initialize will initialize the created TSO allocator.
func (a *Allocator) Initialize() error {
	a.tsoAllocatorRoleGauge.Set(1)
	return a.timestampOracle.syncTimestamp()
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (a *Allocator) IsInitialize() bool {
	return a.timestampOracle.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (a *Allocator) UpdateTSO() (err error) {
	for i := range maxUpdateTSORetryCount {
		err = a.timestampOracle.updateTimestamp()
		if err == nil {
			return nil
		}
		log.Warn("try to update the tso but failed",
			zap.Int("retry-count", i), zap.Duration("retry-interval", updateTSORetryInterval), errs.ZapError(err))
		time.Sleep(updateTSORetryInterval)
	}
	return
}

// SetTSO sets the physical part with given TSO.
func (a *Allocator) SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	return a.timestampOracle.resetUserTimestamp(tso, ignoreSmaller, skipUpperBoundCheck)
}

// GenerateTSO is used to generate the given number of TSOs. Make sure you have initialized the TSO allocator before calling this method.
func (a *Allocator) GenerateTSO(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "Allocator.GenerateTSO").End()
	if !a.isPrimary() {
		a.getMetrics().notLeaderEvent.Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}

	return a.timestampOracle.getTS(ctx, count)
}

// Reset is used to reset the TSO allocator, it will also reset the leadership if the `resetLeader` flag is true.
func (a *Allocator) Reset(resetLeadership bool) {
	a.tsoAllocatorRoleGauge.Set(0)
	a.timestampOracle.resetTimestamp()
	// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
	if resetLeadership && a.isPrimary() {
		a.member.ResetLeader()
	}
}

// The PD server will conduct its own leadership election independently of the TSO allocator,
// while the TSO service will manage its leadership election within the TSO allocator.
// This function is used to manually initiate the TSO allocator leadership election loop.
func (a *Allocator) startPrimaryElectionLoop() {
	a.wg.Add(1)
	go a.primaryElectionLoop()
}

// primaryElectionLoop is used to maintain the TSO primary election and TSO's
// running allocator. It is only used in microservice env.
func (a *Allocator) primaryElectionLoop() {
	defer logutil.LogPanic()
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			log.Info("exit the tso primary election loop", a.logFields...)
			return
		default:
		}

		primary, checkAgain := a.member.CheckLeader()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary",
				append(a.logFields, zap.Stringer("tso-primary", primary))...)
			// Watch will keep looping and never return unless the primary has changed.
			primary.Watch(a.ctx)
			log.Info("the tso primary has changed, try to re-campaign a primary",
				append(a.logFields, zap.Stringer("old-tso-primary", primary))...)
		}

		// To make sure the expected primary(if existed) and new primary are on the same server.
		expectedPrimary := mcsutils.GetExpectedPrimaryFlag(a.member.Client(), &keypath.MsParam{
			ServiceName: constant.TSOServiceName,
			GroupID:     a.keyspaceGroupID,
		})
		// skip campaign the primary if the expected primary is not empty and not equal to the current memberValue.
		// expected primary ONLY SET BY `{service}/primary/transfer` API.
		if len(expectedPrimary) > 0 && !strings.Contains(a.member.MemberValue(), expectedPrimary) {
			log.Info("skip campaigning of tso primary and check later", append(a.logFields,
				zap.String("expected-primary-id", expectedPrimary),
				zap.String("cur-member-value", a.member.MemberValue()))...)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		a.campaignLeader()
	}
}

func (a *Allocator) campaignLeader() {
	log.Info("start to campaign the primary", a.logFields...)
	leaderLease := a.cfg.GetLeaderLease()
	if err := a.member.CampaignLeader(a.ctx, leaderLease); err != nil {
		if errors.Is(err, errs.ErrEtcdTxnConflict) {
			log.Info("campaign tso primary meets error due to txn conflict, another tso server may campaign successfully",
				a.logFields...)
		} else if errors.Is(err, errs.ErrCheckCampaign) {
			log.Info("campaign tso primary meets error due to pre-check campaign failed, the tso keyspace group may be in split",
				a.logFields...)
		} else {
			log.Error("campaign tso primary meets error due to etcd error", append(a.logFields, errs.ZapError(err))...)
		}
		return
	}

	// Start keepalive the leadership and enable TSO service.
	// TSO service is strictly enabled/disabled by the leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.
	ctx, cancel := context.WithCancel(a.ctx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		a.member.ResetLeader()
	})

	// maintain the leadership, after this, TSO can be service.
	a.member.KeepLeader(ctx)
	log.Info("campaign tso primary ok", a.logFields...)

	log.Info("initializing the tso allocator")
	if err := a.Initialize(); err != nil {
		log.Error("failed to initialize the tso allocator", append(a.logFields, errs.ZapError(err))...)
		return
	}
	defer func() {
		// Leader will be reset in `resetLeaderOnce` later.
		a.Reset(false)
	}()

	// check expected primary and watch the primary.
	exitPrimary := make(chan struct{})
	lease, err := mcsutils.KeepExpectedPrimaryAlive(ctx, a.member.Client(), exitPrimary,
		leaderLease, &keypath.MsParam{
			ServiceName: constant.TSOServiceName,
			GroupID:     a.keyspaceGroupID,
		}, a.member.MemberValue())
	if err != nil {
		log.Error("prepare tso primary watch error", append(a.logFields, errs.ZapError(err))...)
		return
	}
	a.expectedPrimaryLease.Store(lease)
	a.member.EnableLeader()

	tsoLabel := fmt.Sprintf("TSO Service Group %d", a.keyspaceGroupID)
	member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(1)
	defer resetLeaderOnce.Do(func() {
		cancel()
		a.member.ResetLeader()
		member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(0)
	})

	log.Info("tso primary is ready to serve", a.logFields...)

	leaderTicker := time.NewTicker(constant.LeaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !a.isPrimary() {
				log.Info("no longer a primary because lease has expired, the tso primary will step down", a.logFields...)
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("exit leader campaign", a.logFields...)
			return
		case <-exitPrimary:
			log.Info("no longer be primary because primary have been updated, the TSO primary will step down", a.logFields...)
			return
		}
	}
}

// GetPrimaryAddr returns the address of primary in the election group.
func (a *Allocator) GetPrimaryAddr() string {
	if a == nil || a.member == nil {
		return ""
	}
	leaderAddrs := a.member.GetLeaderListenUrls()
	if len(leaderAddrs) < 1 {
		return ""
	}
	return leaderAddrs[0]
}

// GetMember returns the member of the allocator.
func (a *Allocator) GetMember() ElectionMember {
	return a.member
}

func (a *Allocator) isPrimary() bool {
	if a == nil || a.member == nil {
		return false
	}
	return a.member.IsLeader()
}

func (a *Allocator) isPrimaryElected() bool {
	if a == nil || a.member == nil {
		return false
	}
	return a.member.IsLeaderElected()
}

// GetExpectedPrimaryLease returns the expected primary lease.
func (a *Allocator) GetExpectedPrimaryLease() *election.Lease {
	l := a.expectedPrimaryLease.Load()
	if l == nil {
		return nil
	}
	return l.(*election.Lease)
}

func (a *Allocator) getMetrics() *tsoMetrics {
	return a.timestampOracle.metrics
}
