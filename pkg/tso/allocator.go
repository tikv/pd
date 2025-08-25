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

// Allocator is the global single point TSO allocator.
type Allocator struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cfg Config
	// keyspaceGroupID is the keyspace group ID of the allocator.
	keyspaceGroupID uint32
	// for election use
	member member.Election
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
	member member.Election,
	storage endpoint.TSOStorage,
	cfg Config,
) *Allocator {
	ctx, cancel := context.WithCancel(ctx)
	keyspaceGroupIDStr := strconv.FormatUint(uint64(keyspaceGroupID), 10)
	maxIndex, uniqueIndex := cfg.GetTSOIndex()
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
			uniqueIndex:            uniqueIndex,
			maxIndex:               maxIndex,
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
		select {
		case <-tsTicker.C:
			// Only try to update when the member is serving and the allocator is initialized.
			if !a.isServing() || !a.IsInitialize() {
				continue
			}
			if err := a.UpdateTSO(); err != nil {
				log.Warn("failed to update allocator's timestamp", append(a.logFields, errs.ZapError(err))...)
				a.Reset(true)
				// To wait for the allocator to be re-initialized next time.
				continue
			}
		case <-a.ctx.Done():
			a.Reset(false)
			log.Info("exit the allocator update loop", a.logFields...)
			return
		}
	}
}

// Close is used to close the allocator and shutdown all the daemon loops.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (a *Allocator) Close() {
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
	if !a.isServing() {
		// "leader" is not suitable name, but we keep it for compatibility.
		a.getMetrics().notLeaderEvent.Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}

	return a.timestampOracle.getTS(ctx, count)
}

// Reset is used to reset the TSO allocator, it will also reset the leadership if the `resetLeadership` flag is true.
func (a *Allocator) Reset(resetLeadership bool) {
	a.tsoAllocatorRoleGauge.Set(0)
	a.timestampOracle.resetTimestamp()
	// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
	if resetLeadership && a.isServing() {
		a.member.Resign()
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
		m := a.member.(*member.Participant)
		primary, checkAgain := m.CheckPrimary()
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
				zap.String("cur-member-value", m.ParticipantString()))...)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		a.campaignPrimary()
	}
}

func (a *Allocator) campaignPrimary() {
	log.Info("start to campaign the primary", a.logFields...)
	lease := a.cfg.GetLease()
	if err := a.member.Campaign(a.ctx, lease); err != nil {
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
	var resetPrimaryOnce sync.Once
	defer resetPrimaryOnce.Do(func() {
		cancel()
		a.member.Resign()
	})

	// maintain the leadership, after this, TSO can be service.
	a.member.GetLeadership().Keep(ctx)
	log.Info("campaign tso primary ok", a.logFields...)

	log.Info("initializing the tso allocator")
	if err := a.Initialize(); err != nil {
		log.Error("failed to initialize the tso allocator", append(a.logFields, errs.ZapError(err))...)
		return
	}
	defer func() {
		// Primary will be reset in `resetPrimaryOnce` later.
		a.Reset(false)
	}()

	// check expected primary and watch the primary.
	exitPrimary := make(chan struct{})
	primaryLease, err := mcsutils.KeepExpectedPrimaryAlive(ctx, a.member.Client(), exitPrimary,
		lease, &keypath.MsParam{
			ServiceName: constant.TSOServiceName,
			GroupID:     a.keyspaceGroupID,
		}, a.member.(*member.Participant))
	if err != nil {
		log.Error("prepare tso primary watch error", append(a.logFields, errs.ZapError(err))...)
		return
	}
	a.expectedPrimaryLease.Store(primaryLease)
	a.member.PromoteSelf()

	tsoLabel := fmt.Sprintf("TSO Service Group %d", a.keyspaceGroupID)
	member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(1)
	defer resetPrimaryOnce.Do(func() {
		cancel()
		a.member.Resign()
		member.ServiceMemberGauge.WithLabelValues(tsoLabel).Set(0)
	})

	log.Info("tso primary is ready to serve", a.logFields...)

	primaryTicker := time.NewTicker(constant.PrimaryTickInterval)
	defer primaryTicker.Stop()

	for {
		select {
		case <-primaryTicker.C:
			if !a.isServing() {
				log.Info("no longer a primary because lease has expired, the tso primary will step down", a.logFields...)
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("exit primary campaign", a.logFields...)
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
	primaryAddrs := a.member.GetServingUrls()
	if len(primaryAddrs) < 1 {
		return ""
	}
	return primaryAddrs[0]
}

// GetMember returns the member of the allocator.
func (a *Allocator) GetMember() member.Election {
	return a.member
}

// isServing returns whether the member is serving or not.
// For PD, whether the member is the leader.
// For microservices, whether the participant is the primary.
func (a *Allocator) isServing() bool {
	if a == nil || a.member == nil {
		return false
	}
	return a.member.IsServing()
}

func (a *Allocator) isPrimaryElected() bool {
	if a == nil || a.member == nil {
		return false
	}
	return a.member.(*member.Participant).IsPrimaryElected()
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
