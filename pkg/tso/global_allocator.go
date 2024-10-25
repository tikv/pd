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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
	// GetTimestampPath returns the timestamp path in etcd, which is:
	// 1. for the default keyspace group:
	//     a. timestamp in /pd/{cluster_id}/timestamp
	//     b. lta/{dc-location}/timestamp in /pd/{cluster_id}/lta/{dc-location}/timestamp
	// 1. for the non-default keyspace groups:
	//     a. {group}/gts/timestamp in /ms/{cluster_id}/tso/{group}/gta/timestamp
	//     b. {group}/lts/{dc-location}/timestamp in /ms/{cluster_id}/tso/{group}/lta/{dc-location}/timestamp
	GetTimestampPath() string
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
	// syncRTT is the RTT duration a SyncMaxTS RPC call will cost,
	// which is used to estimate the MaxTS in a Global TSO generation
	// to reduce the gRPC network IO latency.
	syncRTT atomic.Value // store as int64 milliseconds
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
		dcLocation:             GlobalDCLocation,
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

func (gta *GlobalTSOAllocator) setSyncRTT(rtt int64) {
	gta.syncRTT.Store(rtt)
	gta.getMetrics().globalTSOSyncRTTGauge.Set(float64(rtt))
}

func (gta *GlobalTSOAllocator) getSyncRTT() int64 {
	syncRTT := gta.syncRTT.Load()
	if syncRTT == nil {
		return 0
	}
	return syncRTT.(int64)
}

// GetTimestampPath returns the timestamp path in etcd.
func (gta *GlobalTSOAllocator) GetTimestampPath() string {
	if gta == nil || gta.timestampOracle == nil {
		return ""
	}
	return gta.timestampOracle.GetTimestampPath()
}

func (gta *GlobalTSOAllocator) estimateMaxTS(ctx context.Context, count uint32, suffixBits int) (*pdpb.Timestamp, bool, error) {
	physical, logical, lastUpdateTime := gta.timestampOracle.generateTSO(ctx, int64(count), 0)
	if physical == 0 {
		return &pdpb.Timestamp{}, false, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	estimatedMaxTSO := &pdpb.Timestamp{
		Physical: physical + time.Since(lastUpdateTime).Milliseconds() + 2*gta.getSyncRTT(), // TODO: make the coefficient of RTT configurable
		Logical:  logical,
	}
	// Precheck to make sure the logical part won't overflow after being differentiated.
	// If precheckLogical returns false, it means the logical part is overflow,
	// we need to wait a updatePhysicalInterval and retry the estimation later.
	if !gta.precheckLogical(estimatedMaxTSO, suffixBits) {
		return nil, true, nil
	}
	return estimatedMaxTSO, false, nil
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize(int) error {
	gta.tsoAllocatorRoleGauge.Set(1)
	// The suffix of a Global TSO should always be 0.
	gta.timestampOracle.suffix = 0
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
	return gta.timestampOracle.resetUserTimestampInner(gta.member.GetLeadership(), tso, ignoreSmaller, skipUpperBoundCheck)
}

// GenerateTSO is used to generate the given number of TSOs.
// Make sure you have initialized the TSO allocator before calling this method.
// Basically, there are two ways to generate a Global TSO:
//  1. The old way to generate a normal TSO from memory directly, which makes the TSO service node become single point.
//  2. The new way to generate a Global TSO by synchronizing with all other Local TSO Allocators.
//
// And for the new way, there are two different strategies:
//  1. Collect the max Local TSO from all Local TSO Allocator leaders and write it back to them as MaxTS.
//  2. Estimate a MaxTS and try to write it to all Local TSO Allocator leaders directly to reduce the RTT.
//     During the process, if the estimated MaxTS is not accurate, it will fallback to the collecting way.
func (gta *GlobalTSOAllocator) GenerateTSO(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "GlobalTSOAllocator.GenerateTSO").End()
	if !gta.member.GetLeadership().Check() {
		gta.getMetrics().notLeaderEvent.Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}
	// To check if we have any dc-location configured in the cluster
	dcLocationMap := gta.am.GetClusterDCLocations()
	// No dc-locations configured in the cluster, use the normal Global TSO generation way.
	// (without synchronization with other Local TSO Allocators)
	if len(dcLocationMap) == 0 {
		return gta.timestampOracle.getTS(ctx, gta.member.GetLeadership(), count, 0)
	}
	ctx1 := ctx

	// Have dc-locations configured in the cluster, use the Global TSO generation way.
	// (whit synchronization with other Local TSO Allocators)
	ctx, cancel := context.WithCancel(gta.ctx)
	defer cancel()
	for range maxRetryCount {
		var (
			err                    error
			shouldRetry, skipCheck bool
			globalTSOResp          pdpb.Timestamp
			estimatedMaxTSO        *pdpb.Timestamp
			suffixBits             = gta.am.GetSuffixBits()
		)
		// TODO: add a switch to control whether to enable the MaxTSO estimation.
		// 1. Estimate a MaxTS among all Local TSO Allocator leaders according to the RTT.
		estimatedMaxTSO, shouldRetry, err = gta.estimateMaxTS(ctx1, count, suffixBits)
		if err != nil {
			log.Error("global tso allocator estimates MaxTS failed",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				errs.ZapError(err))
			continue
		}
		if shouldRetry {
			time.Sleep(gta.timestampOracle.updatePhysicalInterval)
			continue
		}
	SETTING_PHASE:
		// 2. Send the MaxTSO to all Local TSO Allocators leaders to make sure the subsequent Local TSOs will be bigger than it.
		// It's not safe to skip check at the first time here because the estimated maxTSO may not be big enough,
		// we need to validate it first before we write it into every Local TSO Allocator's memory.
		globalTSOResp = *estimatedMaxTSO
		if err = gta.SyncMaxTS(ctx, dcLocationMap, &globalTSOResp, skipCheck); err != nil {
			log.Error("global tso allocator synchronizes MaxTS failed",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				errs.ZapError(err))
			continue
		}
		// 3. If skipCheck is false and the maxTSO is bigger than estimatedMaxTSO,
		// we need to redo the setting phase with the bigger one and skip the check safely.
		if !skipCheck && tsoutil.CompareTimestamp(&globalTSOResp, estimatedMaxTSO) > 0 {
			gta.getMetrics().globalTSOSyncEvent.Inc()
			*estimatedMaxTSO = globalTSOResp
			// Re-add the count and check the overflow.
			estimatedMaxTSO.Logical += int64(count)
			if !gta.precheckLogical(estimatedMaxTSO, suffixBits) {
				estimatedMaxTSO.Physical += UpdateTimestampGuard.Milliseconds()
				estimatedMaxTSO.Logical = int64(count)
			}
			skipCheck = true
			goto SETTING_PHASE
		}
		// Is skipCheck is false and globalTSOResp remains the same, it means the estimatedTSO is valid.
		if !skipCheck && tsoutil.CompareTimestamp(&globalTSOResp, estimatedMaxTSO) == 0 {
			gta.getMetrics().globalTSOEstimateEvent.Inc()
		}
		// 4. Persist MaxTS into memory, and etcd if needed
		var currentGlobalTSO *pdpb.Timestamp
		if currentGlobalTSO, err = gta.getCurrentTSO(ctx1); err != nil {
			log.Error("global tso allocator gets the current global tso in memory failed",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				errs.ZapError(err))
			continue
		}
		if tsoutil.CompareTimestamp(currentGlobalTSO, &globalTSOResp) < 0 {
			gta.getMetrics().globalTSOPersistEvent.Inc()
			// Update the Global TSO in memory
			if err = gta.timestampOracle.resetUserTimestamp(ctx1, gta.member.GetLeadership(), tsoutil.GenerateTS(&globalTSOResp), true); err != nil {
				gta.getMetrics().errGlobalTSOPersistEvent.Inc()
				log.Error("global tso allocator update the global tso in memory failed",
					logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
					errs.ZapError(err))
				continue
			}
		}
		// 5. Check leadership again before we returning the response.
		if !gta.member.GetLeadership().Check() {
			gta.getMetrics().notLeaderAnymoreEvent.Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested %s anymore", errs.NotLeaderErr))
		}
		// 6. Calibrate the logical part to make the TSO unique globally by giving it a unique suffix in the whole cluster
		globalTSOResp.Logical = gta.timestampOracle.calibrateLogical(globalTSOResp.GetLogical(), suffixBits)
		globalTSOResp.SuffixBits = uint32(suffixBits)
		return globalTSOResp, nil
	}
	gta.getMetrics().exceededMaxRetryEvent.Inc()
	return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("global tso allocator maximum number of retries exceeded")
}

// Only used for test
var globalTSOOverflowFlag = true

func (gta *GlobalTSOAllocator) precheckLogical(maxTSO *pdpb.Timestamp, suffixBits int) bool {
	failpoint.Inject("globalTSOOverflow", func() {
		if globalTSOOverflowFlag {
			maxTSO.Logical = maxLogical
			globalTSOOverflowFlag = false
		}
	})
	// Make sure the physical time is not empty again.
	if maxTSO.GetPhysical() == 0 {
		return false
	}
	// Check if the logical part will reach the overflow condition after being differentiated.
	if caliLogical := gta.timestampOracle.calibrateLogical(maxTSO.Logical, suffixBits); caliLogical >= maxLogical {
		log.Error("estimated logical part outside of max logical interval, please check ntp time",
			logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
			zap.Reflect("max-tso", maxTSO), errs.ZapError(errs.ErrLogicOverflow))
		gta.getMetrics().precheckLogicalOverflowEvent.Inc()
		return false
	}
	return true
}

const (
	dialTimeout = 3 * time.Second
	rpcTimeout  = 3 * time.Second
	// TODO: maybe make syncMaxRetryCount configurable
	syncMaxRetryCount = 2
)

type syncResp struct {
	rpcRes *pdpb.SyncMaxTSResponse
	err    error
	rtt    time.Duration
}

// SyncMaxTS is used to sync MaxTS with all Local TSO Allocator leaders in dcLocationMap.
// If maxTSO is the biggest TSO among all Local TSO Allocators, it will be written into
// each allocator and remains the same after the synchronization.
// If not, it will be replaced with the new max Local TSO and return.
func (gta *GlobalTSOAllocator) SyncMaxTS(
	ctx context.Context,
	dcLocationMap map[string]DCLocationInfo,
	maxTSO *pdpb.Timestamp,
	skipCheck bool,
) error {
	defer trace.StartRegion(ctx, "GlobalTSOAllocator.SyncMaxTS").End()
	originalMaxTSO := *maxTSO
	for i := range syncMaxRetryCount {
		// Collect all allocator leaders' client URLs
		allocatorLeaders := make(map[string]*pdpb.Member)
		for dcLocation := range dcLocationMap {
			allocator, err := gta.am.GetAllocator(dcLocation)
			if err != nil {
				return err
			}
			allocatorLeader := allocator.(*LocalTSOAllocator).GetAllocatorLeader()
			if allocatorLeader.GetMemberId() == 0 {
				return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Sprintf("%s does not have the local allocator leader yet", dcLocation))
			}
			allocatorLeaders[dcLocation] = allocatorLeader
		}
		leaderURLs := make([]string, 0)
		for _, allocator := range allocatorLeaders {
			// Check if its client URLs are empty
			if len(allocator.GetClientUrls()) < 1 {
				continue
			}
			leaderURL := allocator.GetClientUrls()[0]
			if slice.NoneOf(leaderURLs, func(i int) bool { return leaderURLs[i] == leaderURL }) {
				leaderURLs = append(leaderURLs, leaderURL)
			}
		}
		// Prepare to make RPC requests concurrently
		respCh := make(chan *syncResp, len(leaderURLs))
		wg := sync.WaitGroup{}
		request := &pdpb.SyncMaxTSRequest{
			Header: &pdpb.RequestHeader{
				SenderId: gta.am.member.ID(),
			},
			SkipCheck: skipCheck,
			MaxTs:     maxTSO,
		}
		for _, leaderURL := range leaderURLs {
			leaderConn, err := gta.am.getOrCreateGRPCConn(ctx, leaderURL)
			if err != nil {
				return err
			}
			// Send SyncMaxTSRequest to all allocator leaders concurrently.
			wg.Add(1)
			go func(ctx context.Context, conn *grpc.ClientConn, respCh chan<- *syncResp) {
				defer logutil.LogPanic()
				defer wg.Done()
				syncMaxTSResp := &syncResp{}
				syncCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
				startTime := time.Now()
				syncMaxTSResp.rpcRes, syncMaxTSResp.err = pdpb.NewPDClient(conn).SyncMaxTS(syncCtx, request)
				// Including RPC request -> RPC processing -> RPC response
				syncMaxTSResp.rtt = time.Since(startTime)
				cancel()
				respCh <- syncMaxTSResp
				if syncMaxTSResp.err != nil {
					log.Error("sync max ts rpc failed, got an error",
						logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
						zap.String("local-allocator-leader-url", leaderConn.Target()),
						errs.ZapError(err))
					return
				}
				if syncMaxTSResp.rpcRes.GetHeader().GetError() != nil {
					log.Error("sync max ts rpc failed, got an error",
						logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
						zap.String("local-allocator-leader-url", leaderConn.Target()),
						errs.ZapError(errors.New(syncMaxTSResp.rpcRes.GetHeader().GetError().String())))
					return
				}
			}(ctx, leaderConn, respCh)
		}
		wg.Wait()
		close(respCh)
		var (
			errList   []error
			syncedDCs []string
			maxTSORtt time.Duration
		)
		// Iterate each response to handle the error and compare MaxTSO.
		for resp := range respCh {
			if resp.err != nil {
				errList = append(errList, resp.err)
			}
			// If any error occurs, just jump out of the loop.
			if len(errList) != 0 {
				break
			}
			if resp.rpcRes == nil {
				return errs.ErrSyncMaxTS.FastGenByArgs("got nil response")
			}
			if skipCheck {
				// Set all the Local TSOs to the maxTSO unconditionally, so the MaxLocalTS in response should be nil.
				if resp.rpcRes.GetMaxLocalTs() != nil {
					return errs.ErrSyncMaxTS.FastGenByArgs("got non-nil max local ts in the second sync phase")
				}
				syncedDCs = append(syncedDCs, resp.rpcRes.GetSyncedDcs()...)
			} else {
				// Compare and get the max one
				if tsoutil.CompareTimestamp(resp.rpcRes.GetMaxLocalTs(), maxTSO) > 0 {
					*maxTSO = *(resp.rpcRes.GetMaxLocalTs())
					if resp.rtt > maxTSORtt {
						maxTSORtt = resp.rtt
					}
				}
				syncedDCs = append(syncedDCs, resp.rpcRes.GetSyncedDcs()...)
			}
		}
		// We need to collect all info needed to ensure the consistency of TSO.
		// So if any error occurs, the synchronization process will fail directly.
		if len(errList) != 0 {
			return errs.ErrSyncMaxTS.FastGenWithCause(errList)
		}
		// Check whether all dc-locations have been considered during the synchronization and retry once if any dc-location missed.
		if ok, unsyncedDCs := gta.checkSyncedDCs(dcLocationMap, syncedDCs); !ok {
			log.Info("unsynced dc-locations found, will retry",
				logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
				zap.Bool("skip-check", skipCheck),
				zap.Strings("synced-DCs", syncedDCs),
				zap.Strings("unsynced-DCs", unsyncedDCs))
			if i < syncMaxRetryCount-1 {
				// maxTSO should remain the same.
				*maxTSO = originalMaxTSO
				// To make sure we have the latest dc-location info
				gta.am.ClusterDCLocationChecker()
				continue
			}
			return errs.ErrSyncMaxTS.FastGenByArgs(
				fmt.Sprintf("unsynced dc-locations found, skip-check: %t, synced dc-locations: %+v, unsynced dc-locations: %+v",
					skipCheck, syncedDCs, unsyncedDCs))
		}
		// Update the sync RTT to help estimate MaxTS later.
		if maxTSORtt != 0 {
			gta.setSyncRTT(maxTSORtt.Milliseconds())
		}
	}
	return nil
}

func (gta *GlobalTSOAllocator) checkSyncedDCs(dcLocationMap map[string]DCLocationInfo, syncedDCs []string) (bool, []string) {
	var unsyncedDCs []string
	for dcLocation := range dcLocationMap {
		if slice.NoneOf(syncedDCs, func(i int) bool { return syncedDCs[i] == dcLocation }) {
			unsyncedDCs = append(unsyncedDCs, dcLocation)
		}
	}
	log.Debug("check unsynced dc-locations",
		logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
		zap.Strings("unsynced-DCs", unsyncedDCs),
		zap.Strings("synced-DCs", syncedDCs))
	return len(unsyncedDCs) == 0, unsyncedDCs
}

func (gta *GlobalTSOAllocator) getCurrentTSO(ctx context.Context) (*pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "GlobalTSOAllocator.getCurrentTSO").End()
	currentPhysical, currentLogical := gta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return &pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.tsoAllocatorRoleGauge.Set(0)
	gta.timestampOracle.ResetTimestamp()
}

// primaryElectionLoop is used to maintain the TSO primary election and TSO's
// running allocator. It is only used in API mode.
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
		expectedPrimary := mcsutils.GetExpectedPrimaryFlag(gta.member.Client(), gta.member.GetLeaderPath())
		// skip campaign the primary if the expected primary is not empty and not equal to the current memberValue.
		// expected primary ONLY SET BY `{service}/primary/transfer` API.
		if len(expectedPrimary) > 0 && !strings.Contains(gta.member.MemberValue(), expectedPrimary) {
			log.Info("skip campaigning of tso primary and check later",
				zap.String("server-name", gta.member.Name()),
				zap.String("expected-primary-id", expectedPrimary),
				zap.Uint64("member-id", gta.member.ID()),
				zap.String("cur-memberValue", gta.member.MemberValue()))
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

	allocator, err := gta.am.GetAllocator(GlobalDCLocation)
	if err != nil {
		log.Error("failed to get the global tso allocator",
			logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
			errs.ZapError(err))
		return
	}
	log.Info("initializing the global tso allocator")
	if err := allocator.Initialize(0); err != nil {
		log.Error("failed to initialize the global tso allocator",
			logutil.CondUint32("keyspace-group-id", gta.getGroupID(), gta.getGroupID() > 0),
			errs.ZapError(err))
		return
	}
	defer func() {
		gta.am.ResetAllocatorGroup(GlobalDCLocation, false)
	}()

	// check expected primary and watch the primary.
	exitPrimary := make(chan struct{})
	lease, err := mcsutils.KeepExpectedPrimaryAlive(ctx, gta.member.Client(), exitPrimary,
		gta.am.leaderLease, gta.member.GetLeaderPath(), gta.member.MemberValue(), constant.TSOServiceName)
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

	// TODO: if enable-local-tso is true, check the cluster dc-location after the primary is elected
	// go gta.tsoAllocatorManager.ClusterDCLocationChecker()
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
