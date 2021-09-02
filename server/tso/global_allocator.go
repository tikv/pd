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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/election"
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
	// SetTSO sets the physical part with given TSO. It's mainly used for BR restore
	// and can not forcibly set the TSO smaller than now.
	SetTSO(tso uint64) error
	// GenerateTSO is used to generate a given number of TSOs.
	// Make sure you have initialized the TSO allocator before calling.
	GenerateTSO(count uint32) (pdpb.Timestamp, error)
	// Reset is used to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalTSOAllocator struct {
	// for global TSO synchronization
	allocatorManager *AllocatorManager
	// leadership is used to check the current PD server's leadership
	// to determine whether a TSO request could be processed.
	leadership      *election.Leadership
	timestampOracle *timestampOracle
	// syncRTT is the RTT duration a SyncMaxTS RPC call will cost,
	// which is used to estimate the MaxTS in a Global TSO generation
	// to reduce the gRPC network IO latency.
	syncRTT atomic.Value // store as int64 milliseconds
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(
	am *AllocatorManager,
	leadership *election.Leadership,
) Allocator {
	gta := &GlobalTSOAllocator{
		allocatorManager: am,
		leadership:       leadership,
		timestampOracle: &timestampOracle{
			client:                 leadership.GetClient(),
			rootPath:               am.rootPath,
			saveInterval:           am.saveInterval,
			updatePhysicalInterval: am.updatePhysicalInterval,
			maxResetTSGap:          am.maxResetTSGap,
			dcLocation:             GlobalDCLocation,
			tsoMux:                 &tsoObject{},
		},
	}
	return gta
}

func (gta *GlobalTSOAllocator) setSyncRTT(rtt int64) {
	gta.syncRTT.Store(rtt)
	tsoGauge.WithLabelValues("global_tso_sync_rtt", gta.timestampOracle.dcLocation).Set(float64(rtt))
}

func (gta *GlobalTSOAllocator) getSyncRTT() int64 {
	syncRTT := gta.syncRTT.Load()
	if syncRTT == nil {
		return 0
	}
	return syncRTT.(int64)
}

func (gta *GlobalTSOAllocator) estimateMaxTS(count uint32, suffixBits int) (*pdpb.Timestamp, bool, error) {
	physical, logical, lastUpdateTime := gta.timestampOracle.generateTSO(int64(count), 0)
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
	tsoAllocatorRole.WithLabelValues(gta.timestampOracle.dcLocation).Set(1)
	// The suffix of a Global TSO should always be 0.
	gta.timestampOracle.suffix = 0
	return gta.timestampOracle.SyncTimestamp(gta.leadership)
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (gta *GlobalTSOAllocator) IsInitialize() bool {
	return gta.timestampOracle.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.timestampOracle.UpdateTimestamp(gta.leadership)
}

// SetTSO sets the physical part with given TSO.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64) error {
	return gta.timestampOracle.resetUserTimestamp(gta.leadership, tso, false)
}

// GenerateTSO is used to generate the given number of TSOs.
// Make sure you have initialized the TSO allocator before calling this method.
// Basically, there are two ways to generate a Global TSO:
//   1. The old way to generate a normal TSO from memory directly, which makes the TSO service node become single point.
//   2. The new way to generate a Global TSO by synchronizing with all other Local TSO Allocators.
// And for the new way, there are two different strategies:
//   1. Collect the max Local TSO from all Local TSO Allocator leaders and write it back to them as MaxTS.
//   2. Estimate a MaxTS and try to write it to all Local TSO Allocator leaders directly to reduce the RTT.
//      During the process, if the estimated MaxTS is not accurate, it will fallback to the collecting way.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	if !gta.leadership.Check() {
		tsoCounter.WithLabelValues("not_leader", gta.timestampOracle.dcLocation).Inc()
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}
	// To check if we have any dc-location configured in the cluster
	dcLocationMap := gta.allocatorManager.GetClusterDCLocations()
	// No dc-locations configured in the cluster, use the normal Global TSO generation way.
	// (without synchronization with other Local TSO Allocators)
	if len(dcLocationMap) == 0 {
		return gta.timestampOracle.getTS(gta.leadership, count, 0)
	}

	// Have dc-locations configured in the cluster, use the Global TSO generation way.
	// (whit synchronization with other Local TSO Allocators)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < maxRetryCount; i++ {
		var (
			err                    error
			shouldRetry, skipCheck bool
			globalTSOResp          pdpb.Timestamp
			estimatedMaxTSO        *pdpb.Timestamp
			suffixBits             = gta.allocatorManager.GetSuffixBits()
		)
		// TODO: add a switch to control whether to enable the MaxTSO estimation.
		// 1. Estimate a MaxTS among all Local TSO Allocator leaders according to the RTT.
		estimatedMaxTSO, shouldRetry, err = gta.estimateMaxTS(count, suffixBits)
		if err != nil {
			log.Error("global tso allocator estimates MaxTS failed", errs.ZapError(err))
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
			log.Error("global tso allocator synchronizes MaxTS failed", errs.ZapError(err))
			continue
		}
		// 3. If skipCheck is false and the maxTSO is bigger than estimatedMaxTSO,
		// we need to redo the setting phase with the bigger one and skip the check safely.
		if !skipCheck && tsoutil.CompareTimestamp(&globalTSOResp, estimatedMaxTSO) > 0 {
			tsoCounter.WithLabelValues("global_tso_sync", gta.timestampOracle.dcLocation).Inc()
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
		// Is skipCheck is false and globalTSOResp remains the same, it means the estimatedTSO is valide.
		if !skipCheck && tsoutil.CompareTimestamp(&globalTSOResp, estimatedMaxTSO) == 0 {
			tsoCounter.WithLabelValues("global_tso_estimate", gta.timestampOracle.dcLocation).Inc()
		}
		// 4. Persist MaxTS into memory, and etcd if needed
		var currentGlobalTSO *pdpb.Timestamp
		if currentGlobalTSO, err = gta.getCurrentTSO(); err != nil {
			log.Error("global tso allocator gets the current global tso in memory failed", errs.ZapError(err))
			continue
		}
		if tsoutil.CompareTimestamp(currentGlobalTSO, &globalTSOResp) < 0 {
			tsoCounter.WithLabelValues("global_tso_persist", gta.timestampOracle.dcLocation).Inc()
			// Update the Global TSO in memory
			if err = gta.timestampOracle.resetUserTimestamp(gta.leadership, tsoutil.GenerateTS(&globalTSOResp), true); err != nil {
				tsoCounter.WithLabelValues("global_tso_persist_err", gta.timestampOracle.dcLocation).Inc()
				log.Error("global tso allocator update the global tso in memory failed", errs.ZapError(err))
				continue
			}
		}
		// 5. Check leadership again before we returning the response.
		if !gta.leadership.Check() {
			tsoCounter.WithLabelValues("not_leader_anymore", gta.timestampOracle.dcLocation).Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd leader anymore")
		}
		// 6. Differentiate the logical part to make the TSO unique globally by giving it a unique suffix in the whole cluster
		globalTSOResp.Logical = gta.timestampOracle.differentiateLogical(globalTSOResp.GetLogical(), suffixBits)
		globalTSOResp.SuffixBits = uint32(suffixBits)
		return globalTSOResp, nil
	}
	tsoCounter.WithLabelValues("exceeded_max_retry", gta.timestampOracle.dcLocation).Inc()
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
	// Check if the logical part will reach the overflow condition after being differenitated.
	if differentiatedLogical := gta.timestampOracle.differentiateLogical(maxTSO.Logical, suffixBits); differentiatedLogical >= maxLogical {
		log.Error("estimated logical part outside of max logical interval, please check ntp time",
			zap.Reflect("max-tso", maxTSO), errs.ZapError(errs.ErrLogicOverflow))
		tsoCounter.WithLabelValues("precheck_logical_overflow", gta.timestampOracle.dcLocation).Inc()
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
// each allocator and remines the same after the synchronization.
// If not, it will be replaced with the new max Local TSO and return.
func (gta *GlobalTSOAllocator) SyncMaxTS(
	ctx context.Context,
	dcLocationMap map[string]DCLocationInfo,
	maxTSO *pdpb.Timestamp,
	skipCheck bool,
) error {
	originalMaxTSO := *maxTSO
	for i := 0; i < syncMaxRetryCount; i++ {
		// Collect all allocator leaders' client URLs
		allocatorLeaders := make(map[string]*pdpb.Member)
		for dcLocation := range dcLocationMap {
			allocator, err := gta.allocatorManager.GetAllocator(dcLocation)
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
				SenderId: gta.allocatorManager.member.ID(),
			},
			SkipCheck: skipCheck,
			MaxTs:     maxTSO,
		}
		for _, leaderURL := range leaderURLs {
			leaderConn, err := gta.allocatorManager.getOrCreateGRPCConn(ctx, leaderURL)
			if err != nil {
				return err
			}
			// Send SyncMaxTSRequest to all allocator leaders concurrently.
			wg.Add(1)
			go func(ctx context.Context, conn *grpc.ClientConn, respCh chan<- *syncResp) {
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
					log.Error("sync max ts rpc failed, got an error", zap.String("local-allocator-leader-url", leaderConn.Target()), errs.ZapError(err))
					return
				}
				if syncMaxTSResp == nil {
					log.Error("sync max ts rpc failed, got a nil response", zap.String("local-allocator-leader-url", leaderConn.Target()))
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
			log.Info("unsynced dc-locations found, will retry", zap.Bool("skip-check", skipCheck), zap.Strings("synced-DCs", syncedDCs), zap.Strings("unsynced-DCs", unsyncedDCs))
			if i < syncMaxRetryCount-1 {
				// maxTSO should remain the same.
				*maxTSO = originalMaxTSO
				// To make sure we have the latest dc-location info
				gta.allocatorManager.ClusterDCLocationChecker()
				continue
			}
			return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Sprintf("unsynced dc-locations found, skip-check: %t, synced dc-locations: %+v, unsynced dc-locations: %+v", skipCheck, syncedDCs, unsyncedDCs))
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
	log.Debug("check unsynced dc-locations", zap.Strings("unsynced-DCs", unsyncedDCs), zap.Strings("synced-DCs", syncedDCs))
	return len(unsyncedDCs) == 0, unsyncedDCs
}

func (gta *GlobalTSOAllocator) getCurrentTSO() (*pdpb.Timestamp, error) {
	currentPhysical, currentLogical := gta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return &pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	tsoAllocatorRole.WithLabelValues(gta.timestampOracle.dcLocation).Set(0)
	gta.timestampOracle.ResetTimestamp()
}
