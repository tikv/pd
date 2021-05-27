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
	"fmt"
	"sync"
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

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	if !gta.leadership.Check() {
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested pd %s of cluster", errs.NotLeaderErr))
	}
	// To check if we have any dc-location configured in the cluster
	dcLocationMap := gta.allocatorManager.GetClusterDCLocations()
	// No dc-locations configured in the cluster, use the normal TSO generation way.
	if len(dcLocationMap) == 0 {
		return gta.timestampOracle.getTS(gta.leadership, count, 0)
	}

	// Have dc-locations configured in the cluster, use the Global TSO generation way.
	// Send maxTS to all Local TSO Allocator leaders to prewrite
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxTSO := &pdpb.Timestamp{}
	// 1. Collect the MaxTS with all Local TSO Allocator leaders first
	if err := gta.SyncMaxTS(ctx, dcLocationMap, maxTSO); err != nil {
		return pdpb.Timestamp{}, err
	}
	// 2. Add the count and make sure its logical part won't overflow after being differentiated.
	suffixBits := gta.allocatorManager.GetSuffixBits()
	gta.preprocessLogical(maxTSO, count, suffixBits)
	// 3. Sync the MaxTS with all Local TSO Allocator leaders then
	if err := gta.SyncMaxTS(ctx, dcLocationMap, maxTSO); err != nil {
		return pdpb.Timestamp{}, err
	}
	// 4. Persist MaxTS into memory, and etcd if needed
	var (
		currentGlobalTSO pdpb.Timestamp
		err              error
	)
	if currentGlobalTSO, err = gta.getCurrentTSO(); err != nil {
		return pdpb.Timestamp{}, err
	}
	if tsoutil.CompareTimestamp(&currentGlobalTSO, maxTSO) < 0 {
		// Update the Global TSO in memory
		if err := gta.timestampOracle.resetUserTimestamp(gta.leadership, tsoutil.GenerateTS(maxTSO), true); err != nil {
			log.Warn("update the global tso in memory failed", errs.ZapError(err))
		}
	}
	// 5.Differentiate the logical part to make the TSO unique globally by giving it a unique suffix in the whole cluster
	maxTSO.Logical = gta.timestampOracle.differentiateLogical(maxTSO.Logical, suffixBits)
	return *maxTSO, nil
}

func (gta *GlobalTSOAllocator) preprocessLogical(maxTSO *pdpb.Timestamp, count uint32, suffixBits int) {
	failpoint.Inject("globalTSOOverflow", func() {
		maxTSO.Logical = maxLogical
	})
	maxTSO.Logical += int64(count)
	// Check if the logical part will reach the overflow condition after being differenitated.
	if differentiatedLogical := gta.timestampOracle.differentiateLogical(maxTSO.Logical, suffixBits); differentiatedLogical >= maxLogical {
		maxTSO.Physical += updateTimestampGuard.Milliseconds()
		// Equivalent to setting the logical part to zero and adding the given count.
		maxTSO.Logical = int64(count)
	}
	maxTSO.SuffixBits = uint32(suffixBits)
}

const (
	dialTimeout = 3 * time.Second
	rpcTimeout  = 3 * time.Second
)

// SyncMaxTS is used to sync the MaxTS with the Local TSO Allocator leaders in the dcLocationMap. If the maxTSO is empty, it will collect
// the max Local TSO and load it into maxTSO. If the maxTSO is not empty, it will set in-memory-TSO of the Local TSO Allocator leaders to
// maxTSO if maxTSO is greater.
func (gta *GlobalTSOAllocator) SyncMaxTS(ctx context.Context, dcLocationMap map[string]DCLocationInfo, maxTSO *pdpb.Timestamp) error {
	maxRetryCount := 1
	for i := 0; i < maxRetryCount; i++ {
		// Collect all allocator leaders' client URLs
		allocatorLeaders := make(map[string]*pdpb.Member)
		for dcLocation := range dcLocationMap {
			allocator, err := gta.allocatorManager.GetAllocator(dcLocation)
			if err != nil {
				return err
			}
			allocatorLeader := allocator.(*LocalTSOAllocator).GetAllocatorLeader()
			if allocatorLeader.GetMemberId() == 0 {
				return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Printf("%s does not have the local allocator leader yet", dcLocation))
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
		respCh := make(chan *pdpb.SyncMaxTSResponse, len(leaderURLs))
		errCh := make(chan error, len(leaderURLs))
		var errList []error
		wg := sync.WaitGroup{}
		for _, leaderURL := range leaderURLs {
			leaderConn, err := gta.allocatorManager.getOrCreateGRPCConn(ctx, leaderURL)
			if err != nil {
				return err
			}
			wg.Add(1)
			go func(ctx context.Context, conn *grpc.ClientConn, respCh chan<- *pdpb.SyncMaxTSResponse, errCh chan<- error) {
				defer wg.Done()
				request := &pdpb.SyncMaxTSRequest{
					Header: &pdpb.RequestHeader{
						SenderId: gta.allocatorManager.member.ID(),
					},
				}
				if maxTSO.GetPhysical() != 0 {
					request.MaxTs = maxTSO
				}
				syncCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
				resp, err := pdpb.NewPDClient(conn).SyncMaxTS(syncCtx, request)
				cancel()
				if err != nil {
					errCh <- err
					log.Error("sync max ts rpc failed, got an error", zap.String("local-allocator-leader-url", leaderConn.Target()), zap.Error(err))
				}
				respCh <- resp
				if resp == nil {
					log.Error("sync max ts rpc failed, got a nil response", zap.String("local-allocator-leader-url", leaderConn.Target()))
				}
			}(ctx, leaderConn, respCh, errCh)
		}
		wg.Wait()
		close(respCh)
		close(errCh)
		// If any error occurs, the synchronization process will fail
		if err := <-errCh; err != nil {
			errList = append(errList, err)
		}
		if len(errList) > 0 {
			return errs.ErrSyncMaxTS.FastGenWithCause(errList)
		}
		var (
			respCount         int
			syncedDCs         []string
			inCollectingPhase bool
		)
		for resp := range respCh {
			respCount++
			if resp == nil {
				return errs.ErrSyncMaxTS.FastGenByArgs("got nil response")
			}
			// Once we get a non-nil and non-zero MaxLocalTs first, we will think it's in the first phase
			// of the Global TSO synchronization. So that we can have more detailed processing logic
			// for each phase. For example, if we think we're in the first phase of the Global TSO
			// synchronization, the inCollectingPhase will be set to true, and during this phase,
			// any response with nil or empty MaxLocalTs will be regarded as an invalid response.
			// Then the whole synchronization will fail.
			if respCount == 1 && resp.GetMaxLocalTs() != nil && resp.GetMaxLocalTs().GetPhysical() != 0 {
				inCollectingPhase = true
			}
			if inCollectingPhase {
				// Handle the response of the first phase: collect all the Local TSOs
				if resp.GetMaxLocalTs() == nil || resp.GetMaxLocalTs().GetPhysical() == 0 {
					return errs.ErrSyncMaxTS.FastGenByArgs("got nil or zero max local ts in the first sync phase")
				}
				// Compare and get the max one
				if tsoutil.CompareTimestamp(resp.GetMaxLocalTs(), maxTSO) > 0 {
					*maxTSO = *(resp.GetMaxLocalTs())
				}
				syncedDCs = append(syncedDCs, resp.GetDcs()...)
			} else {
				// Handle the response of the second phase: set all the Local TSOs to the maxTSO
				if resp.GetMaxLocalTs() != nil {
					return errs.ErrSyncMaxTS.FastGenByArgs("got non-nil max local ts in the second sync phase")
				}
				syncedDCs = append(syncedDCs, resp.GetDcs()...)
			}
		}
		if ok, unsyncedDCs := gta.checkSyncedDCs(dcLocationMap, syncedDCs); !ok {
			// Only retry one time when synchronization is incomplete
			if maxRetryCount == 1 {
				log.Warn("unsynced dc-locations found, will retry", zap.Strings("synced-DCs", syncedDCs), zap.Strings("unsynced-DCs", unsyncedDCs))
				maxRetryCount++
				// To make sure we have the latest dc-location info
				gta.allocatorManager.ClusterDCLocationChecker()
				continue
			}
			return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Sprintf("unsynced dc-locations found, synced dc-locations: %+v, unsynced dc-locations: %+v", syncedDCs, unsyncedDCs))
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

func (gta *GlobalTSOAllocator) getCurrentTSO() (pdpb.Timestamp, error) {
	currentPhysical, currentLogical := gta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return *tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	tsoAllocatorRole.WithLabelValues(gta.timestampOracle.dcLocation).Set(0)
	gta.timestampOracle.ResetTimestamp()
}
