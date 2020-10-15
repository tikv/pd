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

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
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
	Initialize() error
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
	// leadership is used to check the current PD server's leadership
	// to determine whether a TSO request could be processed.
	leadership      *election.Leadership
	timestampOracle *timestampOracle
	// for global TSO synchronization
	allocatorManager *AllocatorManager
	// for gRPC use
	localAllocatorConn struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
	}
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(
	am *AllocatorManager,
	leadership *election.Leadership,
	rootPath string,
	saveInterval time.Duration,
	updatePhysicalInterval time.Duration,
	maxResetTSGap func() time.Duration,
) Allocator {
	gta := &GlobalTSOAllocator{
		leadership: leadership,
		timestampOracle: &timestampOracle{
			client:                 leadership.GetClient(),
			rootPath:               rootPath,
			saveInterval:           saveInterval,
			updatePhysicalInterval: updatePhysicalInterval,
			maxResetTSGap:          maxResetTSGap,
		},
		allocatorManager: am,
	}
	gta.localAllocatorConn.clientConns = make(map[string]*grpc.ClientConn)
	return gta
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize() error {
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
	return gta.timestampOracle.ResetUserTimestamp(gta.leadership, tso)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	// To check if we have any dc-location configured in the cluster
	dcLocationMap, err := gta.allocatorManager.GetClusterDCLocations()
	if err != nil {
		log.Error("get cluster dc-locations info failed", errs.ZapError(err))
		return pdpb.Timestamp{}, err
	}
	// No dc-locations configured in the cluster
	if len(dcLocationMap) == 0 {
		return gta.timestampOracle.getTS(gta.leadership, count)
	}
	// Send maxTS to all Local TSO Allocator leaders to prewrite
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxTSO := &pdpb.Timestamp{}
	// Collect the MaxTS with all Local TSO Allocator leaders first
	if err = gta.syncMaxTS(ctx, dcLocationMap, maxTSO); err != nil {
		return pdpb.Timestamp{}, err
	}
	maxTSO.Logical += int64(count)
	// Sync the MaxTS with all Local TSO Allocator leaders then
	if err := gta.syncMaxTS(ctx, dcLocationMap, maxTSO); err != nil {
		return pdpb.Timestamp{}, err
	}
	var currentGlobalTSO pdpb.Timestamp
	if currentGlobalTSO, err = gta.getCurrentTSO(); err != nil {
		return pdpb.Timestamp{}, err
	}
	if tsoutil.CompareTimestamp(&currentGlobalTSO, maxTSO) < 0 {
		// Update the global TSO in memory
		if err := gta.SetTSO(tsoutil.GenerateTS(maxTSO)); err != nil {
			return pdpb.Timestamp{}, err
		}
	}
	return *maxTSO, nil
}

const (
	dialTimeout = 3 * time.Second
	rpcTimeout  = 3 * time.Second
)

func (gta *GlobalTSOAllocator) syncMaxTS(ctx context.Context, dcLocationMap map[string][]uint64, maxTSO *pdpb.Timestamp) error {
	maxRetryCount := 1
	for i := 0; i < maxRetryCount; i++ {
		// Collect all allocator leaders' client URLs
		allocatorLeaders, err := gta.allocatorManager.GetLocalAllocatorLeaders()
		if err != nil {
			return err
		}
		leaderURLs := make([]string, 0, len(allocatorLeaders))
		for _, allocator := range allocatorLeaders {
			// Check if its client URLs are empty
			if len(allocator.GetMember().GetClientUrls()) < 1 {
				continue
			}
			leaderURL := allocator.GetMember().GetClientUrls()[0]
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
			leaderConn, err := gta.getOrCreateGRPCConn(ctx, leaderURL)
			if err != nil {
				return err
			}
			wg.Add(1)
			go func(ctx context.Context, conn *grpc.ClientConn, respCh chan<- *pdpb.SyncMaxTSResponse, errCh chan<- error) {
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
				wg.Done()
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
		var syncedDCs []string
		for resp := range respCh {
			if resp == nil {
				return errs.ErrSyncMaxTS.FastGenWithCause("got nil response")
			}
			syncedDCs = append(syncedDCs, resp.GetDcs()...)
			// Compare and get the max one
			if resp.GetMaxLocalTs() != nil && resp.GetMaxLocalTs().GetPhysical() != 0 {
				if tsoutil.CompareTimestamp(resp.GetMaxLocalTs(), maxTSO) > 0 {
					*maxTSO = *(resp.GetMaxLocalTs())
				}
			}
		}
		if !gta.checkSyncedDCs(dcLocationMap, syncedDCs) {
			// Only retry one time when synchronization is incomplete
			if maxRetryCount == 1 {
				log.Warn("unsynced dc-locations found, will retry", zap.Strings("syncedDCs", syncedDCs))
				maxRetryCount++
				continue
			}
			return errs.ErrSyncMaxTS.FastGenWithCause(fmt.Sprintf("unsynced dc-locations found, synced dc-locations: %+v", syncedDCs))
		}
	}
	return nil
}

func (gta *GlobalTSOAllocator) checkSyncedDCs(dcLocationMap map[string][]uint64, syncedDCs []string) bool {
	unsyncedDCs := make([]string, 0)
	for dcLocation := range dcLocationMap {
		if slice.NoneOf(syncedDCs, func(i int) bool { return syncedDCs[i] == dcLocation }) {
			unsyncedDCs = append(unsyncedDCs, dcLocation)
		}
	}
	log.Info("check unsynced dc-locations", zap.Strings("unsyncedDCs", unsyncedDCs), zap.Strings("syncedDCs", syncedDCs))
	return len(unsyncedDCs) == 0
}

func (gta *GlobalTSOAllocator) getOrCreateGRPCConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	gta.localAllocatorConn.RLock()
	conn, ok := gta.localAllocatorConn.clientConns[addr]
	gta.localAllocatorConn.RUnlock()
	if ok {
		return conn, nil
	}
	tlsCfg, err := gta.allocatorManager.securityConfig.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cc, err := grpcutil.GetClientConn(ctxWithTimeout, addr, tlsCfg)
	if err != nil {
		return nil, err
	}
	gta.localAllocatorConn.Lock()
	defer gta.localAllocatorConn.Unlock()
	if old, ok := gta.localAllocatorConn.clientConns[addr]; ok {
		cc.Close()
		log.Debug("use old connection", zap.String("target", cc.Target()), zap.String("state", cc.GetState().String()))
		return old, nil
	}
	gta.localAllocatorConn.clientConns[addr] = cc
	return cc, nil
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
	gta.timestampOracle.ResetTimestamp()
}
