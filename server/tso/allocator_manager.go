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
	"path"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/election"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

type allocatorGroup struct {
	dcLocation string
	// allocator's parent ctx and cancel function, which is to
	// control the allocator's behavior in AllocatorDaemon and
	// pass the cancel signal to its parent as soon as possible
	// since it is critical to let parent goroutine know whether
	// the allocator is still able to work well.
	parentCtx    context.Context
	parentCancel context.CancelFunc
	// For the Global TSO Allocator, leadership is a PD leader's
	// leadership, and for the Local TSO Allocator, leadership
	// is a DC-level certificate to allow an allocator to generate
	// TSO for local transactions in its DC.
	leadership *election.Leadership
	allocator  Allocator
	// the flag indicates whether this allocator is initialized
	isInitialized bool
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	sync.RWMutex
	wg sync.WaitGroup
	// There are two kinds of TSO Allocators:
	//   1. Global TSO Allocator, as a global single point to allocate
	//      TSO for global transactions, such as cross-region cases.
	//   2. Local TSO Allocator, servers for DC-level transactions.
	// dc-location/global (string) -> TSO Allocator
	allocatorGroups map[string]*allocatorGroup
	// etcd and its client
	etcd   *embed.Etcd
	client *clientv3.Client
	// tso config
	rootPath      string
	saveInterval  time.Duration
	maxResetTSGap func() time.Duration
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(etcd *embed.Etcd, client *clientv3.Client, rootPath string, saveInterval time.Duration, maxResetTSGap func() time.Duration) *AllocatorManager {
	allocatorManager := &AllocatorManager{
		allocatorGroups: make(map[string]*allocatorGroup),
		etcd:            etcd,
		client:          client,
		rootPath:        rootPath,
		saveInterval:    saveInterval,
		maxResetTSGap:   maxResetTSGap,
	}
	return allocatorManager
}

func (am *AllocatorManager) getAllocatorPath(dcLocation string) string {
	// For backward compatibility, the global timestamp's store path will still use the old one
	if dcLocation == "global" {
		return am.rootPath
	}
	return path.Join(am.rootPath, dcLocation)
}

// SetUpAllocator is used to set up an allocator, which will initialize the allocator and put it into allocator daemon.
func (am *AllocatorManager) SetUpAllocator(parentCtx context.Context, parentCancel context.CancelFunc, dcLocation string, leadership *election.Leadership) error {
	am.Lock()
	defer am.Unlock()
	switch dcLocation {
	case "global":
		am.allocatorGroups[dcLocation] = &allocatorGroup{
			dcLocation:   dcLocation,
			parentCtx:    parentCtx,
			parentCancel: parentCancel,
			leadership:   leadership,
			allocator:    NewGlobalTSOAllocator(leadership, am.getAllocatorPath(dcLocation), am.saveInterval, am.maxResetTSGap),
		}
		if err := am.allocatorGroups[dcLocation].allocator.Initialize(); err != nil {
			return errs.ErrSetAllocator.FastGenByArgs(err)
		}
		am.allocatorGroups[dcLocation].isInitialized = true
	default:
		// Todo: set up a Local TSO Allocator
	}
	return nil
}

// GetAllocator get the allocator by dc-location.
func (am *AllocatorManager) GetAllocator(dcLocation string) (Allocator, error) {
	am.RLock()
	defer am.RUnlock()
	allocatorGroup, exist := am.allocatorGroups[dcLocation]
	if !exist {
		return nil, errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found", dcLocation))
	}
	return allocatorGroup.allocator, nil
}

func (am *AllocatorManager) getAllocatorGroups() []*allocatorGroup {
	am.RLock()
	allocatorGroups := make([]*allocatorGroup, 0, len(am.allocatorGroups))
	for _, ag := range am.allocatorGroups {
		allocatorGroups = append(allocatorGroups, ag)
	}
	am.RUnlock()
	return allocatorGroups
}

// AllocatorDaemon is used to update every allocator's TSO.
func (am *AllocatorManager) AllocatorDaemon(serverCtx context.Context) {
	tsTicker := time.NewTicker(UpdateTimestampStep)
	defer tsTicker.Stop()

	for {
		select {
		case <-tsTicker.C:
			// Collect all dc-locations first
			allocatorGroups := am.getAllocatorGroups()
			// Update each allocator concurrently
			for _, ag := range allocatorGroups {
				am.wg.Add(1)
				go am.updateAllocator(ag)
			}
			am.wg.Wait()
		case <-serverCtx.Done():
			return
		}
	}
}

// updateAllocator is used to update the allocator in the group
func (am *AllocatorManager) updateAllocator(ag *allocatorGroup) {
	defer am.wg.Done()
	select {
	case <-ag.parentCtx.Done():
		// Need to initialize first before next use
		ag.isInitialized = false
		// Resetting the allocator will clear TSO in memory
		ag.allocator.Reset()
		return
	default:
	}
	if !ag.isInitialized {
		log.Info("allocator has not been initialized yet", zap.String("dc-location", ag.dcLocation))
		return
	}
	if !ag.leadership.Check() {
		return
	}
	if err := ag.allocator.UpdateTSO(); err != nil {
		log.Warn("failed to update allocator's timestamp", zap.String("dc-location", ag.dcLocation), zap.Error(err))
		ag.parentCancel()
		return
	}
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleTSORequest(dcLocation string, count uint32) (pdpb.Timestamp, error) {
	am.RLock()
	defer am.RUnlock()
	allocatorGroup, exist := am.allocatorGroups[dcLocation]
	if !exist {
		err := errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found, can not get timestamp", dcLocation))
		return pdpb.Timestamp{}, err
	}
	return allocatorGroup.allocator.GenerateTSO(count)
}
