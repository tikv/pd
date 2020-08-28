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
func NewAllocatorManager(serverCtx context.Context, etcd *embed.Etcd, client *clientv3.Client, rootPath string, saveInterval time.Duration, maxResetTSGap func() time.Duration) *AllocatorManager {
	allocatorManager := &AllocatorManager{
		allocatorGroups: make(map[string]*allocatorGroup),
		etcd:            etcd,
		client:          client,
		rootPath:        path.Join(rootPath, "allocator"),
		saveInterval:    saveInterval,
		maxResetTSGap:   maxResetTSGap,
	}
	return allocatorManager
}

func (am *AllocatorManager) getAllocatorPath(dcLocation string) string {
	return path.Join(am.rootPath, dcLocation)
}

// SetUpAllocator is used to set up an allocator, which will initialize the allocator and put it into allocator daemon.
func (am *AllocatorManager) SetUpAllocator(parentCtx context.Context, parentCancel context.CancelFunc, dcLocation string, leadership *election.Leadership) error {
	am.Lock()
	defer am.Unlock()
	switch dcLocation {
	case "global":
		am.allocatorGroups[dcLocation] = &allocatorGroup{
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

// AllocatorDaemon is used to update every allocator's TSO.
func (am *AllocatorManager) AllocatorDaemon(serverCtx context.Context) {
	tsTicker := time.NewTicker(UpdateTimestampStep)
	defer tsTicker.Stop()

	for {
		select {
		case <-tsTicker.C:
			// Collect all dc-locations first
			am.RLock()
			dcLocations := make([]string, 0, len(am.allocatorGroups))
			for dcLocation := range am.allocatorGroups {
				dcLocations = append(dcLocations, dcLocation)
			}
			am.RUnlock()
			// Update each allocator concurrently
			for _, dcLocation := range dcLocations {
				am.wg.Add(1)
				go am.updateAllocator(dcLocation)
			}
			am.wg.Wait()
		case <-serverCtx.Done():
			return
		}
	}
}

func (am *AllocatorManager) updateAllocator(dcLocation string) {
	defer am.wg.Done()

	am.Lock()
	defer am.Unlock()
	allocatorGroup := am.allocatorGroups[dcLocation]
	select {
	case <-allocatorGroup.parentCtx.Done():
		// Need to initialize first before next use
		am.allocatorGroups[dcLocation].isInitialized = false
		// Resetting the allocator will clear TSO in memory
		allocatorGroup.allocator.Reset()
		return
	default:
	}
	if !allocatorGroup.isInitialized {
		log.Info("allocator has not been initialized yet", zap.String("dc-location", dcLocation))
		return
	}
	if !allocatorGroup.leadership.Check() {
		return
	}
	if err := allocatorGroup.allocator.UpdateTSO(); err != nil {
		log.Warn("failed to update allocator's timestamp", zap.String("dc-location", dcLocation), zap.Error(err))
		allocatorGroup.parentCancel()
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
