// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo
	RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo

	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
	GetRegion(id uint64) *core.RegionInfo
	GetRegionStores(region *core.RegionInfo) []*core.StoreInfo
	GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo
	GetLeaderStore(region *core.RegionInfo) *core.StoreInfo
	GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo)
	ScanRegions(startKey []byte, limit int) []*core.RegionInfo

	BlockStore(id uint64) error
	UnblockStore(id uint64)

	IsRegionHot(id uint64) bool
	RegionWriteStats() []*core.RegionStat
	RegionReadStats() []*core.RegionStat
	RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo

	// get config methods
	GetOpt() NamespaceOptions
	Options

	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster Cluster) error
	Cleanup(cluster Cluster)
	Schedule(cluster Cluster, opInfluence OpInfluence) []*Operator
	IsScheduleAllowed(cluster Cluster) bool
}

// CreateSchedulerFunc is for creating scheudler.
type CreateSchedulerFunc func(limiter *Limiter, args []string) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(name string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[name]; ok {
		log.Fatalf("duplicated scheduler name: %v", name)
	}
	schedulerMap[name] = createFn
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(name string, limiter *Limiter, args ...string) (Scheduler, error) {
	fn, ok := schedulerMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(limiter, args)
}

// Limiter a counter that limits the number of operators
type Limiter struct {
	sync.RWMutex
	counts map[OperatorKind]map[uint64]*cache.TTLUint64
}

// NewLimiter create a schedule limiter
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[OperatorKind]map[uint64]*cache.TTLUint64),
	}
}

const (
	limiterCacheGCInterval = time.Second * 5
	limiterCacheTTL        = time.Minute * 1
)

// UpdateCounts updates resouce counts using current pending operators.
func (l *Limiter) UpdateCounts(op *Operator, region *core.RegionInfo) {
	l.Lock()
	defer l.Unlock()

	for _, store := range op.InvolvedStores(region) {
		ttl, ok := l.counts[op.Kind()][store]
		if !ok {
			ttl = cache.NewIDTTL(limiterCacheGCInterval, limiterCacheTTL)
			l.counts[op.Kind()][store] = ttl
		}
		ttl.Put(op.RegionID())
	}
}

// Remove deletes related items from all involved stores cache.
func (l *Limiter) Remove(op *Operator, region *core.RegionInfo) {
	l.Lock()
	defer l.Unlock()

	for _, store := range op.InvolvedStores(region) {
		ttl, ok := l.counts[op.Kind()][store]
		if ok {
			ttl.Remove(op.RegionID())
		}
	}
}

// OperatorCount gets the max count of operators of all involved stores filtered by mask.
func (l *Limiter) OperatorCount(mask OperatorKind) uint64 {
	l.RLock()
	defer l.RUnlock()

	var max uint64 = 1
	for k, stores := range l.counts {
		if k&mask != 0 {
			for _, store := range stores {
				if max < uint64(store.Len()) {
					max = uint64(store.Len())
				}
			}
		}
	}
	return max
}

// StoreOperatorCount gets the count of operators for specific store filtered by mask.
func (l *Limiter) StoreOperatorCount(mask OperatorKind, storeID uint64) uint64 {
	l.RLock()
	defer l.RUnlock()

	var total uint64
	for k, stores := range l.counts {
		if k&mask != 0 {
			total += uint64(stores[storeID].Len())
		}
	}
	return total
}
