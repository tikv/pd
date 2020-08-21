// Copyright 2017 TiKV Project Authors.
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

package cases

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

// Store is used to simulate tikv.
type Store struct {
	ID           uint64
	Status       metapb.StoreState
	Labels       []*metapb.StoreLabel
	Capacity     uint64
	Available    uint64
	LeaderWeight float32
	RegionWeight float32
	Version      string
}

// Region is used to simulate a region.
type Region struct {
	ID     uint64
	Peers  []*metapb.Peer
	Leader *metapb.Peer
	Size   int64
	Keys   int64
}

// CheckerFunc checks if the scheduler is finished.
type CheckerFunc func(*core.RegionsInfo, []info.StoreStats) bool

// Case represents a test suite for simulator.
type Case struct {
	Stores          []*Store
	Regions         []Region
	RegionSplitSize int64
	RegionSplitKeys int64
	Events          []EventDescriptor
	TableNumber     int

	Checker CheckerFunc // To check the schedule is finished.
}

// unit of storage
const (
	B = 1 << (iota * 10)
	KB
	MB
	GB
	TB
)

// IDAllocator is used to alloc unique ID.
type idAllocator struct {
	id uint64
}

// nextID gets the next unique ID.
func (a *idAllocator) nextID() uint64 {
	a.id++
	return a.id
}

// ResetID resets the IDAllocator.
func (a *idAllocator) ResetID() {
	a.id = 0
}

// GetID gets the current ID.
func (a *idAllocator) GetID() uint64 {
	return a.id
}

// IDAllocator is used to alloc unique ID.
var IDAllocator idAllocator

// CaseMap is a mapping of the cases to the their corresponding initialize functions.
var CaseMap = map[string]func() *Case{
	"balance-leader":           newBalanceLeader,
	"redundant-balance-region": newRedundantBalanceRegion,
	"add-nodes":                newAddNodes,
	"add-nodes-dynamic":        newAddNodesDynamic,
	"delete-nodes":             newDeleteNodes,
	"region-split":             newRegionSplit,
	"region-merge":             newRegionMerge,
	"hot-read":                 newHotRead,
	"hot-write":                newHotWrite,
	"makeup-down-replicas":     newMakeupDownReplicas,
	"import-data":              newImportData,
}

// NewCase creates a new case.
func NewCase(name string) *Case {
	if f, ok := CaseMap[name]; ok {
		return f()
	}
	return nil
}

func leaderAndRegionIsUniform(leaderCount, regionCount, regionNum int, threshold float64) bool {
	return isUniform(leaderCount, regionNum/3, threshold) && isUniform(regionCount, regionNum, threshold)
}

func isUniform(count, meanCount int, threshold float64) bool {
	maxCount := int((1.0 + threshold) * float64(meanCount))
	minCount := int((1.0 - threshold) * float64(meanCount))
	return minCount <= count && count <= maxCount
}

func getStoreNum() int {
	storeNum := simutil.CaseConfigure.StoreNum
	if storeNum < 3 {
		simutil.Logger.Fatal("Store num should be larger than or equal to 3.")
	}
	return storeNum
}

func getRegionNum() int {
	regionNum := simutil.CaseConfigure.RegionNum
	if regionNum <= 0 {
		simutil.Logger.Fatal("Region num should be larger than 0.")
	}
	return regionNum
}

func getNoEmptyStoreNum(storeNum int, noEmptyRatio float64) uint64 {
	noEmptyStoreNum := uint64(float64(storeNum) * noEmptyRatio)
	if noEmptyStoreNum < 3 || noEmptyStoreNum == uint64(storeNum) {
		noEmptyStoreNum = 3
	}
	return noEmptyStoreNum
}
