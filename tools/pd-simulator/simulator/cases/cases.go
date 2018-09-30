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

package cases

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
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
type CheckerFunc func(*core.RegionsInfo) bool

// Case represents a test suite for simulator.
type Case struct {
	Stores          []*Store
	Regions         []Region
	MaxID           uint64
	RegionSplitSize int64
	RegionSplitKeys int64
	Events          []EventInner

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

type idAllocator struct {
	maxID uint64
}

func (a *idAllocator) nextID() uint64 {
	a.maxID++
	return a.maxID
}

// CaseMap is a mapping of the cases to the their corresponding initialize functions.
var CaseMap = map[string]func() *Case{
	"balance-leader":       newBalanceLeader,
	"add-nodes":            newAddNodes,
	"add-nodes-dynamic":    newAddNodesDynamic,
	"delete-nodes":         newDeleteNodes,
	"region-split":         newRegionSplit,
	"region-merge":         newRegionMerge,
	"hot-read":             newHotRead,
	"hot-write":            newHotWrite,
	"makeup-down-replicas": newMakeupDownReplicas,
}

// NewCase creates a new case.
func NewCase(name string) *Case {
	if f, ok := CaseMap[name]; ok {
		return f()
	}
	return nil
}
