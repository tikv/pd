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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
)

// Store is used to simulate tikv.
type Store struct {
	ID                uint64
	Status            metapb.StoreState
	Labels            []*metapb.StoreLabel
	Capacity          uint64
	LeaderWeight      float32
	RegionWeight      float32
	Version           string
	HasExtraUsedSpace bool
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
	Rules   []*placement.Rule
	Labels  typeutil.StringSlice
}

// CaseMap is a mapping of the cases to the their corresponding initialize functions.
var CaseMap = map[string]func(*config.SimConfig) *Case{
	"balance-leader":            newBalanceLeader,
	"redundant-balance-region":  newRedundantBalanceRegion,
	"region-split":              newRegionSplit,
	"region-merge":              newRegionMerge,
	"hot-read":                  newHotRead,
	"hot-write":                 newHotWrite,
	"makeup-down-replicas":      newMakeupDownReplicas,
	"import-data":               newImportData,
	"diagnose-rule1":            newRule1,
	"diagnose-rule2":            newRule2,
	"diagnose-label-not-match1": newLabelNotMatch1,
	"diagnose-label-isolation1": newLabelIsolation1,
	"diagnose-label-isolation2": newLabelIsolation2,
}

// NewCase creates a new case.
func NewCase(name string, simConfig *config.SimConfig) *Case {
	if f, ok := CaseMap[name]; ok {
		return f(simConfig)
	}
	return nil
}

func isUniform(count, meanCount int) bool {
	threshold := 0.05
	maxCount := int((1.0 + threshold) * float64(meanCount))
	minCount := int((1.0 - threshold) * float64(meanCount))
	return minCount <= count && count <= maxCount
}

func getNoEmptyStoreNum(storeNum int, replica int) int {
	noEmptyStoreNum := rand.Intn(storeNum)
	if noEmptyStoreNum < replica {
		return replica
	}
	if noEmptyStoreNum == storeNum {
		return storeNum - 1
	}
	return noEmptyStoreNum
}
