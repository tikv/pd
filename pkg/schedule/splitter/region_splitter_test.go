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

package splitter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

type mockSplitRegionsHandler struct {
	// regionID -> startKey, endKey
	regions map[uint64][2][]byte
}

func newMockSplitRegionsHandler() *mockSplitRegionsHandler {
	return &mockSplitRegionsHandler{
		regions: map[uint64][2][]byte{},
	}
}

// SplitRegionByKeys mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	m.regions[region.GetID()] = [2][]byte{
		region.GetStartKey(),
		region.GetEndKey(),
	}
	return nil
}

// WatchRegionsByKeyRange mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) ScanRegionsByKeyRange(groupKeys *regionGroupKeys, results *splitKeyResults) {
	splitKeys := groupKeys.keys
	startKey, endKey := groupKeys.region.GetStartKey(), groupKeys.region.GetEndKey()
	for regionID, keyRange := range m.regions {
		if bytes.Equal(startKey, keyRange[0]) && bytes.Equal(endKey, keyRange[1]) {
			regions := make(map[uint64][]byte)
			for i := 0; i < len(splitKeys); i++ {
				regions[regionID+uint64(i)+1000] = splitKeys[i]
			}
			results.addRegionsID(regions)
		}
	}
	groupKeys.finished = true
}

type regionSplitterTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc
}

func TestRegionSplitterTestSuite(t *testing.T) {
	suite.Run(t, new(regionSplitterTestSuite))
}

func (suite *regionSplitterTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
}

func (suite *regionSplitterTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *regionSplitterTestSuite) TestRegionSplitter() {
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(suite.ctx, opt)
	handler := newMockSplitRegionsHandler()
	tc.AddLeaderRegionWithRange(1, "eee", "hhh", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler)
	newRegions := map[uint64]struct{}{}
	// assert success
	failureKeys := splitter.splitRegionsByKeys(suite.ctx, [][]byte{[]byte("fff"), []byte("ggg")}, newRegions)
	suite.Empty(failureKeys)
	suite.Len(newRegions, 2)

	percentage, newRegionsID := splitter.SplitRegions(suite.ctx, [][]byte{[]byte("fff"), []byte("ggg")}, 1)
	suite.Equal(100, percentage)
	suite.Len(newRegionsID, 2)
	// assert out of range
	newRegions = map[uint64]struct{}{}
	failureKeys = splitter.splitRegionsByKeys(suite.ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, newRegions)
	suite.Len(failureKeys, 2)
	suite.Empty(newRegions)

	percentage, newRegionsID = splitter.SplitRegions(suite.ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, 1)
	suite.Equal(0, percentage)
	suite.Empty(newRegionsID)
}

func (suite *regionSplitterTestSuite) TestGroupKeysByRegion() {
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(suite.ctx, opt)
	handler := newMockSplitRegionsHandler()
	tc.AddLeaderRegionWithRange(1, "aaa", "ccc", 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "ccc", "eee", 2, 3, 4)
	tc.AddLeaderRegionWithRange(3, "fff", "ggg", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler)
	groupKeys := splitter.groupKeysByRegion([][]byte{
		[]byte("bbb"),
		[]byte("ddd"),
		[]byte("fff"),
		[]byte("zzz"),
	})
	suite.Len(groupKeys, 2)
	for k, v := range groupKeys {
		switch k {
		case uint64(1):
			suite.Len(v.keys, 1)
			suite.Equal([]byte("bbb"), v.keys[0])
		case uint64(2):
			suite.Len(v.keys, 1)
			suite.Equal([]byte("ddd"), v.keys[0])
		}
	}
}
