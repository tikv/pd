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
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

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
func (m *mockSplitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, _ [][]byte) error {
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
			for i := range splitKeys {
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

func (suite *regionSplitterTestSuite) TearDownSuite() {
	suite.cancel()
}

func (suite *regionSplitterTestSuite) TestRegionSplitter() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(suite.ctx, opt)
	handler := newMockSplitRegionsHandler()
	// Create stores before creating regions
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderStore(3, 1)
	tc.AddLeaderStore(4, 1)
	tc.AddLeaderRegionWithRange(1, "eee", "hhh", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
	newRegions := map[uint64]struct{}{}
	// assert success
	failureKeys := splitter.splitRegionsByKeys(suite.ctx, [][]byte{[]byte("fff"), []byte("ggg")}, newRegions)
	re.Empty(failureKeys)
	re.Len(newRegions, 2)

	percentage, newRegionsID := splitter.SplitRegions(suite.ctx, [][]byte{[]byte("fff"), []byte("ggg")}, 1)
	re.Equal(100, percentage)
	re.Len(newRegionsID, 2)
	// assert out of range
	newRegions = map[uint64]struct{}{}
	failureKeys = splitter.splitRegionsByKeys(suite.ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, newRegions)
	re.Len(failureKeys, 2)
	re.Empty(newRegions)

	percentage, newRegionsID = splitter.SplitRegions(suite.ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, 1)
	re.Equal(0, percentage)
	re.Empty(newRegionsID)
}

func (suite *regionSplitterTestSuite) TestGroupKeysByRegion() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(suite.ctx, opt)
	handler := newMockSplitRegionsHandler()
	// Create stores before creating regions
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderStore(3, 1)
	tc.AddLeaderStore(4, 1)
	tc.AddLeaderRegionWithRange(1, "aaa", "ccc", 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "ccc", "eee", 2, 3, 4)
	tc.AddLeaderRegionWithRange(3, "fff", "ggg", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
	keys := [][]byte{
		[]byte("bbb"),
		[]byte("ddd"),
		[]byte("fff"),
		[]byte("zzz"),
	}
	groupKeys, pendingKeys := splitter.groupKeysByRegion(keys)
	re.Equal([][]byte{[]byte("bbb"), []byte("ddd"), []byte("zzz")}, pendingKeys)
	re.Len(groupKeys, 2)
	for k, v := range groupKeys {
		switch k {
		case uint64(1):
			re.Len(v.keys, 1)
			re.Equal([]byte("bbb"), v.keys[0])
		case uint64(2):
			re.Len(v.keys, 1)
			re.Equal([]byte("ddd"), v.keys[0])
		}
	}
}

func (suite *regionSplitterTestSuite) TestExistingBoundaries() {
	for _, test := range []struct {
		name       string
		keys       [][]byte
		percentage int
		newIDs     []uint64
		lookups    map[string]int
		leaderless bool
	}{
		{
			name:       "existing only",
			keys:       [][]byte{[]byte("eee"), []byte("hhh")},
			percentage: 100,
			lookups:    map[string]int{"eee": 1, "hhh": 1},
		},
		{
			name:       "single existing boundary",
			keys:       [][]byte{[]byte("eee")},
			percentage: 100,
			lookups:    map[string]int{"eee": 1},
		},
		{
			name:       "duplicate existing boundaries",
			keys:       [][]byte{[]byte("eee"), []byte("eee"), []byte("hhh")},
			percentage: 100,
			lookups:    map[string]int{"eee": 2, "hhh": 1},
		},
		{
			name:       "existing boundary without leader",
			keys:       [][]byte{[]byte("eee")},
			percentage: 100,
			lookups:    map[string]int{"eee": 1},
			leaderless: true,
		},
		{
			name:       "existing and new",
			keys:       [][]byte{[]byte("eee"), []byte("fff")},
			percentage: 100,
			newIDs:     []uint64{1001},
			lookups:    map[string]int{"eee": 1, "fff": 1},
		},
		{
			name:       "existing between new keys",
			keys:       [][]byte{[]byte("fff"), []byte("eee"), []byte("ggg")},
			percentage: 100,
			newIDs:     []uint64{1001, 1002},
			lookups:    map[string]int{"eee": 1, "fff": 1, "ggg": 1},
		},
		{
			name:       "new only",
			keys:       [][]byte{[]byte("fff"), []byte("ggg")},
			percentage: 100,
			newIDs:     []uint64{1001, 1002},
			lookups:    map[string]int{"fff": 1, "ggg": 1},
		},
		{
			name:       "existing and missing region",
			keys:       [][]byte{[]byte("eee"), []byte("aaa")},
			percentage: 50,
			lookups:    map[string]int{"eee": 1, "aaa": 2},
		},
		{
			name:    "missing region",
			keys:    [][]byte{[]byte("aaa")},
			lookups: map[string]int{"aaa": 2},
		},
	} {
		suite.Run(test.name, func() {
			re := suite.Require()
			tc := &regionLookupCountingCluster{
				Cluster: newSplitterTestCluster(suite.ctx),
				lookups: make(map[string]int),
			}
			if test.leaderless {
				tc.PutRegion(tc.GetRegion(1).Clone(core.WithLeader(nil)))
			}
			handler := newMockSplitRegionsHandler()
			splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
			originalKeys := slices.Clone(test.keys)

			percentage, newIDs := splitter.SplitRegions(suite.ctx, test.keys, 0)
			re.Equal(test.percentage, percentage)
			re.ElementsMatch(test.newIDs, newIDs)
			re.Equal(test.lookups, tc.lookups)
			re.Equal(originalKeys, test.keys, "completion filtering must preserve the request keys")
			if len(test.newIDs) == 0 {
				re.Empty(handler.regions, "existing boundaries must not schedule splits")
			}
		})
	}
}

type regionLookupCountingCluster struct {
	*mockcluster.Cluster
	lookups map[string]int
}

func (c *regionLookupCountingCluster) GetRegionByKey(key []byte) *core.RegionInfo {
	c.lookups[string(key)]++
	return c.Cluster.GetRegionByKey(key)
}

func newSplitterTestCluster(ctx context.Context) *mockcluster.Cluster {
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderStore(3, 1)
	tc.AddLeaderStore(4, 1)
	tc.AddLeaderRegionWithRange(1, "eee", "hhh", 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "hhh", "zzz", 2, 3, 4)
	return tc
}

type splitRegionsHandlerFunc struct {
	split func(*core.RegionInfo, [][]byte) error
	scan  func(*regionGroupKeys, *splitKeyResults)
}

func (h *splitRegionsHandlerFunc) SplitRegionByKeys(region *core.RegionInfo, keys [][]byte) error {
	return h.split(region, keys)
}

func (h *splitRegionsHandlerFunc) ScanRegionsByKeyRange(keys *regionGroupKeys, results *splitKeyResults) {
	if h.scan != nil {
		h.scan(keys, results)
	}
}

func TestSplitRegionsConcurrentBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tc := newSplitterTestCluster(ctx)
		calls := 0
		handler := &splitRegionsHandlerFunc{
			split: func(*core.RegionInfo, [][]byte) error {
				calls++
				// Another request splits the Region before this operator is admitted.
				tc.AddLeaderRegionWithRange(1, "eee", "fff", 2, 3, 4)
				tc.AddLeaderRegionWithRange(3, "fff", "hhh", 2, 3, 4)
				return errors.New("split operator rejected")
			},
		}
		splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
		for range 2 {
			percentage, ids := splitter.SplitRegions(ctx, [][]byte{[]byte("fff")}, 10)
			require.Equal(t, 100, percentage)
			require.Empty(t, ids, "another request's Region must not be returned as newly split")
		}
		require.Equal(t, 1, calls)
	})
}

func TestSplitRegionsBoundaryCreatedDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tc := newSplitterTestCluster(ctx)
		calls := 0
		handler := &splitRegionsHandlerFunc{
			split: func(*core.RegionInfo, [][]byte) error {
				calls++
				return errors.New("split operator rejected")
			},
		}
		go func() {
			time.Sleep(initialSleepDuration / 2)
			tc.AddLeaderRegionWithRange(1, "eee", "fff", 2, 3, 4)
			tc.AddLeaderRegionWithRange(3, "fff", "hhh", 2, 3, 4)
		}()
		splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
		percentage, ids := splitter.SplitRegions(ctx, [][]byte{[]byte("fff")}, 10)
		require.Equal(t, 100, percentage)
		require.Empty(t, ids)
		require.Equal(t, 1, calls, "the retry must recognize the newly existing boundary")
	})
}

func TestSplitRegionsRetryBackoff(t *testing.T) {
	for _, test := range []struct {
		name     string
		deadline time.Duration
	}{
		{name: "no sleep after last attempt"},
		{name: "cancel during backoff", deadline: initialSleepDuration / 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tc := newSplitterTestCluster(ctx)
				retryLimit := 0
				if test.deadline > 0 {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, test.deadline)
					defer stop()
					retryLimit = 10
				}
				calls := 0
				handler := &splitRegionsHandlerFunc{
					split: func(*core.RegionInfo, [][]byte) error {
						calls++
						return errors.New("split operator rejected")
					},
				}
				splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
				start := time.Now()
				percentage, ids := splitter.SplitRegions(ctx, [][]byte{[]byte("fff")}, retryLimit)
				require.Zero(t, percentage, "an absent boundary must remain unfinished")
				require.Empty(t, ids)
				require.Equal(t, 1, calls)
				require.Equal(t, test.deadline, time.Since(start))
			})
		})
	}
}

func TestSplitRegionsCancellationDuringDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tc := newSplitterTestCluster(ctx)
		calls := 0
		handler := &splitRegionsHandlerFunc{
			split: func(*core.RegionInfo, [][]byte) error {
				calls++
				cancel()
				return nil
			},
		}
		splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
		keys := [][]byte{[]byte("eee"), []byte("fff"), []byte("hhh"), []byte("iii")}
		percentage, ids := splitter.SplitRegions(ctx, keys, 10)
		require.Equal(t, 50, percentage, "preserve completed boundaries on cancellation")
		require.Empty(t, ids)
		require.Equal(t, 1, calls, "stop dispatching additional operators after cancellation")

		percentage, ids = splitter.SplitRegions(ctx, keys, 10)
		require.Zero(t, percentage)
		require.Empty(t, ids)
		require.Equal(t, 1, calls, "a cancelled request must not dispatch any operators")
	})
}

func TestSplitRegionsCancellationWithPartialResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tc := newSplitterTestCluster(ctx)
		calls := 0
		handler := &splitRegionsHandlerFunc{
			split: func(*core.RegionInfo, [][]byte) error {
				calls++
				return nil
			},
			scan: func(_ *regionGroupKeys, results *splitKeyResults) {
				results.addRegionsID(map[uint64][]byte{3: []byte("fff")})
				cancel()
			},
		}
		splitter := NewRegionSplitter(tc, handler, tc.AddPendingProcessedRegions)
		percentage, ids := splitter.SplitRegions(ctx, [][]byte{[]byte("fff"), []byte("ggg")}, 10)
		require.Equal(t, 50, percentage)
		require.Equal(t, []uint64{3}, ids, "preserve split results observed before cancellation")
		require.Equal(t, 1, calls)
	})
}
