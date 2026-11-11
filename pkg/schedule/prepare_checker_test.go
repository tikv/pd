// Copyright 2026 TiKV Project Authors.
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

package schedule

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPrepareCheckerRequiresAllRegions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		total         int
		cached        int
		collected     int
		storeReported int
		prepared      bool
	}{
		{name: "global 90 percent", total: 100, cached: 90, collected: 90, storeReported: 90},
		{name: "global 99 percent", total: 100, cached: 99, collected: 99, storeReported: 99},
		{name: "store reported 99 percent", total: 99, cached: 99, collected: 99, storeReported: 100},
		{name: "store cached 99 percent", total: 99, cached: 100, collected: 99, storeReported: 99},
		{name: "all collected", total: 100, cached: 100, collected: 100, storeReported: 100, prepared: true},
		{name: "exceeds upstream count", total: 99, cached: 100, collected: 100, storeReported: 100, prepared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			peer := &metapb.Peer{Id: 1, StoreId: 1}
			cluster.PutStore(core.NewStoreInfo(
				&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving},
				core.SetStoreStats(&pdpb.StoreStats{RegionCount: uint32(tc.storeReported)}),
			))
			for i := range tc.cached {
				source := core.Storage
				if i < tc.collected {
					source = core.Heartbeat
				}
				cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
					Id:       uint64(i + 1),
					StartKey: []byte(fmt.Sprintf("%03d", i)),
					EndKey:   []byte(fmt.Sprintf("%03d", i+1)),
					Peers:    []*metapb.Peer{peer},
				}, peer, core.SetSource(source)))
			}
			checker := newPrepareChecker(func() (int, error) { return tc.total, nil })
			re.Equal(tc.prepared, checker.check(cluster))
			re.Equal(tc.prepared, checker.IsPrepared())
		})
	}
}

func TestPrepareCheckerCollectionTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster := core.NewBasicCluster()
		checker := newPrepareChecker(func() (int, error) { return 100, nil })
		re.False(checker.check(cluster))
		time.Sleep(CollectTimeout)
		re.False(checker.check(cluster))
		time.Sleep(time.Nanosecond)
		re.True(checker.check(cluster))
		re.True(checker.IsPrepared())
	})
}
