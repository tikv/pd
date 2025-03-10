// Copyright 2022 TiKV Project Authors.
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

package schedulers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/buckets"
)

func TestSplitBucket(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.AddRegionStore(10, 10)
	hotBuckets := make(map[uint64][]*buckets.BucketStat, 10)
	// init cluster: there are 8 regions and their size is 600MB,
	// their key range is [1 10][11 20]....[71 80]
	for i := range uint64(8) {
		peers := []*metapb.Peer{{
			Id:      i * 100,
			StoreId: i,
			Role:    metapb.PeerRole_Voter,
		}, {
			Id:      i*100 + 1,
			StoreId: i + 1,
			Role:    metapb.PeerRole_Voter,
		}, {
			Id:      i*100 + 2,
			StoreId: i + 2,
			Role:    metapb.PeerRole_Voter,
		}}

		metaRegion := &metapb.Region{
			Id:       i,
			Peers:    peers,
			StartKey: []byte(fmt.Sprintf("%20d", i*10+1)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}

		region := core.NewRegionInfo(metaRegion, peers[0], core.SetApproximateSize(600))
		tc.PutRegion(region)
	}

	conf := &splitBucketSchedulerConfig{Degree: 10}
	scheduler := newSplitBucketScheduler(oc, nil)

	// case1: the key range of the hot bucket stat is [1 2] and the region is [1 10],
	// so it can split two regions by [1 2] and [2 10].
	hotBuckets[0] = []*buckets.BucketStat{{
		RegionID:  0,
		HotDegree: 10,
		StartKey:  []byte(fmt.Sprintf("%20d", 1)),
		EndKey:    []byte(fmt.Sprintf("%20d", 2)),
	}}
	plan := &splitBucketPlan{
		cluster:            tc,
		hotBuckets:         hotBuckets,
		hotRegionSplitSize: 512,
		conf:               conf,
	}
	ops := scheduler.splitBucket(plan)
	re.Len(ops, 1)
	step := ops[0].Step(0).(operator.SplitRegion)
	re.Len(step.SplitKeys, 1)
	re.Equal([]byte(fmt.Sprintf("%20d", 2)), step.SplitKeys[0])

	// case 2: the key range of the hot bucket stat is [1 10] and the region is [1 10],
	// it can't be split.
	hotBuckets[0][0].EndKey = []byte(fmt.Sprintf("%20d", 10))
	ops = scheduler.splitBucket(plan)
	re.Empty(ops)

	// case 3: the key range of the hot bucket stat is [0 9], the key range is not less
	// than the region [1 10], it will have no operator.
	hotBuckets[0][0].StartKey = []byte(fmt.Sprintf("%20d", 0))
	hotBuckets[0][0].EndKey = []byte(fmt.Sprintf("%20d", 9))
	ops = scheduler.splitBucket(plan)
	re.Empty(ops)

	// case 3: the key range of the hot bucket stat is [3 9]
	// it can split by [2 3],[3 9],[9 10]
	hotBuckets[0][0].StartKey = []byte(fmt.Sprintf("%20d", 3))
	ops = scheduler.splitBucket(plan)
	re.Len(ops, 1)
	step = ops[0].Step(0).(operator.SplitRegion)
	re.Len(step.SplitKeys, 2)
}
