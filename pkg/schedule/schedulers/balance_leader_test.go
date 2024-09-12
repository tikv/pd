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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestBalanceLeaderSchedulerConfigClone(t *testing.T) {
	re := require.New(t)
	keyRanges1, _ := getKeyRanges([]string{"a", "b", "c", "d"})
	conf := &balanceLeaderSchedulerConfig{
		Ranges: keyRanges1,
		Batch:  10,
	}
	conf2 := conf.clone()
	re.Equal(conf.Batch, conf2.Batch)
	re.Equal(conf.Ranges, conf2.Ranges)

	keyRanges2, _ := getKeyRanges([]string{"e", "f", "g", "h"})
	// update conf2
	conf2.Ranges[1] = keyRanges2[1]
	re.NotEqual(conf.Ranges, conf2.Ranges)
}

func BenchmarkCandidateStores(b *testing.B) {
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()

	for id := uint64(1); id < uint64(10000); id++ {
		leaderCount := int(rand.Int31n(10000))
		tc.AddLeaderStore(id, leaderCount)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updateAndResortStoresInCandidateStores(tc)
	}
}

func updateAndResortStoresInCandidateStores(tc *mockcluster.Cluster) {
	deltaMap := make(map[uint64]int64)
	getScore := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(0, deltaMap[store.GetID()])
	}
	cs := newCandidateStores(tc.GetStores(), false, getScore)
	stores := tc.GetStores()
	// update score for store and reorder
	for id, store := range stores {
		offsets := cs.binarySearchStores(store)
		if id%2 == 1 {
			deltaMap[store.GetID()] = int64(rand.Int31n(10000))
		} else {
			deltaMap[store.GetID()] = int64(-rand.Int31n(10000))
		}
		cs.resortStoreWithPos(offsets[0])
	}
}
