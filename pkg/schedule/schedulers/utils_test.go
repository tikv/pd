// Copyright 2021 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func TestRetryQuota(t *testing.T) {
	re := require.New(t)

	q := newRetryQuota()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2})
	keepStores := []*core.StoreInfo{store1}

	// test getLimit
	re.Equal(10, q.getLimit(store1))

	// test attenuate
	for _, expected := range []int{5, 2, 1, 1, 1} {
		q.attenuate(store1)
		re.Equal(expected, q.getLimit(store1))
	}

	// test GC
	re.Equal(10, q.getLimit(store2))
	q.attenuate(store2)
	re.Equal(5, q.getLimit(store2))
	q.gc(keepStores)
	re.Equal(1, q.getLimit(store1))
	re.Equal(10, q.getLimit(store2))

	// test resetLimit
	q.resetLimit(store1)
	re.Equal(10, q.getLimit(store1))
}

func TestGetCountThreshold(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.PutStoreWithLabels(1, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(2, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(3, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(4, "region", "z2", "zone", "z1")
	tc.PutStoreWithLabels(5, "region", "z2", "zone", "z1")

	rule1 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_0",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Leader,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
		LocationLabels: []string{"zone"},
	}
	rule2 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_1",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Follower,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
		LocationLabels: []string{"zone"},
	}

	rule3 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_2",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Learner,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z2"}},
		},
		LocationLabels: []string{"zone"},
	}

	re.NoError(tc.SetRules([]*placement.Rule{rule1, rule2, rule3}))
	re.NoError(tc.GetRuleManager().DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID))

	for i := range 100 {
		starKey, endKey := 100+i-1, 100+i
		tc.AddLeaderRegionWithRange(uint64(i), strconv.Itoa(starKey), strconv.Itoa(endKey), 1, 2, 4)
	}

	available := 0
	for _, store := range tc.GetStores() {
		count := GetCountThreshold(tc, tc.GetStores(), store, keyutil.NewKeyRange("100", "200"), core.LeaderScatter)
		if count > 0 {
			available++
		}
	}
	re.Equal(3, available)
}
