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

package cluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tests"
)

// TestCrossTableMergeWithKeyspace verifies through a real pd-server that
// enable-cross-table-merge=false blocks merging adjacent regions of different
// tables under a keyspace, while same-table regions still merge. See #10991.
func TestCrossTableMergeWithKeyspace(t *testing.T) {
	re := require.New(t)
	tc, err := tests.NewTestCluster(t.Context(), 1)
	defer tc.Destroy()
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	tests.MustPutStore(re, tc, &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		NodeState:     metapb.NodeState_Serving,
		LastHeartbeat: time.Now().UnixNano(),
	})

	// Mirror the issue reproduction: pd-ctl config set enable-cross-table-merge false.
	svr := leaderServer.GetServer()
	schedule := leaderServer.GetConfig().Schedule
	schedule.EnableCrossTableMerge = false
	schedule.SplitMergeInterval = typeutil.NewDuration(time.Second)
	re.NoError(svr.SetScheduleConfig(schedule))
	replication := leaderServer.GetConfig().Replication
	replication.MaxReplicas = 1
	re.NoError(svr.SetReplicationConfig(replication))

	// Keyspace 42, txn mode.
	keyspacePrefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 42)
	tableKey := func(tableID int64) []byte {
		return codec.EncodeBytes(append(append([]byte{}, keyspacePrefix...), codec.GenerateTableKey(tableID)...))
	}
	rowKey := func(tableID, rowID int64) []byte {
		return codec.EncodeBytes(append(append([]byte{}, keyspacePrefix...), codec.GenerateRowKey(tableID, rowID)...))
	}

	// Five contiguous regions inside keyspace 42: three empty single-table
	// regions (tables 100..102), then table 103 split at a row key so its two
	// halves form a same-table merge control pair.
	regions := []struct {
		id         uint64
		start, end []byte
	}{
		{10, tableKey(100), tableKey(101)},
		{11, tableKey(101), tableKey(102)},
		{12, tableKey(102), tableKey(103)},
		{13, tableKey(103), rowKey(103, 500)},
		{14, rowKey(103, 500), tableKey(104)},
	}
	for _, r := range regions {
		tests.MustPutRegion(re, tc, r.id, 1, r.start, r.end,
			core.SetApproximateSize(1), core.SetApproximateKeys(1))
	}

	oc := leaderServer.GetRaftCluster().GetOperatorController()
	// The same-table pair must merge: proves the whole merge pipeline
	// (patrol -> merge checker -> operator) is live in this setup.
	testutil.Eventually(re, func() bool {
		op13, op14 := oc.GetOperator(13), oc.GetOperator(14)
		return op13 != nil && op14 != nil
	})
	// Regions of different tables under the same keyspace must never be
	// merged while enable-cross-table-merge is false.
	for range 20 {
		for _, id := range []uint64{10, 11, 12} {
			re.Nil(oc.GetOperator(id), "unexpected operator on cross-table region %d", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
