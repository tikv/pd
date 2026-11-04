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

package affinity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type failingAffinityStorage struct {
	endpoint.AffinityStorage
	txnCount int
	failAt   int
}

func (s *failingAffinityStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	s.txnCount++
	if s.txnCount == s.failAt {
		return errors.New("injected affinity transaction failure")
	}
	return s.AffinityStorage.RunInTxn(ctx, f)
}

func TestCreateAffinityGroupsBatchFailure(t *testing.T) {
	testAffinityGroupsBatchFailure(t, false)
}

func TestDeleteAffinityGroupsBatchFailure(t *testing.T) {
	testAffinityGroupsBatchFailure(t, true)
}

func TestGroupRuleSyncOrder(t *testing.T) {
	for _, ruleFirst := range []bool{false, true} {
		for _, ruleDeletedFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("rule-first=%t/rule-deleted-first=%t", ruleFirst, ruleDeletedFirst), func(t *testing.T) {
				re := require.New(t)
				store := storage.NewStorageWithMemoryBackend()
				regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Hour)
				re.NoError(err)
				manager, err := NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
				re.NoError(err)
				change := GroupKeyRanges{GroupID: "sync-order", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
				data, err := json.Marshal(MakeLabelRule(change))
				re.NoError(err)
				rule, err := labeler.NewLabelRuleFromJSON(data)
				re.NoError(err)
				putGroup := func() { manager.SyncGroupFromEtcd(&Group{ID: change.GroupID}) }
				putRule := func() { re.NoError(manager.SyncKeyRangesFromEtcd(rule)) }
				if ruleFirst {
					putRule()
					putGroup()
				} else {
					putGroup()
					putRule()
				}
				re.Equal(1, manager.GetAffinityGroupState(change.GroupID).RangeCount)
				re.Equal(change, manager.keyRanges[change.GroupID])
				re.Empty(manager.labelRuleBuffer)
				deleteGroup := func() { manager.SyncGroupDeleteFromEtcd(change.GroupID) }
				deleteRule := func() { manager.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(change.GroupID)) }
				if ruleDeletedFirst {
					deleteRule()
					deleteGroup()
				} else {
					deleteGroup()
					deleteRule()
				}
				re.False(manager.IsGroupExist(change.GroupID))
				re.Empty(manager.keyRanges)
				re.Empty(manager.labelRuleBuffer)
			})
		}
	}
}

func testAffinityGroupsBatchFailure(t *testing.T, deleting bool) {
	for _, withEmptyGroup := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty-group=%t", withEmptyGroup), func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
				ServerCfgModifier: func(cfg *embed.Config) { cfg.MaxTxnOps = etcdutil.MaxEtcdTxnOps },
			})
			t.Cleanup(clean)
			store := storage.NewStorageWithEtcdBackend(client)
			labelerManager, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
			re.NoError(err)
			failingStore := &failingAffinityStorage{AffinityStorage: store}
			manager, err := NewManager(ctx, failingStore, core.NewStoresInfo(), mockconfig.NewTestOptions(), labelerManager)
			re.NoError(err)

			changes := make([]GroupKeyRanges, etcdutil.MaxEtcdTxnOps/2+2)
			ids := make([]string, len(changes))
			for i := range changes {
				ids[i] = fmt.Sprintf("batch-group-%d", i)
				changes[i] = GroupKeyRanges{
					GroupID: ids[i],
					KeyRanges: []keyutil.KeyRange{{
						StartKey: []byte(fmt.Sprintf("%04d", i)),
						EndKey:   []byte(fmt.Sprintf("%04d", i+1)),
					}},
				}
			}
			if withEmptyGroup {
				// One operation before the pairs makes a fixed 120-operation cut unsafe.
				changes[0].KeyRanges = nil
			}
			if deleting {
				re.NoError(manager.CreateAffinityGroups(changes))
			}
			failingStore.txnCount, failingStore.failAt = 0, 2
			if deleting {
				err = manager.DeleteAffinityGroups(ids, true)
			} else {
				err = manager.CreateAffinityGroups(changes)
			}
			re.ErrorContains(err, "injected affinity transaction failure")
			re.Equal(2, failingStore.txnCount)

			persisted := make(map[string]bool)
			for _, change := range changes {
				resp, err := client.Txn(ctx).Then(
					clientv3.OpGet(keypath.AffinityGroupPath(change.GroupID)),
					clientv3.OpGet(keypath.RegionLabelKeyPath(GetLabelRuleID(change.GroupID))),
				).Commit()
				re.NoError(err)
				groupKVs := resp.Responses[0].GetResponseRange().Kvs
				ruleKVs := resp.Responses[1].GetResponseRange().Kvs
				hasGroup := len(groupKVs) > 0
				re.Equal(hasGroup && len(change.KeyRanges) > 0, len(ruleKVs) > 0,
					"group %s and its label rule must persist together", change.GroupID)
				if hasGroup {
					persisted[change.GroupID] = true
					if len(ruleKVs) > 0 {
						re.Equal(groupKVs[0].ModRevision, ruleKVs[0].ModRevision)
					}
				}
				// A partial commit must not apply the plan to local memory.
				re.Equal(deleting, manager.IsGroupExist(change.GroupID))
				re.Equal(deleting && len(change.KeyRanges) > 0, labelerManager.GetLabelRule(GetLabelRuleID(change.GroupID)) != nil)
			}
			re.NotEmpty(persisted)
			re.Less(len(persisted), len(changes))

			// Classic restart loads only complete groups and rebuilds their ranges.
			reloadedLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
			re.NoError(err)
			reloadedManager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), reloadedLabeler)
			re.NoError(err)
			for _, change := range changes {
				re.Equal(persisted[change.GroupID], reloadedManager.IsGroupExist(change.GroupID))
				if state := reloadedManager.GetAffinityGroupState(change.GroupID); state != nil {
					re.Equal(len(change.KeyRanges), state.RangeCount)
				}
			}
		})
	}
}
