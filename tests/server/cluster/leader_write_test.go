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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// Hold a write from the actual node-state job until a new PD leader has
// persisted newer store metadata. The old transaction must leave it untouched.
func TestOldTermStoreWriteIsFenced(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer tc.Destroy()
	const fast = "github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"
	re.NoError(failpoint.Enable(fast, "return(true)"))
	defer func() { re.NoError(failpoint.Disable(fast)) }()
	re.NoError(tc.RunInitialServers())
	oldName := tc.WaitLeader()
	re.NotEmpty(oldName)
	old := tc.GetServer(oldName)
	re.NoError(old.BootstrapCluster())
	oldServer := old.GetServer()
	oldRC := oldServer.GetRaftCluster()
	re.NotNil(oldRC)
	oldCtx := oldRC.Context()
	oldClient := oldServer.GetClient()
	leadership := oldServer.GetMember().GetLeadership()
	leaderKey, leaderValue := leadership.GetLeaderKey(), leadership.GetLeaderValue()
	const storeID uint64 = 42
	storeKey := keypath.StorePath(storeID)
	paused, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	var entered atomic.Bool
	type result struct {
		succeeded bool
		err       error
	}
	committed := make(chan result, 1)
	isTarget := func(client *clientv3.Client, key, value string) bool {
		if client != oldClient || key != storeKey {
			return false
		}
		var meta metapb.Store
		return meta.Unmarshal([]byte(value)) == nil && meta.GetNodeState() == metapb.NodeState_Serving
	}
	const before = "github.com/tikv/pd/pkg/storage/kv/beforeSaveCommit"
	const after = "github.com/tikv/pd/pkg/storage/kv/afterSaveCommit"
	re.NoError(failpoint.EnableCall(before, func(client *clientv3.Client, key, value string) {
		if isTarget(client, key, value) && entered.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}))
	defer func() { re.NoError(failpoint.Disable(before)) }()
	re.NoError(failpoint.EnableCall(after, func(client *clientv3.Client, key, value string, resp *clientv3.TxnResponse, err *error) {
		if isTarget(client, key, value) {
			select {
			case committed <- result{resp != nil && resp.Succeeded, *err}:
			default:
			}
		}
	}))
	defer func() { re.NoError(failpoint.Disable(after)) }()
	defer release()
	re.NoError(oldRC.PutMetaStore(&metapb.Store{
		Id: storeID, Address: "mock://audit-store:42", Version: oldRC.GetStore(1).GetVersion(),
		NodeState: metapb.NodeState_Preparing,
		Labels:    []*metapb.StoreLabel{{Key: "zone", Value: "old-zone"}},
	}))
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		t.Fatal("real node-state job did not reach Save")
	}
	var next *tests.TestServer
	for name, member := range tc.GetServers() {
		if name != oldName {
			next = member
			break
		}
	}
	re.NotNil(next)
	re.NoError(old.MoveEtcdLeader(oldServer.GetMember().ID(), next.GetServer().GetMember().ID()))
	testutil.Eventually(re, func() bool {
		return next.GetServer().IsServing() && next.GetServer().GetRaftCluster() != nil && oldCtx.Err() != nil
	}, testutil.WithWaitFor(8*time.Second), testutil.WithTickInterval(10*time.Millisecond))
	re.ErrorIs(oldCtx.Err(), context.Canceled)
	re.False(oldServer.IsServing())
	re.False(oldRC.IsRunning())
	re.NoError(oldClient.Ctx().Err())
	newRC := next.GetServer().GetRaftCluster()
	testutil.Eventually(re, func() bool { return newRC.GetStore(storeID).IsServing() })
	re.NoError(newRC.UpdateStoreLabels(storeID, []*metapb.StoreLabel{{Key: "zone", Value: "new-zone"}}, true))
	readStore := func() (*metapb.Store, int64) {
		resp, err := next.GetEtcdClient().Get(ctx, storeKey)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		meta := &metapb.Store{}
		re.NoError(meta.Unmarshal(resp.Kvs[0].Value))
		return meta, resp.Kvs[0].ModRevision
	}
	meta, beforeRevision := readStore()
	re.Equal("new-zone", meta.Labels[0].Value)
	leaderResp, err := next.GetEtcdClient().Get(ctx, leaderKey)
	re.NoError(err)
	re.Len(leaderResp.Kvs, 1)
	re.NotEqual(leaderValue, string(leaderResp.Kvs[0].Value))
	t.Logf("old=%s new=%s old-context=%v old-running=%v; new label=%s revision=%d leader-revision=%d", oldName, next.GetServer().Name(), oldCtx.Err(), oldRC.IsRunning(), meta.Labels[0].Value, beforeRevision, leaderResp.Kvs[0].ModRevision)
	release()
	select {
	case res := <-committed:
		re.NoError(res.err)
		re.False(res.succeeded)
	case <-time.After(10 * time.Second):
		t.Fatal("old Save did not finish")
	}
	meta, afterRevision := readStore()
	re.Equal("new-zone", meta.Labels[0].Value)
	re.Equal(beforeRevision, afterRevision)
	re.Equal("new-zone", newRC.GetStore(storeID).GetLabelValue("zone"))
	t.Logf("final persisted label=%s revision=%d; new leader cache label=%s", meta.Labels[0].Value, afterRevision, newRC.GetStore(storeID).GetLabelValue("zone"))
}
