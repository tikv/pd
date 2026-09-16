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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
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

// A real gRPC PutStore snapshots a serving store, then survives A -> B -> A.
// B tombstones the store while that call is paused before GetStorage.
func TestOldTermPutStoreCannotAdoptNewLease(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			testOldTermPutStore(t, committed)
		})
	}
}

func testOldTermPutStore(t *testing.T, alreadyCommitted bool) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer tc.Destroy()
	re.NoError(tc.RunInitialServers())
	oldName := tc.WaitLeader()
	re.NotEmpty(oldName)
	old := tc.GetServer(oldName)
	re.NoError(old.BootstrapCluster())
	oldServer := old.GetServer()
	oldRC := oldServer.GetRaftCluster()
	re.NotNil(oldRC)
	oldCtx := oldRC.Context()
	oldLease := oldServer.GetMember().GetLeadership().GetLease().GetID()
	const storeID uint64 = 42
	store := &metapb.Store{Id: storeID, Address: "mock://qa-store:42", Version: oldRC.GetStore(1).GetVersion(), State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving, Labels: []*metapb.StoreLabel{{Key: "zone", Value: "initial"}}}
	re.NoError(oldRC.PutMetaStore(store))

	// Exercise the dynamically-created scheduler path as well as managers
	// constructed by Start.
	re.NoError(oldServer.GetHandler().AddScheduler(types.EvictLeaderScheduler, "1"))
	oldScheduler := oldRC.GetCoordinator().GetSchedulersController().GetSchedulerHandlers()[types.EvictLeaderScheduler.String()]
	re.NotNil(oldScheduler)
	updateScheduler := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		oldScheduler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/config", strings.NewReader(`{"batch":5}`)))
		return response
	}
	re.Equal(http.StatusOK, updateScheduler().Code)

	paused, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	var entered atomic.Bool
	pause := func(client *clientv3.Client, meta *metapb.Store) {
		if client == oldServer.GetClient() && meta.GetId() == storeID && meta.GetNodeState() == metapb.NodeState_Serving && len(meta.Labels) > 0 && meta.Labels[0].Value == "old-request" && entered.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}
	hook := "github.com/tikv/pd/server/cluster/beforePersistStore"
	if alreadyCommitted {
		hook = "github.com/tikv/pd/pkg/storage/kv/afterSaveCommit"
		re.NoError(failpoint.EnableCall(hook, func(client *clientv3.Client, key, value string, resp *clientv3.TxnResponse, _ *error) {
			if key != keypath.StorePath(storeID) || resp == nil || !resp.Succeeded {
				return
			}
			meta := &metapb.Store{}
			if meta.Unmarshal([]byte(value)) == nil {
				pause(client, meta)
			}
		}))
	} else {
		re.NoError(failpoint.EnableCall(hook, pause))
	}
	defer func() { re.NoError(failpoint.Disable(hook)) }()
	defer release()
	grpcClient, conn := testutil.MustNewGrpcClient(re, old.GetAddr())
	defer conn.Close()
	type callResult struct {
		response *pdpb.PutStoreResponse
		err      error
	}
	finished := make(chan callResult, 1)
	requestStore := *store
	requestStore.Labels = []*metapb.StoreLabel{{Key: "zone", Value: "old-request"}}
	go func() {
		res, err := grpcClient.PutStore(ctx, &pdpb.PutStoreRequest{Header: &pdpb.RequestHeader{ClusterId: old.GetClusterID()}, Store: &requestStore})
		finished <- callResult{res, err}
	}()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("PutStore did not pause")
	}
	var next *tests.TestServer
	for name, server := range tc.GetServers() {
		if name != oldName {
			next = server
			break
		}
	}
	re.NotNil(next)
	re.NoError(old.MoveEtcdLeader(oldServer.GetMember().ID(), next.GetServer().GetMember().ID()))
	testutil.Eventually(re, func() bool {
		return next.GetServer().IsServing() && next.GetServer().GetRaftCluster() != nil && oldCtx.Err() != nil
	}, testutil.WithWaitFor(15*time.Second))
	re.False(oldRC.IsRunning())
	re.NoError(next.GetServer().GetRaftCluster().UpdateStoreLabels(storeID, []*metapb.StoreLabel{{Key: "zone", Value: "new-leader"}}, true))
	re.NoError(next.GetServer().GetRaftCluster().BuryStore(storeID, true))
	re.True(next.GetServer().GetRaftCluster().GetStore(storeID).IsRemoved())
	readStore := func() (*metapb.Store, int64) {
		resp, err := oldServer.GetClient().Get(ctx, keypath.StorePath(storeID))
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		meta := &metapb.Store{}
		re.NoError(meta.Unmarshal(resp.Kvs[0].Value))
		return meta, resp.Kvs[0].ModRevision
	}
	before, beforeRev := readStore()
	re.Equal(metapb.NodeState_Removed, before.GetNodeState())
	re.NoError(next.MoveEtcdLeader(next.GetServer().GetMember().ID(), oldServer.GetMember().ID()))
	testutil.Eventually(re, func() bool { return oldServer.IsServing() && oldServer.GetRaftCluster() != nil }, testutil.WithWaitFor(15*time.Second))
	re.NotSame(oldRC, oldServer.GetRaftCluster())
	re.Same(oldRC.GetBasicCluster(), oldServer.GetRaftCluster().GetBasicCluster())
	re.True(oldRC.GetStore(storeID).IsRemoved())
	newLease := oldServer.GetMember().GetLeadership().GetLease().GetID()
	re.NotEqual(oldLease, newLease)
	release()
	select {
	case result := <-finished:
		re.NoError(result.err)
		re.Equal("new-leader", oldServer.GetRaftCluster().GetStore(storeID).GetLabelValue("zone"))
		re.NotNil(result.response.GetHeader().GetError())
		re.Contains(result.response.GetHeader().GetError().GetMessage(), errs.ErrEtcdTxnConflict.Error())
	case <-ctx.Done():
		t.Fatal("PutStore did not return")
	}
	after, afterRev := readStore()
	t.Logf("retained RaftCluster=%p old lease=%d new lease=%d; before persisted state=%s revision=%d; after persisted state=%s revision=%d; current cache removed=%v", oldRC, oldLease, newLease, before.GetNodeState(), beforeRev, after.GetNodeState(), afterRev, oldRC.GetStore(storeID).IsRemoved())
	re.Equal("new-leader", oldServer.GetRaftCluster().GetStore(storeID).GetLabelValue("zone"))
	// Manager getters on a retained handle must not follow the new run either.
	re.NotSame(oldRC.GetRegionLabeler(), oldServer.GetRaftCluster().GetRegionLabeler())
	re.NotSame(oldRC.GetRuleManager(), oldServer.GetRaftCluster().GetRuleManager())
	rule := &labeler.LabelRule{ID: "old-term-rule", Labels: []labeler.RegionLabel{{Key: "test", Value: "old"}}, RuleType: "key-range", Data: []any{map[string]any{"start_key": "", "end_key": ""}}}
	re.Error(oldRC.GetRegionLabeler().SetLabelRule(rule))
	rule.Data = []any{map[string]any{"start_key": "", "end_key": ""}}
	re.NoError(oldServer.GetRaftCluster().GetRegionLabeler().SetLabelRule(rule))
	schedulersBefore := oldServer.GetPersistOptions().GetScheduleConfig().Clone().Schedulers
	re.ErrorIs(oldRC.RemoveScheduler(types.EvictLeaderScheduler.String()), context.Canceled)
	re.Equal(schedulersBefore, oldServer.GetPersistOptions().GetScheduleConfig().Schedulers)
	response := updateScheduler()
	re.Equal(http.StatusBadRequest, response.Code)
	re.Contains(response.Body.String(), errs.ErrEtcdTxnConflict.Error())
	// The shared ExternalTS cache must only publish successful current writes.
	currentRC := oldServer.GetRaftCluster()
	oldServer.GetPersistOptions().SetMaxReplicas(1)
	re.NoError(currentRC.SetExternalTS(20))
	re.ErrorIs(oldRC.SetExternalTS(10), errs.ErrEtcdTxnConflict)
	re.Equal(uint64(20), currentRC.GetExternalTS())
	re.Error(oldServer.SetExternalTS(15, 100))
	// The delayed request must retain its original lease even after re-election.
	re.Equal(metapb.NodeState_Removed, after.GetNodeState())
	re.Equal(beforeRev, afterRev)
	re.True(oldRC.GetStore(storeID).IsRemoved())
}

// A task already executing in ConcurrentRunner may outlive Stop. Its closure
// must retain the request's term even when the same member leads again.
func TestOldTermRegionTaskCannotAdoptNewLease(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.PDServerCfg.UseRegionStorage = false
		conf.Schedule.EnableHeartbeatConcurrentRunner = true
	})
	re.NoError(err)
	defer tc.Destroy()
	re.NoError(tc.RunInitialServers())
	oldName := tc.WaitLeader()
	re.NotEmpty(oldName)
	old := tc.GetServer(oldName)
	re.NoError(old.BootstrapCluster())
	oldServer := old.GetServer()
	oldRC := oldServer.GetRaftCluster()
	re.NotNil(oldRC)
	cfg := oldRC.GetScheduleConfig().Clone()
	cfg.EnableHeartbeatConcurrentRunner = true
	oldRC.SetScheduleConfig(cfg)
	oldCtx := oldRC.Context()
	oldLease := oldServer.GetMember().GetLeadership().GetLease().GetID()
	region := oldRC.GetRegions()[0].Clone(core.SetRegionVersion(10))
	regionKey := keypath.RegionPath(region.GetID())
	paused, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	var entered atomic.Bool
	const hook = "github.com/tikv/pd/server/cluster/beforePersistRegion"
	re.NoError(failpoint.EnableCall(hook, func(client *clientv3.Client, meta *metapb.Region) {
		if client == oldServer.GetClient() && meta.GetId() == region.GetID() && meta.GetRegionEpoch().GetVersion() == 10 && entered.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}))
	defer func() { re.NoError(failpoint.Disable(hook)) }()
	committed := make(chan bool, 1)
	const after = "github.com/tikv/pd/pkg/storage/kv/afterSaveCommit"
	re.NoError(failpoint.EnableCall(after, func(client *clientv3.Client, key, value string, resp *clientv3.TxnResponse, _ *error) {
		if client != oldServer.GetClient() || key != regionKey {
			return
		}
		meta := &metapb.Region{}
		if meta.Unmarshal([]byte(value)) == nil && meta.GetRegionEpoch().GetVersion() == 10 {
			committed <- resp != nil && resp.Succeeded
		}
	}))
	defer func() { re.NoError(failpoint.Disable(after)) }()
	defer release()
	re.NoError(oldRC.HandleRegionHeartbeat(region))
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("region task did not pause")
	}
	var next *tests.TestServer
	for name, server := range tc.GetServers() {
		if name != oldName {
			next = server
			break
		}
	}
	re.NotNil(next)
	re.NoError(old.MoveEtcdLeader(oldServer.GetMember().ID(), next.GetServer().GetMember().ID()))
	testutil.Eventually(re, func() bool {
		return next.GetServer().IsServing() && next.GetServer().GetRaftCluster() != nil && oldCtx.Err() != nil
	}, testutil.WithWaitFor(15*time.Second))
	newerRegion := region.Clone(core.SetRegionVersion(20))
	re.NoError(next.GetServer().GetRaftCluster().HandleRegionHeartbeat(newerRegion))
	readRegion := func() (*metapb.Region, int64) {
		resp, err := oldServer.GetClient().Get(ctx, regionKey)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		meta := &metapb.Region{}
		re.NoError(meta.Unmarshal(resp.Kvs[0].Value))
		return meta, resp.Kvs[0].ModRevision
	}
	testutil.Eventually(re, func() bool {
		meta, _ := readRegion()
		return meta.GetRegionEpoch().GetVersion() == 20
	})
	_, beforeRevision := readRegion()
	re.NoError(next.MoveEtcdLeader(next.GetServer().GetMember().ID(), oldServer.GetMember().ID()))
	testutil.Eventually(re, func() bool { return oldServer.IsServing() && oldServer.GetRaftCluster() != nil }, testutil.WithWaitFor(15*time.Second))
	re.NotSame(oldRC, oldServer.GetRaftCluster())
	re.NotEqual(oldLease, oldServer.GetMember().GetLeadership().GetLease().GetID())
	release()
	select {
	case succeeded := <-committed:
		re.False(succeeded)
	case <-ctx.Done():
		t.Fatal("old region write did not finish")
	}
	meta, afterRevision := readRegion()
	re.Equal(uint64(20), meta.GetRegionEpoch().GetVersion())
	re.Equal(beforeRevision, afterRevision)
}

// An in-flight ExternalTS setter must neither hold up the replacement run nor
// publish into its cache after the original member is elected again.
func TestOldTermExternalTSInFlight(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			tc, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
				conf.PDServerCfg.UseRegionStorage = false
				conf.Replication.MaxReplicas = 1
			})
			re.NoError(err)
			defer tc.Destroy()
			re.NoError(tc.RunInitialServers())
			oldName := tc.WaitLeader()
			re.NotEmpty(oldName)
			old := tc.GetServer(oldName)
			re.NoError(old.BootstrapCluster())
			oldServer := old.GetServer()
			oldRC := oldServer.GetRaftCluster()
			oldCtx := oldRC.Context()
			oldServer.GetPersistOptions().SetMaxReplicas(1)
			paused, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var entered atomic.Bool
			pause := func(client *clientv3.Client, key, value string) {
				if client == oldServer.GetClient() && key == keypath.ExternalTimestampPath() && value == "a" && entered.CompareAndSwap(false, true) {
					close(paused)
					<-resume
				}
			}
			hook := "github.com/tikv/pd/pkg/storage/kv/beforeSaveCommit"
			if committed {
				hook = "github.com/tikv/pd/pkg/storage/kv/afterSaveCommit"
				re.NoError(failpoint.EnableCall(hook, func(client *clientv3.Client, key, value string, resp *clientv3.TxnResponse, _ *error) {
					if resp != nil && resp.Succeeded {
						pause(client, key, value)
					}
				}))
			} else {
				re.NoError(failpoint.EnableCall(hook, pause))
			}
			defer func() { re.NoError(failpoint.Disable(hook)) }()
			defer release()
			finished := make(chan error, 1)
			go func() { finished <- oldServer.SetExternalTS(10, 100) }()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("ExternalTS setter did not pause")
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
			}, testutil.WithWaitFor(15*time.Second))
			next.GetServer().GetPersistOptions().SetMaxReplicas(1)
			re.NoError(next.GetServer().SetExternalTS(20, 100))
			re.NoError(next.MoveEtcdLeader(next.GetServer().GetMember().ID(), oldServer.GetMember().ID()))
			testutil.Eventually(re, func() bool {
				return oldServer.IsServing() && oldServer.GetRaftCluster() != nil
			}, testutil.WithWaitFor(15*time.Second))
			re.NotSame(oldRC, oldServer.GetRaftCluster())
			oldServer.GetPersistOptions().SetMaxReplicas(1)
			currentDone := make(chan error, 1)
			go func() { currentDone <- oldServer.SetExternalTS(30, 100) }()
			select {
			case err := <-currentDone:
				re.NoError(err)
			case <-ctx.Done():
				t.Fatal("old setter blocked the replacement run")
			}
			before, err := oldServer.GetClient().Get(ctx, keypath.ExternalTimestampPath())
			re.NoError(err)
			re.Len(before.Kvs, 1)
			re.Equal("1e", string(before.Kvs[0].Value))
			release()
			select {
			case err := <-finished:
				re.ErrorIs(err, errs.ErrEtcdTxnConflict)
			case <-ctx.Done():
				t.Fatal("old setter did not return")
			}
			re.Equal(uint64(30), oldServer.GetExternalTS())
			re.Error(oldServer.SetExternalTS(25, 100))
			after, err := oldServer.GetClient().Get(ctx, keypath.ExternalTimestampPath())
			re.NoError(err)
			re.Len(after.Kvs, 1)
			re.Equal(before.Kvs[0].ModRevision, after.Kvs[0].ModRevision)
			re.Equal(before.Kvs[0].Value, after.Kvs[0].Value)
		})
	}
}
