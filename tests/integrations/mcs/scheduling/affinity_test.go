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

package scheduling

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestAffinityFirstWinTargetWriteUnavailable(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const groupID = "c08-target-write"
	const targetsPath = "/pd/api/v2/affinity-groups/targets"

	// Install before starting servers, since their clients can already be in use
	// once scheduling becomes primary. Only targets use the proxy; heartbeats,
	// etcd watches and other HTTP requests keep their route.
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	var blocked atomic.Bool
	var failures atomic.Int64
	blocked.Store(true)
	forward := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.Out.URL = r.In.URL }, Transport: original}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			failures.Add(1)
			http.Error(w, "C08 target writes blocked", http.StatusServiceUnavailable)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	re.NoError(err)
	transport.Proxy = func(r *http.Request) (*url.URL, error) {
		if r.Method == http.MethodPost && r.URL.Path == targetsPath {
			return proxyURL, nil
		}
		if original.(*http.Transport).Proxy != nil {
			return original.(*http.Transport).Proxy(r)
		}
		return nil, nil
	}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = original
		transport.CloseIdleConnections()
	})
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker")) })
	pd, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(c *config.Config, _ string) {
		c.Schedule.AffinityScheduleLimit = 4
	})
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	re.NotEmpty(pd.WaitLeader())
	leader := pd.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	tc, err := tests.NewTestSchedulingCluster(ctx, 1, pd)
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	primary := tc.WaitForPrimaryServing(re)
	pd.SetSchedulingCluster(tc)
	cluster := primary.GetCluster()
	cluster.SetPrepared()
	for id := uint64(1); id <= 3; id++ {
		tests.MustPutStore(re, pd, &metapb.Store{Id: id, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})
	}
	pdManager, err := leader.GetServer().GetAffinityManager()
	re.NoError(err)
	// Avoid bootstrap keyspace label boundaries, which would make the split
	// checker wait for the simulated TiKV to execute a split before first-win.
	re.NoError(pdManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("b"), EndKey: []byte("c")}},
	}}))
	manager := cluster.GetAffinityManager()
	testutil.Eventually(re, func() bool {
		g := manager.GetAffinityGroupState(groupID)
		return g != nil && g.RangeCount == 1 && cluster.GetStore(3) != nil
	})
	observerCore, logs := observer.New(zap.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(observerCore), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.WarnLevel)})
	t.Cleanup(restore)
	counter := func() float64 {
		metrics, err := prometheus.DefaultGatherer.Gather()
		re.NoError(err)
		for _, metric := range metrics {
			if metric.GetName() == "pd_affinity_target_write_failures_total" {
				return metric.GetMetric()[0].GetCounter().GetValue()
			}
		}
		re.FailNow("target write failure metric not registered")
		return 0
	}
	before := counter()

	client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
	t.Cleanup(func() { re.NoError(conn.Close()) })
	stream, err := client.RegionHeartbeat(ctx)
	re.NoError(err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
	heartbeat := func() {
		re.NoError(stream.Send(&pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Region: &metapb.Region{Id: 100, StartKey: []byte("b"), EndKey: []byte("c"),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}, Peers: peers},
			Leader: peers[0], ApproximateSize: 1, ApproximateKeys: 1,
		}))
	}
	loadStored := func() *affinity.Group {
		value, err := leader.GetServer().GetStorage().LoadAffinityGroup(groupID)
		re.NoError(err)
		var group affinity.Group
		re.NoError(json.Unmarshal([]byte(value), &group))
		return &group
	}
	testutil.Eventually(re, func() bool {
		heartbeat()
		return failures.Load() > 0 && counter() > before &&
			logs.FilterLevelExact(zap.WarnLevel).FilterMessage("failed to write affinity group target").FilterField(zap.String("group-id", groupID)).Len() > 0
	})
	re.NotNil(cluster.GetRegion(100), "Region heartbeats must still reach scheduling")
	response, err := primary.GetHTTPClient().Get(leader.GetAddr() + "/health")
	re.NoError(err)
	re.NoError(response.Body.Close())
	re.Equal(http.StatusOK, response.StatusCode, "other scheduling-to-PD HTTP requests must still work")
	for _, group := range []*affinity.GroupState{pdManager.GetAffinityGroupState(groupID), manager.GetAffinityGroupState(groupID)} {
		re.NotNil(group)
		re.Zero(group.LeaderStoreID, "failed target writes must not be published")
		re.Empty(group.VoterStoreIDs)
		re.False(group.AffinitySchedulingAllowed)
		re.Equal(affinity.PhasePending, group.Phase)
	}
	re.Zero(loadStored().LeaderStoreID)
	re.Empty(loadStored().VoterStoreIDs)

	blocked.Store(false)
	testutil.Eventually(re, func() bool {
		heartbeat()
		stored := loadStored()
		pdGroup := pdManager.GetAffinityGroupState(groupID)
		schedulingGroup := manager.GetAffinityGroupState(groupID)
		return stored.LeaderStoreID == 1 && len(stored.VoterStoreIDs) == 3 &&
			pdGroup != nil && schedulingGroup != nil && pdGroup.AffinitySchedulingAllowed && schedulingGroup.AffinitySchedulingAllowed &&
			stored.String() == pdGroup.String() && stored.String() == schedulingGroup.String()
	}, testutil.WithWaitFor(30*time.Second))
	re.Equal([]uint64{1, 2, 3}, loadStored().VoterStoreIDs)
}
