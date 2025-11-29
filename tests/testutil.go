// Copyright 2023 TiKV Project Authors.
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

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	router "github.com/tikv/pd/pkg/mcs/router/server"
	rc "github.com/tikv/pd/pkg/mcs/router/server/config"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	sc "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

var (
	// TestDialClient is a http client for test.
	TestDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	testPortMutex sync.Mutex
	testPortMap   = make(map[string]struct{})
)

// SetRangePort sets the range of ports for test.
func SetRangePort(start, end int) {
	portRange := []int{start, end}
	dialContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{}
		randomPort := strconv.Itoa(rand.Intn(portRange[1]-portRange[0]) + portRange[0])
		testPortMutex.Lock()
		for range 10 {
			if _, ok := testPortMap[randomPort]; !ok {
				break
			}
			randomPort = strconv.Itoa(rand.Intn(portRange[1]-portRange[0]) + portRange[0])
		}
		testPortMutex.Unlock()
		localAddr, err := net.ResolveTCPAddr(network, "0.0.0.0:"+randomPort)
		if err != nil {
			return nil, err
		}
		dialer.LocalAddr = localAddr
		return dialer.DialContext(ctx, network, addr)
	}

	TestDialClient.Transport = &http.Transport{
		DisableKeepAlives: true,
		DialContext:       dialContext,
	}
}

var once sync.Once

// InitLogger initializes the logger for test.
func InitLogger(logConfig log.Config, logger *zap.Logger, logProps *log.ZapProperties, redactInfoLog logutil.RedactInfoLogType) (err error) {
	once.Do(func() {
		// Setup the logger.
		err = logutil.SetupLogger(&logConfig, &logger, &logProps, redactInfoLog)
		if err != nil {
			return
		}
		log.ReplaceGlobals(logger, logProps)
		// Flushing any buffered log entries.
		log.Sync()
	})
	return err
}

// StartSingleResourceManagerTestServer creates and starts a resource manager server with default config for testing.
func StartSingleResourceManagerTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*rm.Server, func()) {
	cfg := rm.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg.Name = cfg.ListenAddr
	cfg, err := rm.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := rm.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// StartSingleRouterServerWithoutCheck creates and starts a router server with default config for testing.
func StartSingleRouterServerWithoutCheck(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*router.Server, func(), error) {
	cfg := rc.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg.Name = cfg.ListenAddr
	cfg, err := router.GenerateConfig(cfg)
	re.NoError(err)
	s, cleanup, err := router.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup, nil
}

// StartSingleTSOTestServerWithoutCheck creates and starts a tso server with default config for testing.
func StartSingleTSOTestServerWithoutCheck(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func(), error) {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg.Name = cfg.ListenAddr
	cfg, err := tso.GenerateConfig(cfg)
	re.NoError(err)
	// Setup the logger.
	err = InitLogger(cfg.Log, cfg.Logger, cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	return NewTSOTestServer(ctx, cfg)
}

// StartSingleTSOTestServer creates and starts a tso server with default config for testing.
func StartSingleTSOTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func()) {
	s, cleanup, err := StartSingleTSOTestServerWithoutCheck(ctx, re, backendEndpoints, listenAddrs)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// NewTSOTestServer creates a tso server with given config for testing.
func NewTSOTestServer(ctx context.Context, cfg *tso.Config) (*tso.Server, testutil.CleanupFunc, error) {
	s := tso.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
	}
	return s, cleanup, nil
}

// StartSingleSchedulingTestServer creates and starts a scheduling server with default config for testing.
func StartSingleSchedulingTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*scheduling.Server, func()) {
	cfg := sc.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg.Name = cfg.ListenAddr
	cfg, err := scheduling.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := scheduling.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// NewSchedulingTestServer creates a scheduling server with given config for testing.
func NewSchedulingTestServer(ctx context.Context, cfg *sc.Config) (*scheduling.Server, testutil.CleanupFunc, error) {
	s := scheduling.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
	}
	return s, cleanup, nil
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary/leader
func WaitForPrimaryServing(re *require.Assertions, serverMap map[string]bs.Server) string {
	var primary string
	testutil.Eventually(re, func() bool {
		for name, s := range serverMap {
			if s.IsServing() {
				primary = name
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return primary
}

// MustPutStore is used for test purpose.
func MustPutStore(re *require.Assertions, tc *TestCluster, store *metapb.Store) {
	store.Address = fmt.Sprintf("mock://tikv-%d:%d", store.GetId(), store.GetId())
	if len(store.Version) == 0 {
		store.Version = versioninfo.MinSupportedVersion(versioninfo.Version2_0).String()
	}
	var raftCluster *cluster.RaftCluster
	// Make sure the raft cluster is ready, no matter if the leader is changed.
	testutil.Eventually(re, func() bool {
		leader := tc.GetLeaderServer()
		if leader == nil {
			return false
		}
		svr := leader.GetServer()
		if svr == nil {
			return false
		}
		raftCluster = svr.GetRaftCluster()
		// Wait for the raft cluster on the leader to be bootstrapped.
		return raftCluster != nil && raftCluster.IsRunning()
	})
	if store.LastHeartbeat == 0 {
		store.LastHeartbeat = time.Now().UnixNano()
	}
	re.NoError(raftCluster.PutMetaStore(store))
	ts := store.GetLastHeartbeat()
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	storeInfo := raftCluster.GetStore(store.GetId())
	newStore := storeInfo.Clone(
		core.SetStoreStats(&pdpb.StoreStats{
			Capacity:  uint64(10 * units.GiB),
			UsedSize:  uint64(9 * units.GiB),
			Available: uint64(1 * units.GiB),
		}),
		core.SetStoreState(store.GetState(), store.GetPhysicallyDestroyed()),
		core.SetNodeState(store.GetNodeState()),
		core.SetLastHeartbeatTS(time.Unix(ts/1e9, ts%1e9)),
	)
	raftCluster.GetBasicCluster().PutStore(newStore)
	raftCluster.UpdateAllStoreStatus()
	if tc.GetSchedulingPrimaryServer() != nil {
		tc.GetSchedulingPrimaryServer().GetCluster().PutStore(newStore)
		tc.GetSchedulingPrimaryServer().GetCluster().UpdateAllStoreStatus()
	}
}

// MustPutRegion is used for test purpose.
func MustPutRegion(re *require.Assertions, cluster *TestCluster, regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	opts = append(opts, core.SetSource(core.Heartbeat))
	r := core.NewRegionInfo(metaRegion, leader, opts...)
	MustPutRegionInfo(re, cluster, r)
	return r
}

// MustPutRegionInfo is used for test purpose.
func MustPutRegionInfo(re *require.Assertions, cluster *TestCluster, regionInfo *core.RegionInfo) {
	err := cluster.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)
	if cluster.GetSchedulingPrimaryServer() != nil {
		err = cluster.GetSchedulingPrimaryServer().GetCluster().HandleRegionHeartbeat(regionInfo)
		re.NoError(err)
	}
}

// MustHandleStoreHeartbeat is used for test purpose.
func MustHandleStoreHeartbeat(re *require.Assertions, cluster *TestCluster, heartbeat *pdpb.StoreHeartbeatRequest) {
	err := cluster.GetLeaderServer().GetRaftCluster().HandleStoreHeartbeat(heartbeat, &pdpb.StoreHeartbeatResponse{})
	re.NoError(err)
	if cluster.GetSchedulingPrimaryServer() != nil {
		hb := &schedulingpb.StoreHeartbeatRequest{
			Header: &schedulingpb.RequestHeader{
				ClusterId: heartbeat.Header.ClusterId,
			},
			Stats: heartbeat.GetStats(),
		}
		err = cluster.GetSchedulingPrimaryServer().GetCluster().HandleStoreHeartbeat(hb)
		re.NoError(err)
	}
}

// MustReportBuckets is used for test purpose.
func MustReportBuckets(re *require.Assertions, cluster *TestCluster, regionID uint64, start, end []byte, stats *metapb.BucketStats) *metapb.Buckets {
	buckets := &metapb.Buckets{
		RegionId: regionID,
		Version:  1,
		Keys:     [][]byte{start, end},
		Stats:    stats,
		// report buckets interval is 10s
		PeriodInMs: 10000,
	}
	err := cluster.HandleRegionBuckets(buckets)
	re.NoError(err)
	// TODO: forwards to scheduling server after it supports buckets
	return buckets
}

// Env is used for test purpose.
type Env int

const (
	// Both represents both scheduler environments.
	Both Env = iota
	// NonMicroserviceEnv represents non-microservice env.
	NonMicroserviceEnv
	// MicroserviceEnv represents microservice env.
	MicroserviceEnv
)

// SchedulingTestEnvironment is used for test purpose.
type SchedulingTestEnvironment struct {
	t        *testing.T
	opts     []ConfigOption
	clusters map[Env]*TestCluster
	cancels  []context.CancelFunc
	// only take effect in non-microservice env
	SkipBootstrap bool
	PDCount       int
	Env           Env
}

// NewSchedulingTestEnvironment is to create a new SchedulingTestEnvironment.
func NewSchedulingTestEnvironment(t *testing.T, opts ...ConfigOption) *SchedulingTestEnvironment {
	return &SchedulingTestEnvironment{
		t:        t,
		opts:     opts,
		clusters: make(map[Env]*TestCluster),
		cancels:  make([]context.CancelFunc, 0),
	}
}

// RunTest is to run test based on the environment.
// If env not set, it will run test in both non-microservice env and microservice env.
func (s *SchedulingTestEnvironment) RunTest(test func(*TestCluster)) {
	switch s.Env {
	case NonMicroserviceEnv:
		s.RunTestInNonMicroserviceEnv(test)
	case MicroserviceEnv:
		s.RunTestInMicroserviceEnv(test)
	default:
		s.RunTestInNonMicroserviceEnv(test)
		s.RunTestInMicroserviceEnv(test)
	}
}

// RunFunc runs a given function on the active test cluster.
// If env not set, it will run test in both non-microservice env and microservice env.
// It is used to run some helper functions.
func (s *SchedulingTestEnvironment) RunFunc(f func(*TestCluster)) {
	switch s.Env {
	case NonMicroserviceEnv:
		s.runFuncInNonMicroserviceEnv(f)
	case MicroserviceEnv:
		s.runFuncInMicroserviceEnv(f)
	default:
		s.runFuncInNonMicroserviceEnv(f)
		s.runFuncInMicroserviceEnv(f)
	}
}

// RunTestInNonMicroserviceEnv is to run test in non-microservice environment.
func (s *SchedulingTestEnvironment) RunTestInNonMicroserviceEnv(test func(*TestCluster)) {
	s.t.Logf("start test %s in non-microservice environment", getTestName())
	s.runFuncInNonMicroserviceEnv(test)
}

// RunTestInMicroserviceEnv is to run test in microservice environment.
func (s *SchedulingTestEnvironment) RunTestInMicroserviceEnv(test func(*TestCluster)) {
	s.t.Logf("start test %s in microservice environment", getTestName())
	s.runFuncInMicroserviceEnv(test)
}

// Reset is to reset the environment.
// It will reset stores, regions, rules and schedulers.
func (s *SchedulingTestEnvironment) Reset(re *require.Assertions) {
	resetFunc := func(cluster *TestCluster) {
		urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
		leaderServer := cluster.GetLeaderServer()
		rc := leaderServer.GetRaftCluster()
		// replace rules with default rule
		configURL := fmt.Sprintf("%s/config", urlPrefix)
		reqData, e := json.Marshal(map[string]any{
			"enable-placement-rules": "true",
		})
		re.NoError(e)
		err := testutil.CheckPostJSON(TestDialClient, configURL, reqData, testutil.StatusOK(re))
		re.NoError(err)
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			// wait for the scheduling server to update the config
			testutil.Eventually(re, func() bool {
				return sche.GetCluster().GetCheckerConfig().IsPlacementRulesEnabled()
			})
		}
		defaultRule := placement.GroupBundle{
			ID: placement.DefaultGroupID,
			Rules: []*placement.Rule{
				{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3},
			},
		}
		data, err := json.Marshal([]placement.GroupBundle{defaultRule})
		re.NoError(err)
		ruleURL := fmt.Sprintf("%s/config/placement-rule", urlPrefix)
		err = testutil.CheckPostJSON(TestDialClient, ruleURL, data, testutil.StatusOK(re))
		re.NoError(err)
		respBundle := make([]placement.GroupBundle, 0)
		testutil.Eventually(re, func() bool {
			err = testutil.CheckGetJSON(TestDialClient, ruleURL, nil,
				testutil.StatusOK(re), testutil.ExtractJSON(re, &respBundle))
			re.NoError(err)
			return len(respBundle) == 1 && respBundle[0].ID == placement.DefaultGroupID && len(respBundle[0].Rules) == 1 &&
				respBundle[0].Rules[0].ID == placement.DefaultRuleID && respBundle[0].Rules[0].Count == 3 && respBundle[0].Rules[0].Role == placement.Voter
		})
		// clean region storage and cache
		for _, region := range leaderServer.GetRegions() {
			url := fmt.Sprintf("%s/admin/storage/region/%d", urlPrefix, region.GetID())
			err = testutil.CheckDelete(TestDialClient, url, testutil.StatusOK(re))
			re.NoError(err)
		}
		re.Empty(leaderServer.GetRegions())
		// clean stores
		for _, store := range leaderServer.GetStores() {
			if store.NodeState == metapb.NodeState_Removed {
				continue
			}
			err := rc.RemoveStore(store.GetId(), true)
			if err != nil {
				re.ErrorIs(err, errs.ErrStoreRemoved)
			}
			re.NoError(rc.BuryStore(store.GetId(), true))
		}
		re.NoError(rc.RemoveTombStoneRecords())
		re.Empty(leaderServer.GetStores())
		testutil.Eventually(re, func() bool {
			if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
				for _, s := range sche.GetBasicCluster().GetStores() {
					if s.GetState() != metapb.StoreState_Tombstone {
						return false
					}
				}
			}
			return true
		})
		// clean schedulers
		schedulerURL := fmt.Sprintf("%s/schedulers", urlPrefix)
		testutil.Eventually(re, func() bool {
			// get current schedulers
			var currentSchedulers []string
			err := testutil.CheckGetJSON(http.DefaultClient, schedulerURL, nil,
				testutil.StatusOK(re), testutil.ExtractJSON(re, &currentSchedulers))
			re.NoError(err)
			// compare schedulers
			defaultSet := make(map[string]struct{}, len(types.DefaultSchedulers))
			for _, s := range types.DefaultSchedulers {
				defaultSet[s.String()] = struct{}{}
			}
			currentSet := make(map[string]struct{}, len(currentSchedulers))
			for _, s := range currentSchedulers {
				currentSet[s] = struct{}{}
			}
			var toAdd, toRemove []string
			for name := range defaultSet {
				if _, ok := currentSet[name]; !ok {
					toAdd = append(toAdd, name)
				}
			}
			for name := range currentSet {
				if _, ok := defaultSet[name]; !ok {
					toRemove = append(toRemove, name)
				}
			}
			if len(toAdd) == 0 && len(toRemove) == 0 {
				return true
			}
			// sync schedulers
			for _, name := range toAdd {
				input := map[string]any{"name": name}
				body, err := json.Marshal(input)
				re.NoError(err)
				err = testutil.CheckPostJSON(http.DefaultClient, schedulerURL, body)
				re.NoError(err)
			}
			for _, name := range toRemove {
				err = testutil.CheckDelete(http.DefaultClient, schedulerURL+"/"+name)
				re.NoError(err)
			}
			return false
		})
		// clean hot cache
		hotStat := leaderServer.GetRaftCluster().GetHotStat()
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			hotStat = sche.GetCluster().GetHotStat()
		}
		hotStat.CleanCache()
		// clean operators
		operatorURL := fmt.Sprintf("%s/operators", urlPrefix)
		err = testutil.CheckDelete(TestDialClient, operatorURL, testutil.StatusOK(re))
		re.NoError(err)
		testutil.Eventually(re, func() bool {
			var operators []*operator.Operator
			err := testutil.CheckGetJSON(TestDialClient, operatorURL, nil,
				testutil.StatusOK(re), testutil.ExtractJSON(re, &operators))
			re.NoError(err)
			return len(operators) == 0
		})
		rc.GetOperatorController().CleanAllOpRecords()
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			sche.GetCoordinator().GetOperatorController().CleanAllOpRecords()
		}
		// clean pending processed regions
		rc.GetCoordinator().GetCheckerController().ClearPendingProcessedRegions()
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			sche.GetCoordinator().GetCheckerController().ClearPendingProcessedRegions()
		}
		// reset id allocator
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = leaderServer.GetServer().RecoverAllocID(ctx, 0)
		re.NoError(err)
	}
	s.RunFunc(resetFunc)
}

// Cleanup is to cleanup the environment.
func (s *SchedulingTestEnvironment) Cleanup() {
	for _, cluster := range s.clusters {
		cluster.Destroy()
	}
	for _, cancel := range s.cancels {
		cancel()
	}
}

func (s *SchedulingTestEnvironment) runFuncInNonMicroserviceEnv(f func(*TestCluster)) {
	if _, ok := s.clusters[NonMicroserviceEnv]; !ok {
		s.startCluster(NonMicroserviceEnv)
	}
	f(s.clusters[NonMicroserviceEnv])
}

func (s *SchedulingTestEnvironment) runFuncInMicroserviceEnv(f func(*TestCluster)) {
	re := require.New(s.t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	}()
	if _, ok := s.clusters[MicroserviceEnv]; !ok {
		s.startCluster(MicroserviceEnv)
	}
	f(s.clusters[MicroserviceEnv])
}

func (s *SchedulingTestEnvironment) startCluster(m Env) {
	re := require.New(s.t)
	ctx, cancel := context.WithCancel(context.Background())
	s.cancels = append(s.cancels, cancel)
	if s.PDCount == 0 {
		s.PDCount = 1
	}
	switch m {
	case NonMicroserviceEnv:
		cluster, err := NewTestCluster(ctx, s.PDCount, s.opts...)
		re.NoError(err)
		err = cluster.RunInitialServers()
		re.NoError(err)
		re.NotEmpty(cluster.WaitLeader())
		leaderServer := cluster.GetServer(cluster.GetLeader())
		if !s.SkipBootstrap {
			re.NoError(leaderServer.BootstrapCluster())
		}
		s.clusters[NonMicroserviceEnv] = cluster
	case MicroserviceEnv:
		cluster, err := NewTestClusterWithKeyspaceGroup(ctx, s.PDCount, s.opts...)
		re.NoError(err)
		err = cluster.RunInitialServers()
		re.NoError(err)
		re.NotEmpty(cluster.WaitLeader())
		leaderServer := cluster.GetServer(cluster.GetLeader())
		re.NoError(leaderServer.BootstrapCluster())
		leaderServer.GetRaftCluster().SetPrepared()
		// start scheduling cluster
		tc, err := NewTestSchedulingCluster(ctx, 1, cluster)
		re.NoError(err)
		tc.WaitForPrimaryServing(re)
		tc.GetPrimaryServer().GetCluster().SetPrepared()
		cluster.SetSchedulingCluster(tc)
		testutil.Eventually(re, func() bool {
			return cluster.GetLeaderServer().GetServer().IsServiceIndependent(constant.SchedulingServiceName)
		})
		s.clusters[MicroserviceEnv] = cluster
	}
}

func getTestName() string {
	pc, _, _, _ := runtime.Caller(2)
	caller := runtime.FuncForPC(pc)
	if caller == nil || strings.Contains(caller.Name(), "RunTest") {
		pc, _, _, _ = runtime.Caller(3)
		caller = runtime.FuncForPC(pc)
	}
	if caller != nil {
		elements := strings.Split(caller.Name(), ".")
		return elements[len(elements)-1]
	}
	return ""
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	v, _, _ := i.allocator.Alloc(1)
	return v
}

// InitRegions is used for test purpose.
func InitRegions(regionLen int) []*core.RegionInfo {
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := range regionLen {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers: []*metapb.Peer{
				{Id: allocator.alloc(), StoreId: uint64(1)},
				{Id: allocator.alloc(), StoreId: uint64(2)},
				{Id: allocator.alloc(), StoreId: uint64(3)},
			},
		}
		switch i {
		case 0:
			r.StartKey = []byte{}
		case regionLen - 1:
			r.EndKey = []byte{}
		default:
		}
		region := core.NewRegionInfo(r, r.Peers[0], core.SetSource(core.Heartbeat))
		// Here is used to simulate the upgrade process.
		if i < regionLen/2 {
			buckets := &metapb.Buckets{
				RegionId: r.Id,
				Keys:     [][]byte{r.StartKey, r.EndKey},
				Version:  1,
			}
			region.UpdateBuckets(buckets, region.GetBuckets())
		} else {
			region.UpdateBuckets(&metapb.Buckets{}, region.GetBuckets())
		}
		regions = append(regions, region)
	}
	return regions
}

var logOnce sync.Once

// NewTestSingleConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func NewTestSingleConfig(c *assertutil.Checker) *config.Config {
	schedulers.Register()
	cfg := &config.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     10,
		TSOSaveInterval: typeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = os.MkdirTemp("", "pd_tests")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.DisableStrictReconfigCheck = true
	cfg.TickInterval = typeutil.NewDuration(100 * time.Millisecond)
	cfg.ElectionInterval = typeutil.NewDuration(3 * time.Second)
	cfg.LeaderPriorityCheckInterval = typeutil.NewDuration(100 * time.Millisecond)
	err := logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	c.AssertNil(err)
	logOnce.Do(func() {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	})

	c.AssertNil(cfg.Adjust(nil, false))
	cfg.Keyspace.WaitRegionSplit = false

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because PD client also needs this, so export here.
func NewTestMultiConfig(c *assertutil.Checker, count int) []*config.Config {
	cfgs := make([]*config.Config, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig(c)
		cfg.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}

// NewServer creates a pd server for testing.
func NewServer(re *require.Assertions, c *assertutil.Checker) (*server.Server, testutil.CleanupFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := NewTestSingleConfig(c)
	mockHandler := CreateMockHandler(re, "127.0.0.1")
	s, err := server.CreateServer(ctx, cfg, nil, mockHandler)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if err = s.Run(); err != nil {
		cancel()
		return nil, nil, err
	}

	cleanup := func() {
		cancel()
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// MustWaitLeader return the leader until timeout.
func MustWaitLeader(re *require.Assertions, svrs []*server.Server) *server.Server {
	var leader *server.Server
	testutil.Eventually(re, func() bool {
		for _, svr := range svrs {
			// All servers' GetLeader should return the same leader.
			if svr.GetLeader() == nil || (leader != nil && svr.GetLeader().GetMemberId() != leader.GetLeader().GetMemberId()) {
				return false
			}
			if leader == nil && !svr.IsClosed() {
				leader = svr
			}
		}
		return true
	})
	return leader
}

// CreateMockHandler creates a mock handler for test.
func CreateMockHandler(re *require.Assertions, ip string) server.HandlerBuilder {
	return func(context.Context, *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/apis/mock/v1/hello", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello World")
			// test getting ip
			clientIP, _ := apiutil.GetIPPortFromHTTPRequest(r)
			re.Equal(ip, clientIP)
		})
		info := apiutil.APIServiceGroup{
			Name:    "mock",
			Version: "v1",
		}
		return mux, info, nil
	}
}

const (
	schedulersPrefix      = "/pd/api/v1/schedulers"
	schedulerConfigPrefix = "/pd/api/v1/scheduler-config"
)

// MustAddScheduler adds a scheduler with HTTP API.
func MustAddScheduler(
	re *require.Assertions, serverAddr string,
	schedulerName string, args map[string]any,
) {
	request := map[string]any{
		"name": schedulerName,
	}
	for arg, val := range args {
		request[arg] = val
	}
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s%s", serverAddr, schedulersPrefix), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustDeleteScheduler deletes a scheduler with HTTP API.
func MustDeleteScheduler(re *require.Assertions, serverAddr, schedulerName string) {
	httpReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s%s/%s", serverAddr, schedulersPrefix, schedulerName), http.NoBody)
	re.NoError(err)
	resp, err := TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustCallSchedulerConfigAPI calls a scheduler config with HTTP API with the given args.
func MustCallSchedulerConfigAPI(
	re *require.Assertions,
	method, serverAddr, schedulerName string, args []string,
	input map[string]any,
) {
	data, err := json.Marshal(input)
	re.NoError(err)
	args = append([]string{schedulerConfigPrefix, schedulerName}, args...)
	httpReq, err := http.NewRequest(method, fmt.Sprintf("%s%s", serverAddr, path.Join(args...)), bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// NewResourceManagerTestServer creates a resource manager server with given config for testing.
func NewResourceManagerTestServer(ctx context.Context, cfg *rm.Config) (*rm.Server, testutil.CleanupFunc, error) {
	s := rm.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
	}
	return s, cleanup, nil
}
