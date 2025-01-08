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
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	sc "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
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
		err = logutil.SetupLogger(logConfig, &logger, &logProps, redactInfoLog)
		if err != nil {
			return
		}
		log.ReplaceGlobals(logger, logProps)
		// Flushing any buffered log entries.
		log.Sync()
	})
	return err
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
		os.RemoveAll(cfg.DataDir)
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
		os.RemoveAll(cfg.DataDir)
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
func MustPutStore(re *require.Assertions, cluster *TestCluster, store *metapb.Store) {
	store.Address = fmt.Sprintf("tikv%d", store.GetId())
	if len(store.Version) == 0 {
		store.Version = versioninfo.MinSupportedVersion(versioninfo.Version2_0).String()
	}
	svr := cluster.GetLeaderServer().GetServer()
	grpcServer := &server.GrpcServer{Server: svr}
	_, err := grpcServer.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Store:  store,
	})
	re.NoError(err)

	ts := store.GetLastHeartbeat()
	if ts == 0 {
		ts = time.Now().UnixNano()
	}
	storeInfo := grpcServer.GetRaftCluster().GetStore(store.GetId())
	newStore := storeInfo.Clone(
		core.SetStoreStats(&pdpb.StoreStats{
			Capacity:  uint64(10 * units.GiB),
			UsedSize:  uint64(9 * units.GiB),
			Available: uint64(1 * units.GiB),
		}),
		core.SetLastHeartbeatTS(time.Unix(ts/1e9, ts%1e9)),
	)
	grpcServer.GetRaftCluster().GetBasicCluster().PutStore(newStore)
	if cluster.GetSchedulingPrimaryServer() != nil {
		cluster.GetSchedulingPrimaryServer().GetCluster().PutStore(newStore)
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
	err := cluster.HandleReportBuckets(buckets)
	re.NoError(err)
	// TODO: forwards to scheduling server after it supports buckets
	return buckets
}

// SchedulerMode is used for test purpose.
type SchedulerMode int

const (
	// Both represents both PD mode and API mode.
	Both SchedulerMode = iota
	// PDMode represents PD mode.
	PDMode
	// PDServiceMode represents API mode.
	PDServiceMode
)

// SchedulingTestEnvironment is used for test purpose.
type SchedulingTestEnvironment struct {
	t        *testing.T
	opts     []ConfigOption
	clusters map[SchedulerMode]*TestCluster
	cancels  []context.CancelFunc
	RunMode  SchedulerMode
}

// NewSchedulingTestEnvironment is to create a new SchedulingTestEnvironment.
func NewSchedulingTestEnvironment(t *testing.T, opts ...ConfigOption) *SchedulingTestEnvironment {
	return &SchedulingTestEnvironment{
		t:        t,
		opts:     opts,
		clusters: make(map[SchedulerMode]*TestCluster),
		cancels:  make([]context.CancelFunc, 0),
	}
}

// RunTestBasedOnMode runs test based on mode.
// If mode not set, it will run test in both PD mode and API mode.
func (s *SchedulingTestEnvironment) RunTestBasedOnMode(test func(*TestCluster)) {
	switch s.RunMode {
	case PDMode:
		s.RunTestInPDMode(test)
	case PDServiceMode:
		s.RunTestInPDServiceMode(test)
	default:
		s.RunTestInPDMode(test)
		s.RunTestInPDServiceMode(test)
	}
}

// RunTestInPDMode is to run test in pd mode.
func (s *SchedulingTestEnvironment) RunTestInPDMode(test func(*TestCluster)) {
	s.t.Logf("start test %s in pd mode", getTestName())
	if _, ok := s.clusters[PDMode]; !ok {
		s.startCluster(PDMode)
	}
	test(s.clusters[PDMode])
}

func getTestName() string {
	pc, _, _, _ := runtime.Caller(2)
	caller := runtime.FuncForPC(pc)
	if caller == nil || strings.Contains(caller.Name(), "RunTestBasedOnMode") {
		pc, _, _, _ = runtime.Caller(3)
		caller = runtime.FuncForPC(pc)
	}
	if caller != nil {
		elements := strings.Split(caller.Name(), ".")
		return elements[len(elements)-1]
	}
	return ""
}

// RunTestInPDServiceMode is to run test in pd service mode.
func (s *SchedulingTestEnvironment) RunTestInPDServiceMode(test func(*TestCluster)) {
	re := require.New(s.t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	}()
	s.t.Logf("start test %s in pd service mode", getTestName())
	if _, ok := s.clusters[PDServiceMode]; !ok {
		s.startCluster(PDServiceMode)
	}
	test(s.clusters[PDServiceMode])
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

func (s *SchedulingTestEnvironment) startCluster(m SchedulerMode) {
	re := require.New(s.t)
	ctx, cancel := context.WithCancel(context.Background())
	s.cancels = append(s.cancels, cancel)
	switch m {
	case PDMode:
		cluster, err := NewTestCluster(ctx, 1, s.opts...)
		re.NoError(err)
		err = cluster.RunInitialServers()
		re.NoError(err)
		re.NotEmpty(cluster.WaitLeader())
		leaderServer := cluster.GetServer(cluster.GetLeader())
		re.NoError(leaderServer.BootstrapCluster())
		s.clusters[PDMode] = cluster
	case PDServiceMode:
		cluster, err := NewTestPDServiceCluster(ctx, 1, s.opts...)
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
		time.Sleep(200 * time.Millisecond) // wait for scheduling cluster to update member
		testutil.Eventually(re, func() bool {
			return cluster.GetLeaderServer().GetServer().IsServiceIndependent(constant.SchedulingServiceName)
		})
		s.clusters[PDServiceMode] = cluster
	}
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	v, _ := i.allocator.Alloc()
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
		if i == 0 {
			r.StartKey = []byte{}
		} else if i == regionLen-1 {
			r.EndKey = []byte{}
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
		}
		regions = append(regions, region)
	}
	return regions
}
