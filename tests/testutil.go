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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	sc "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

var once sync.Once

// InitLogger initializes the logger for test.
func InitLogger(logConfig log.Config, logger *zap.Logger, logProps *log.ZapProperties, isRedactInfoLogEnabled bool) (err error) {
	once.Do(func() {
		// Setup the logger.
		err = logutil.SetupLogger(logConfig, &logger, &logProps, isRedactInfoLogEnabled)
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
	cfg, err := rm.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := rm.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// StartSingleTSOTestServerWithoutCheck creates and starts a tso server with default config for testing.
func StartSingleTSOTestServerWithoutCheck(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func(), error) {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
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
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

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
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store:  store,
	})
	re.NoError(err)

	storeInfo := grpcServer.GetRaftCluster().GetStore(store.GetId())
	newStore := storeInfo.Clone(core.SetStoreStats(&pdpb.StoreStats{
		Capacity:  uint64(10 * units.GiB),
		UsedSize:  uint64(9 * units.GiB),
		Available: uint64(1 * units.GiB),
	}))
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

type mode int

const (
	pdMode mode = iota
	apiMode
)

// SchedulingTestEnvironment is used for test purpose.
type SchedulingTestEnvironment struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *TestCluster
	opts    []ConfigOption
}

// NewSchedulingTestEnvironment is to create a new SchedulingTestEnvironment.
func NewSchedulingTestEnvironment(t *testing.T, opts ...ConfigOption) *SchedulingTestEnvironment {
	return &SchedulingTestEnvironment{
		t:    t,
		opts: opts,
	}
}

// RunTestInTwoModes is to run test in two modes.
func (s *SchedulingTestEnvironment) RunTestInTwoModes(test func(*TestCluster)) {
	s.RunTestInPDMode(test)
	s.RunTestInAPIMode(test)
}

// RunTestInPDMode is to run test in pd mode.
func (s *SchedulingTestEnvironment) RunTestInPDMode(test func(*TestCluster)) {
	s.t.Log("start to run test in pd mode")
	s.startCluster(pdMode)
	test(s.cluster)
	s.cleanup()
	s.t.Log("finish to run test in pd mode")
}

// RunTestInAPIMode is to run test in api mode.
func (s *SchedulingTestEnvironment) RunTestInAPIMode(test func(*TestCluster)) {
	s.t.Log("start to run test in api mode")
	re := require.New(s.t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	s.startCluster(apiMode)
	test(s.cluster)
	s.cleanup()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
	s.t.Log("finish to run test in api mode")
}

func (s *SchedulingTestEnvironment) cleanup() {
	s.cluster.Destroy()
	s.cancel()
}

func (s *SchedulingTestEnvironment) startCluster(m mode) {
	var err error
	re := require.New(s.t)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	switch m {
	case pdMode:
		s.cluster, err = NewTestCluster(s.ctx, 1, s.opts...)
		re.NoError(err)
		err = s.cluster.RunInitialServers()
		re.NoError(err)
		re.NotEmpty(s.cluster.WaitLeader())
		leaderServer := s.cluster.GetServer(s.cluster.GetLeader())
		re.NoError(leaderServer.BootstrapCluster())
	case apiMode:
		s.cluster, err = NewTestAPICluster(s.ctx, 1, s.opts...)
		re.NoError(err)
		err = s.cluster.RunInitialServers()
		re.NoError(err)
		re.NotEmpty(s.cluster.WaitLeader())
		leaderServer := s.cluster.GetServer(s.cluster.GetLeader())
		re.NoError(leaderServer.BootstrapCluster())
		// start scheduling cluster
		tc, err := NewTestSchedulingCluster(s.ctx, 1, leaderServer.GetAddr())
		re.NoError(err)
		tc.WaitForPrimaryServing(re)
		s.cluster.SetSchedulingCluster(tc)
		time.Sleep(200 * time.Millisecond) // wait for scheduling cluster to update member
	}
}
