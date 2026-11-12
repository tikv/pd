<<<<<<< HEAD
// Copyright 2016 TiKV Project Authors.
=======
// Copyright 2026 TiKV Project Authors.
>>>>>>> 8179ce7c2d (server: resync follower region cache reset (#10689))
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

package server

import (
	"context"
<<<<<<< HEAD
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type leaderServerTestSuite struct {
	suite.Suite

	ctx        context.Context
	cancel     context.CancelFunc
	svrs       map[string]*Server
	leaderPath string
}

func TestLeaderServerTestSuite(t *testing.T) {
	suite.Run(t, new(leaderServerTestSuite))
}

func (suite *leaderServerTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.svrs = make(map[string]*Server)

	cfgs := NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), 3)

	ch := make(chan *Server, 3)
	for i := range 3 {
		cfg := cfgs[i]

		go func() {
			mockHandler := CreateMockHandler(re, "127.0.0.1")
			svr, err := CreateServer(suite.ctx, cfg, nil, mockHandler)
			re.NoError(err)
			err = svr.Run()
			re.NoError(err)
			ch <- svr
		}()
	}

	for range 3 {
		svr := <-ch
		suite.svrs[svr.GetAddr()] = svr
		suite.leaderPath = svr.GetMember().GetLeaderPath()
	}
}

func (suite *leaderServerTestSuite) TearDownSuite() {
	suite.cancel()
	for _, svr := range suite.svrs {
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}
}

func newTestServersWithCfgs(
	ctx context.Context,
	cfgs []*config.Config,
	re *require.Assertions,
) ([]*Server, testutil.CleanupFunc) {
	svrs := make([]*Server, 0, len(cfgs))

	ch := make(chan *Server)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			mockHandler := CreateMockHandler(re, "127.0.0.1")
			svr, err := CreateServer(ctx, cfg, nil, mockHandler)
			// prevent blocking if Asserts fails
			failed := true
			defer func() {
				if failed {
					ch <- nil
				} else {
					ch <- svr
				}
			}()
			re.NoError(err)
			err = svr.Run()
			re.NoError(err)
			failed = false
		}(cfg)
	}

	for range cfgs {
		svr := <-ch
		re.NotNil(svr)
		svrs = append(svrs, svr)
	}
	MustWaitLeader(re, svrs)

	cleanup := func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return svrs, cleanup
}

func (suite *leaderServerTestSuite) TestRegisterServerHandler() {
	re := suite.Require()
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	mockHandler := CreateMockHandler(re, "127.0.0.1")
	svr, err := CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	re.NoError(err)
	resp, err := http.Get(fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()))
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderForwarded() {
	re := suite.Require()
	mockHandler := CreateMockHandler(re, "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XForwardedForHeader, "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderXReal() {
	re := suite.Require()
	mockHandler := CreateMockHandler(re, "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XRealIPHeader, "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderBoth() {
	re := suite.Require()
	mockHandler := CreateMockHandler(re, "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XForwardedForHeader, "127.0.0.2")
	req.Header.Add(apiutil.XRealIPHeader, "127.0.0.3")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func TestAPIService(t *testing.T) {
	re := require.New(t)

	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	defer testutil.CleanServer(cfg.DataDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockHandler := CreateMockHandler(re, "127.0.0.1")
	svr, err := CreateServer(ctx, cfg, []string{constant.APIServiceName}, mockHandler)
	re.NoError(err)
	defer svr.Close()
	err = svr.Run()
	re.NoError(err)
	MustWaitLeader(re, []*Server{svr})
	re.True(svr.IsAPIServiceMode())
}

func TestIsPathInDirectory(t *testing.T) {
	re := require.New(t)
	fileName := "test"
	directory := "/root/project"
	path := filepath.Join(directory, fileName)
	re.True(isPathInDirectory(path, directory))

	fileName = filepath.Join("..", "..", "test")
	path = filepath.Join(directory, fileName)
	re.False(isPathInDirectory(path, directory))
}

func TestCheckClusterID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgs := NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), 2)
	for _, cfg := range cfgs {
		cfg.DataDir, _ = os.MkdirTemp("", "pd_tests")
		// Clean up before testing.
		testutil.CleanServer(cfg.DataDir)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster.
	svrsA, cleanA := newTestServersWithCfgs(ctx, []*config.Config{cfgA}, re)
	defer cleanA()
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := newTestServersWithCfgs(ctx, []*config.Config{cfgB}, re)
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	mockHandler := CreateMockHandler(re, "127.0.0.1")
	svr, err := CreateServer(ctx, cfgA, nil, mockHandler)
	re.NoError(err)

	etcd, err := embed.StartEtcd(svr.etcdCfg)
	re.NoError(err)
	urlsMap, err := etcdtypes.NewURLsMap(svr.cfg.InitialCluster)
	re.NoError(err)
	tlsConfig, err := svr.cfg.Security.ToClientTLSConfig()
	re.NoError(err)
	err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlsMap, tlsConfig)
	re.Error(err)
	etcd.Close()
	testutil.CleanServer(cfgA.DataDir)
=======
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

var errTestFollowerRegionStorage = stderrors.New("test follower region storage error")

func TestResetFollowerRegionCacheRequiresRegionStorage(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	cfg.PDServerCfg.UseRegionStorage = false
	s := &Server{persistOptions: config.NewPersistOptions(cfg)}

	re.ErrorContains(s.ResetFollowerRegionCache(), "region storage is disabled")

	cfg.PDServerCfg.UseRegionStorage = true
	s = newTestFollowerRegionResetServer(context.Background())
	s.persistOptions = config.NewPersistOptions(cfg)
	s.member = member.NewMember(nil, nil, 1)
	re.Error(s.ResetFollowerRegionCache())
}

func TestDeleteFollowerRegion(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*require.Assertions, *Server) uint64
		errContains string
		check       func(*require.Assertions, *Server, uint64)
	}{
		{
			name: "cached region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(1)
				re.NoError(s.storage.SaveRegion(region))
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "storage-only region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(2)
				re.NoError(s.storage.SaveRegion(region))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "missing region",
			setup: func(*require.Assertions, *Server) uint64 {
				return 3
			},
		},
		{
			name: "load storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				s.storage = &testFollowerRegionStorage{
					Storage:       s.storage,
					loadRegionErr: errTestFollowerRegionStorage,
				}
				return 4
			},
			errContains: "load follower region from local storage",
		},
		{
			name: "delete storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(5)
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				s.storage = &testFollowerRegionStorage{
					Storage:         s.storage,
					deleteRegionErr: errTestFollowerRegionStorage,
				}
				return region.GetId()
			},
			errContains: "delete follower region from local storage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			s := newTestFollowerRegionResetServer(context.Background())
			regionID := test.setup(re, s)

			err := s.deleteFollowerRegion(regionID)
			if test.errContains != "" {
				re.ErrorContains(err, test.errContains)
				return
			}
			re.NoError(err)
			if test.check != nil {
				test.check(re, s, regionID)
			}
		})
	}
}

func TestDeleteFollowerRegionStorage(t *testing.T) {
	tests := []struct {
		name        string
		ctx         context.Context
		setup       func(*require.Assertions, *Server) func(*require.Assertions, *Server)
		errIs       error
		errContains string
	}{
		{
			name: "deletes all region storage keys",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				regions := []*metapb.Region{
					newTestFollowerRegionMeta(10),
					newTestFollowerRegionMeta(11),
				}
				for _, region := range regions {
					re.NoError(s.storage.SaveRegion(region))
				}
				return func(re *require.Assertions, s *Server) {
					for _, region := range regions {
						assertTestFollowerRegionDeleted(re, s, region.GetId())
					}
				}
			},
		},
		{
			name: "deletes by key without loading region meta",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				region := newTestFollowerRegionMeta(12)
				re.NoError(s.storage.SaveRegion(region))
				regionStorage := s.storage
				s.storage = &testFollowerRegionStorage{
					Storage:       regionStorage,
					loadRegionErr: errTestFollowerRegionStorage,
				}
				return func(re *require.Assertions, s *Server) {
					s.storage = regionStorage
					assertTestFollowerRegionDeleted(re, s, region.GetId())
				}
			},
		},
		{
			name: "context canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			errIs: context.Canceled,
		},
		{
			name: "load range error",
			setup: func(_ *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				s.storage = &testFollowerRegionStorage{
					Storage:      s.storage,
					loadRangeErr: errTestFollowerRegionStorage,
				}
				return nil
			},
			errContains: "load follower regions from local storage",
		},
		{
			name: "delete transaction error",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				region := newTestFollowerRegionMeta(21)
				re.NoError(s.storage.SaveRegion(region))
				s.storage = &testFollowerRegionStorage{
					Storage:     s.storage,
					runInTxnErr: errTestFollowerRegionStorage,
				}
				return nil
			},
			errContains: "delete follower regions from local storage",
		},
		{
			name: "invalid region storage key",
			setup: func(_ *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				s.storage = &testFollowerRegionStorage{
					Storage:       s.storage,
					loadRangeKeys: []string{"invalid-region-key"},
				}
				return nil
			},
			errContains: "invalid region storage key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			ctx := test.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			s := newTestFollowerRegionResetServer(ctx)
			var check func(*require.Assertions, *Server)
			if test.setup != nil {
				check = test.setup(re, s)
			}

			err := s.deleteFollowerRegionStorage()
			switch {
			case test.errIs != nil:
				re.ErrorIs(err, test.errIs)
				return
			case test.errContains != "":
				re.ErrorContains(err, test.errContains)
				return
			default:
				re.NoError(err)
			}
			if check != nil {
				check(re, s)
			}
		})
	}
}

func TestParseRegionIDFromStorageKey(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		regionID    uint64
		errContains string
	}{
		{
			name:     "valid region key",
			key:      keypath.RegionPath(123),
			regionID: 123,
		},
		{
			name:        "missing region id",
			key:         "invalid-region-key",
			errContains: "invalid region storage key",
		},
		{
			name:        "invalid region id",
			key:         "/pd/0/raft/r/not-a-number",
			errContains: "parse region storage key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			regionID, err := parseRegionIDFromStorageKey(test.key)
			if test.errContains != "" {
				re.ErrorContains(err, test.errContains)
				return
			}
			re.NoError(err)
			re.Equal(test.regionID, regionID)
		})
	}
}

func newTestFollowerRegionResetServer(ctx context.Context) *Server {
	cfg := config.NewConfig()
	return &Server{
		ctx:          ctx,
		cfg:          cfg,
		storage:      storage.NewStorageWithMemoryBackend(),
		basicCluster: core.NewBasicCluster(),
	}
}

func newTestFollowerRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte{byte(regionID)},
		EndKey:      []byte{byte(regionID + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{
			{Id: regionID*10 + 1, StoreId: 1},
		},
	}
}

func assertTestFollowerRegionDeleted(re *require.Assertions, s *Server, regionID uint64) {
	region := &metapb.Region{}
	ok, err := s.storage.LoadRegion(regionID, region)
	re.NoError(err)
	re.False(ok)
	re.Nil(s.basicCluster.GetRegion(regionID))
}

type testFollowerRegionStorage struct {
	storage.Storage
	loadRegionErr   error
	deleteRegionErr error
	loadRangeErr    error
	runInTxnErr     error
	loadRangeKeys   []string
}

func (s *testFollowerRegionStorage) LoadRange(key, endKey string, limit int) (keys []string, values []string, err error) {
	if s.loadRangeErr != nil {
		return nil, nil, s.loadRangeErr
	}
	if s.loadRangeKeys != nil {
		return s.loadRangeKeys, nil, nil
	}
	return s.Storage.LoadRange(key, endKey, limit)
}

func (s *testFollowerRegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	if s.loadRegionErr != nil {
		return false, s.loadRegionErr
	}
	return s.Storage.LoadRegion(regionID, region)
}

func (s *testFollowerRegionStorage) DeleteRegion(region *metapb.Region) error {
	if s.deleteRegionErr != nil {
		return s.deleteRegionErr
	}
	return s.Storage.DeleteRegion(region)
}

func (s *testFollowerRegionStorage) RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error {
	if s.runInTxnErr != nil {
		return s.runInTxnErr
	}
	return s.Storage.RunInTxn(ctx, f)
>>>>>>> 8179ce7c2d (server: resync follower region cache reset (#10689))
}
