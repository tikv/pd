// Copyright 2016 TiKV Project Authors.
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
=======
	stderrors "errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/member"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
>>>>>>> a186e0cc61 (config: persist default store limit for future stores (#10900))
	"github.com/tikv/pd/server/config"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

<<<<<<< HEAD
type leaderServerTestSuite struct {
	suite.Suite
=======
func TestPartialScheduleConfigUpdatesPreserveLatestFields(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	re.NoError(cfg.Adjust(nil, false))
	store := storage.NewStorageWithMemoryBackend()
	s := &Server{
		persistOptions: config.NewPersistOptions(cfg),
		storage:        store,
	}

	defaultLimit := s.GetScheduleConfig().DefaultStoreLimit.AddPeer + 30
	re.NoError(s.persistOptions.UpdateScheduleConfig(store, func(_ *sc.ScheduleConfig, next *sc.ScheduleConfig) (bool, error) {
		next.DefaultStoreLimit.AddPeer = defaultLimit
		return true, nil
	}))
	re.NoError(s.PatchScheduleConfig([]byte(`{"max-snapshot-count":99}`)))
	re.NoError(s.SetScheduleConfigItem("max-pending-peer-count", float64(88)))

	current := s.GetScheduleConfig()
	re.Equal(defaultLimit, current.DefaultStoreLimit.AddPeer)
	re.Equal(uint64(99), current.MaxSnapshotCount)
	re.Equal(uint64(88), current.MaxPendingPeerCount)

	reloaded := config.NewPersistOptions(config.NewConfig())
	re.NoError(reloaded.Reload(store))
	re.Equal(defaultLimit, reloaded.GetScheduleConfig().DefaultStoreLimit.AddPeer)
	re.Equal(uint64(99), reloaded.GetScheduleConfig().MaxSnapshotCount)
	re.Equal(uint64(88), reloaded.GetScheduleConfig().MaxPendingPeerCount)
}

func TestConcurrentPartialScheduleConfigUpdatesDoNotConflict(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	re.NoError(cfg.Adjust(nil, false))
	store := storage.NewStorageWithMemoryBackend()
	s := &Server{
		persistOptions: config.NewPersistOptions(cfg),
		storage:        store,
	}

	const updates = 16
	start := make(chan struct{})
	errCh := make(chan error, updates*2)
	var wg sync.WaitGroup
	for i := range updates {
		wg.Add(2)
		go func(value int) {
			defer wg.Done()
			<-start
			errCh <- s.PatchScheduleConfig([]byte(fmt.Sprintf(`{"max-snapshot-count":%d}`, value+1)))
		}(i)
		go func(value int) {
			defer wg.Done()
			<-start
			errCh <- s.persistOptions.UpdateScheduleConfig(store, func(_ *sc.ScheduleConfig, next *sc.ScheduleConfig) (bool, error) {
				next.DefaultStoreLimit.AddPeer = float64(value + 30)
				return true, nil
			})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		re.NoError(err)
	}

	current := s.GetScheduleConfig()
	reloaded := config.NewPersistOptions(config.NewConfig())
	re.NoError(reloaded.Reload(store))
	re.Equal(current.DefaultStoreLimit, reloaded.GetScheduleConfig().DefaultStoreLimit)
	re.Equal(current.MaxSnapshotCount, reloaded.GetScheduleConfig().MaxSnapshotCount)
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
>>>>>>> a186e0cc61 (config: persist default store limit for future stores (#10900))

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
}
