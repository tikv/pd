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

package client_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

const (
	globalConfigPath            = "/global/config/"
	resourceGroupControllerPath = "resource_group/controller"
)

type testReceiver struct {
	re  *require.Assertions
	ctx context.Context
	grpc.ServerStream
}

func (s testReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		s.re.Contains(change.Name, globalConfigPath+string(change.Payload))
	}
	return nil
}

func (s testReceiver) Context() context.Context {
	return s.ctx
}

type globalConfigTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *server.GrpcServer
	client  pd.Client
	mu      syncutil.Mutex
}

func TestGlobalConfigTestSuite(t *testing.T) {
	suite.Run(t, new(globalConfigTestSuite))
}

func (suite *globalConfigTestSuite) SetupSuite() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())

	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	gsi := suite.cluster.GetLeaderServer().GetServer()
	suite.server = &server.GrpcServer{Server: gsi}
	addr := suite.server.GetAddr()
	suite.client, err = pd.NewClientWithContext(suite.server.Context(),
		caller.TestComponent,
		[]string{addr}, pd.SecurityOption{},
	)
	re.NoError(err)
}

func (suite *globalConfigTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.cancel()
	suite.cluster.Destroy()
}

func getEtcdPath(configPath string) string {
	return globalConfigPath + configPath
}

func (suite *globalConfigTestSuite) TestLoadWithoutNames() {
	re := suite.Require()
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath("test"))
		re.NoError(err)
	}()
	r, err := suite.server.GetClient().Put(suite.server.Context(), getEtcdPath("test"), "test")
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.NoError(err)
	re.Len(res.Items, 1)
	suite.LessOrEqual(r.Header.GetRevision(), res.Revision)
	re.Equal("test", string(res.Items[0].Payload))
}

func (suite *globalConfigTestSuite) TestLoadWithoutConfigPath() {
	re := suite.Require()
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath("source_id"))
		re.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), getEtcdPath("source_id"), "1")
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"source_id"},
	})
	re.NoError(err)
	re.Len(res.Items, 1)
	re.Equal([]byte("1"), res.Items[0].Payload)
}

func (suite *globalConfigTestSuite) TestRejectInvalidConfigPath() {
	re := suite.Require()
	invalidPaths := []string{
		"OtherConfigPath",
		"/tmp/codex-repro/",
		"/global/configuration/",
		"/",
	}
	for _, configPath := range invalidPaths {
		_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
			ConfigPath: configPath,
			Changes: []*pdpb.GlobalConfigItem{{
				Kind:    pdpb.EventType_PUT,
				Name:    "source_id",
				Payload: []byte("1"),
			}},
		})
		re.Equal(codes.InvalidArgument, status.Code(err), configPath)

		_, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
			Names:      []string{"source_id"},
			ConfigPath: configPath,
		})
		re.Equal(codes.InvalidArgument, status.Code(err), configPath)

		_, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
			ConfigPath: configPath,
		})
		re.Equal(codes.InvalidArgument, status.Code(err), configPath)

		err = suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
			ConfigPath: configPath,
		}, testReceiver{re: re, ctx: suite.server.Context()})
		re.Equal(codes.InvalidArgument, status.Code(err), configPath)
	}
}

func (suite *globalConfigTestSuite) TestLiteralConfigPath() {
	re := suite.Require()
	configPaths := []string{
		"/global/config/../pd/",
		"/global/config/child/../../pd/",
		"/global/config/./child/",
		"/global/config//child/",
		"/global/config//",
		`/global/config/child\secret/`,
	}
	defer func() {
		for _, configPath := range configPaths {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), configPath+"source_id")
			re.NoError(err)
		}
	}()

	for _, configPath := range configPaths {
		_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
			ConfigPath: configPath,
			Changes: []*pdpb.GlobalConfigItem{{
				Kind:    pdpb.EventType_PUT,
				Name:    "source_id",
				Payload: []byte(configPath),
			}},
		})
		re.NoError(err, configPath)

		res, err := suite.server.GetClient().Get(suite.server.Context(), configPath+"source_id")
		re.NoError(err, configPath)
		re.Len(res.Kvs, 1, configPath)
		re.Equal([]byte(configPath), res.Kvs[0].Value, configPath)
	}
}

func (suite *globalConfigTestSuite) TestResourceGroupControllerConfig() {
	re := suite.Require()
	siblingKey := resourceGroupControllerPath + "-other"
	settingsKey := resourceGroupControllerPath + "/settings"
	defer func() {
		for _, key := range []string{resourceGroupControllerPath, siblingKey, settingsKey} {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), key)
			re.NoError(err)
		}
	}()

	_, err := suite.server.GetClient().Put(suite.server.Context(), resourceGroupControllerPath, "controller")
	re.NoError(err)
	_, err = suite.server.GetClient().Put(suite.server.Context(), siblingKey, "other")
	re.NoError(err)

	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: resourceGroupControllerPath,
	})
	re.NoError(err)
	re.Equal([]*pdpb.GlobalConfigItem{{
		Kind:    pdpb.EventType_PUT,
		Name:    resourceGroupControllerPath,
		Payload: []byte("controller"),
	}, {
		Kind:    pdpb.EventType_PUT,
		Name:    siblingKey,
		Payload: []byte("other"),
	}}, res.Items)

	_, err = suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: resourceGroupControllerPath,
		Changes: []*pdpb.GlobalConfigItem{{
			Kind:    pdpb.EventType_PUT,
			Name:    "settings",
			Payload: []byte("1"),
		}},
	})
	re.NoError(err)

	res, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names:      []string{"settings"},
		ConfigPath: resourceGroupControllerPath,
	})
	re.NoError(err)
	re.Equal([]*pdpb.GlobalConfigItem{{
		Kind:    pdpb.EventType_PUT,
		Name:    "settings",
		Payload: []byte("1"),
	}}, res.Items)

	watchCtx, cancel := context.WithCancel(suite.server.Context())
	cancel()
	err = suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
		ConfigPath: resourceGroupControllerPath,
	}, testReceiver{re: re, ctx: watchCtx})
	re.NotEqual(codes.InvalidArgument, status.Code(err))
}

func (suite *globalConfigTestSuite) TestNestedConfigPath() {
	re := suite.Require()
	nestedPath := "/global/config/tidb"
	nestedKey := nestedPath + "/source_id"
	siblingKey := "/global/config/tidb-other/source_id"
	defer func() {
		for _, key := range []string{nestedKey, siblingKey} {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), key)
			re.NoError(err)
		}
	}()

	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: nestedPath,
		Changes: []*pdpb.GlobalConfigItem{{
			Kind:    pdpb.EventType_PUT,
			Name:    "source_id",
			Payload: []byte("1"),
		}},
	})
	re.NoError(err)
	_, err = suite.server.GetClient().Put(suite.server.Context(), siblingKey, "2")
	re.NoError(err)

	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: nestedPath + "/",
	})
	re.NoError(err)
	re.Equal([]*pdpb.GlobalConfigItem{{
		Kind:    pdpb.EventType_PUT,
		Name:    nestedKey,
		Payload: []byte("1"),
	}}, res.Items)

	res, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names:      []string{"source_id"},
		ConfigPath: nestedPath + "/",
	})
	re.NoError(err)
	re.Equal([]*pdpb.GlobalConfigItem{{
		Kind:    pdpb.EventType_PUT,
		Name:    "source_id",
		Payload: []byte("1"),
	}}, res.Items)
}

func (suite *globalConfigTestSuite) TestEmptyConfigName() {
	re := suite.Require()
	rootPath := "/global/config"
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), rootPath)
		re.NoError(err)
	}()

	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		Changes: []*pdpb.GlobalConfigItem{{
			Kind:    pdpb.EventType_PUT,
			Name:    "",
			Payload: []byte("root"),
		}},
	})
	re.NoError(err)
	getRes, err := suite.server.GetClient().Get(suite.server.Context(), rootPath)
	re.NoError(err)
	re.Len(getRes.Kvs, 1)
	re.Equal([]byte("root"), getRes.Kvs[0].Value)

	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{""},
	})
	re.NoError(err)
	re.Equal([]*pdpb.GlobalConfigItem{{
		Kind:    pdpb.EventType_PUT,
		Name:    "",
		Payload: []byte("root"),
	}}, res.Items)
}

func (suite *globalConfigTestSuite) TestCompatibleConfigName() {
	re := suite.Require()
	names := []string{
		".",
		"..",
		"nested/source_id",
		"nested/..",
		"nested/../source_id",
		"../source_id",
		"nested/../../pd",
		`nested\source_id`,
		"source id",
		"source:id",
		"source..id",
		"配置",
		"/absolute",
	}
	defer func() {
		for _, name := range names {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(name))
			re.NoError(err)
		}
	}()

	changes := make([]*pdpb.GlobalConfigItem, 0, len(names))
	for _, name := range names {
		changes = append(changes, &pdpb.GlobalConfigItem{
			Kind:    pdpb.EventType_PUT,
			Name:    name,
			Payload: []byte(name),
		})
	}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		Changes: changes,
	})
	re.NoError(err)

	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: names,
	})
	re.NoError(err)
	re.Len(res.Items, len(names))
	for i, item := range res.Items {
		re.Equal(names[i], item.Name)
		re.Equal([]byte(names[i]), item.Payload)

		expectedKey := getEtcdPath(names[i])
		getRes, err := suite.server.GetClient().Get(suite.server.Context(), expectedKey)
		re.NoError(err)
		re.Len(getRes.Kvs, 1)
		re.Equal(expectedKey, string(getRes.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestLoadAndStore() {
	re := suite.Require()
	defer func() {
		for i := range 3 {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: "/global/config",
		Changes:    changes,
	})
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.Len(res.Items, 3)
	re.NoError(err)
	for i, item := range res.Items {
		re.Equal(&pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: getEtcdPath(strconv.Itoa(i)), Payload: []byte(strconv.Itoa(i))}, item)
	}
}

func (suite *globalConfigTestSuite) TestStore() {
	re := suite.Require()
	defer func() {
		for i := range 3 {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		Changes: changes,
	})
	re.NoError(err)
	for i := range 3 {
		res, err := suite.server.GetClient().Get(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
		re.Equal(getEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestWatch() {
	re := suite.Require()
	defer func() {
		for i := range 3 {
			// clean up
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	ctx, cancel := context.WithCancel(suite.server.Context())
	defer cancel()
	server := testReceiver{re: suite.Require(), ctx: ctx}
	go func() {
		err := suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
			ConfigPath: globalConfigPath,
			Revision:   0,
		}, server)
		re.NoError(err)
	}()
	for i := range 6 {
		_, err := suite.server.GetClient().Put(suite.server.Context(), getEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	for i := 3; i < 6; i++ {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
	}
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.Len(res.Items, 3)
	re.NoError(err)
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutNames() {
	re := suite.Require()
	defer func() {
		for i := range 3 {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	for i := range 3 {
		_, err := suite.server.GetClient().Put(suite.server.Context(), getEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), nil, globalConfigPath)
	re.NoError(err)
	re.Len(res, 3)
	for i, item := range res {
		re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: getEtcdPath(strconv.Itoa(i)), PayLoad: []byte(strconv.Itoa(i)), Value: strconv.Itoa(i)}, item)
	}
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutConfigPath() {
	re := suite.Require()
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath("source_id"))
		re.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), getEtcdPath("source_id"), "1")
	re.NoError(err)
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"source_id"}, "")
	re.NoError(err)
	re.Len(res, 1)
	re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: "source_id", PayLoad: []byte("1"), Value: "1"}, res[0])
}

func (suite *globalConfigTestSuite) TestClientRejectOtherConfigPath() {
	re := suite.Require()
	_, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"source_id"}, "OtherConfigPath")
	re.Equal(codes.InvalidArgument, status.Code(err))
}

func (suite *globalConfigTestSuite) TestClientStore() {
	re := suite.Require()
	defer func() {
		for i := range 3 {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	err := suite.client.StoreGlobalConfig(suite.server.Context(), globalConfigPath,
		[]pd.GlobalConfigItem{{Name: "0", Value: "0"}, {Name: "1", Value: "1"}, {Name: "2", Value: "2"}})
	re.NoError(err)
	for i := range 3 {
		res, err := suite.server.GetClient().Get(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
		re.Equal(getEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestClientWatchWithRevision() {
	re := suite.Require()
	ctx := suite.server.Context()
	defer func() {
		_, err := suite.server.GetClient().Delete(ctx, getEtcdPath("test"))
		re.NoError(err)

		for i := 3; i < 9; i++ {
			_, err := suite.server.GetClient().Delete(ctx, getEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	// Mock get revision by loading
	r, err := suite.server.GetClient().Put(ctx, getEtcdPath("test"), "test")
	re.NoError(err)
	res, revision, err := suite.client.LoadGlobalConfig(ctx, nil, globalConfigPath)
	re.NoError(err)
	re.Len(res, 1)
	suite.LessOrEqual(r.Header.GetRevision(), revision)
	re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: getEtcdPath("test"), PayLoad: []byte("test"), Value: "test"}, res[0])
	// Mock when start watcher there are existed some keys, will load firstly
	for i := range 6 {
		_, err = suite.server.GetClient().Put(suite.server.Context(), getEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	// Start watcher at next revision
	configChan, err := suite.client.WatchGlobalConfig(suite.server.Context(), globalConfigPath, revision)
	re.NoError(err)
	// Mock delete
	for i := range 3 {
		_, err = suite.server.GetClient().Delete(suite.server.Context(), getEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
	}
	// Mock put
	for i := 6; i < 9; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), getEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	runTest := false
	for {
		select {
		case <-timer.C:
			re.True(runTest)
			return
		case res := <-configChan:
			for _, r := range res {
				re.Equal(getEtcdPath(r.Value), r.Name)
			}
			runTest = true
		}
	}
}

func (suite *globalConfigTestSuite) TestEtcdNotStart() {
	re := suite.Require()
	cli := suite.server.GetClient()
	defer func() {
		suite.mu.Lock()
		suite.server.SetClient(cli)
		suite.mu.Unlock()
	}()
	suite.mu.Lock()
	suite.server.SetClient(nil)
	suite.mu.Unlock()
	err := suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Revision:   0,
	}, nil)
	re.Error(err)

	_, err = suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}},
	})
	re.Error(err)

	_, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"pd_tests"},
	})
	re.Error(err)
}
