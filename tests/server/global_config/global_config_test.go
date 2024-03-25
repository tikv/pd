// Copyright 2021 TiKV Project Authors.
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

package global_config_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/assertutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const globalConfigPath = "/global/config/"

type testReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

func (s testReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		s.re.Equal(globalConfigPath+change.Value, change.Name)
	}
	return nil
}

type globalConfigTestSuite struct {
	suite.Suite
	server  *server.GrpcServer
	client  *grpc.ClientConn
	cleanup server.CleanupFunc
	mu      sync.Mutex
}

func TestGlobalConfigTestSuite(t *testing.T) {
	suite.Run(t, new(globalConfigTestSuite))
}

func (suite *globalConfigTestSuite) SetupSuite() {
	var err error
	var gsi *server.Server
	checker := assertutil.NewChecker()
	checker.FailNow = func() {}
	gsi, suite.cleanup, err = server.NewTestServer(checker)
	suite.server = &server.GrpcServer{Server: gsi}
	suite.NoError(err)
	addr := suite.server.GetAddr()
	suite.client, err = grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	suite.NoError(err)
}

func (suite *globalConfigTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.cleanup()
}

func (suite *globalConfigTestSuite) TestLoad() {
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+"test")
		suite.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), globalConfigPath+"test", "test")
	suite.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{Names: []string{"test"}})
	suite.NoError(err)
	suite.Len(res.Items, 1)
	suite.Equal("test", res.Items[0].Value)
}

func (suite *globalConfigTestSuite) TestLoadError() {
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{Names: []string{"test"}})
	suite.NoError(err)
	suite.NotNil(res.Items[0].Error)
}

func (suite *globalConfigTestSuite) TestStore() {
	defer func() {
		for i := 1; i <= 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
			suite.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Name: "1", Value: "1"}, {Name: "2", Value: "2"}, {Name: "3", Value: "3"}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{Changes: changes})
	suite.NoError(err)
	for i := 1; i <= 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
		suite.NoError(err)
		suite.Equal(globalConfigPath+string(res.Kvs[0].Value), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestWatch() {
	defer func() {
		for i := 0; i < 3; i++ {
			// clean up
			_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
			suite.NoError(err)
		}
	}()
	server := testReceiver{re: suite.Require()}
	go suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{}, server)
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), globalConfigPath+strconv.Itoa(i), strconv.Itoa(i))
		suite.NoError(err)
	}
}

func (suite *globalConfigTestSuite) loadGlobalConfig(ctx context.Context, names []string) ([]*pdpb.GlobalConfigItem, error) {
	res, err := pdpb.NewPDClient(suite.client).LoadGlobalConfig(ctx, &pdpb.LoadGlobalConfigRequest{Names: names})
	return res.GetItems(), err
}

func (suite *globalConfigTestSuite) storeGlobalConfig(ctx context.Context, changes []*pdpb.GlobalConfigItem) error {
	_, err := pdpb.NewPDClient(suite.client).StoreGlobalConfig(ctx, &pdpb.StoreGlobalConfigRequest{Changes: changes})
	return err
}

func (suite *globalConfigTestSuite) watchGlobalConfig(ctx context.Context) (chan []*pdpb.GlobalConfigItem, error) {
	globalConfigWatcherCh := make(chan []*pdpb.GlobalConfigItem, 16)
	res, err := pdpb.NewPDClient(suite.client).WatchGlobalConfig(ctx, &pdpb.WatchGlobalConfigRequest{})
	if err != nil {
		close(globalConfigWatcherCh)
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		for {
			select {
			case <-ctx.Done():
				close(globalConfigWatcherCh)
				return
			default:
				m, err := res.Recv()
				if err != nil {
					return
				}
				arr := make([]*pdpb.GlobalConfigItem, len(m.Changes))
				for j, i := range m.Changes {
					arr[j] = &pdpb.GlobalConfigItem{Name: i.GetName(), Value: i.GetValue()}
				}
				globalConfigWatcherCh <- arr
			}
		}
	}()
	return globalConfigWatcherCh, err
}

func (suite *globalConfigTestSuite) TestClientLoad() {
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+"test")
		suite.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), globalConfigPath+"test", "test")
	suite.NoError(err)
	res, err := suite.loadGlobalConfig(suite.server.Context(), []string{"test"})
	suite.NoError(err)
	suite.Len(res, 1)
	suite.Equal(&pdpb.GlobalConfigItem{Name: "test", Value: "test", Error: nil}, res[0])
}

func (suite *globalConfigTestSuite) TestClientLoadError() {
	res, err := suite.loadGlobalConfig(suite.server.Context(), []string{"test"})
	suite.NoError(err)
	suite.NotNil(res[0].Error)
}

func (suite *globalConfigTestSuite) TestClientStore() {
	defer func() {
		for i := 1; i <= 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
			suite.NoError(err)
		}
	}()
	err := suite.storeGlobalConfig(suite.server.Context(), []*pdpb.GlobalConfigItem{{Name: "1", Value: "1"}, {Name: "2", Value: "2"}, {Name: "3", Value: "3"}})
	suite.NoError(err)
	for i := 1; i <= 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
		suite.NoError(err)
		suite.Equal(globalConfigPath+string(res.Kvs[0].Value), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestClientWatch() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+strconv.Itoa(i))
			suite.NoError(err)
		}
	}()
	wc, err := suite.watchGlobalConfig(suite.server.Context())
	suite.NoError(err)
	for i := 0; i < 3; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), globalConfigPath+strconv.Itoa(i), strconv.Itoa(i))
		suite.NoError(err)
	}
	for {
		select {
		case <-time.After(time.Second):
			return
		case res := <-wc:
			for _, r := range res {
				suite.Equal(globalConfigPath+r.Value, r.Name)
			}
		}
	}
}

func (suite *globalConfigTestSuite) TestEtcdNotStart() {
	cli := suite.server.GetClient()
	defer func() {
		suite.mu.Lock()
		suite.server.SetClient(cli)
		suite.mu.Unlock()
	}()
	suite.mu.Lock()
	suite.server.SetClient(nil)
	suite.mu.Unlock()
	err := suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{}, testReceiver{re: suite.Require()})
	suite.Error(err)

	_, err = suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		Changes: []*pdpb.GlobalConfigItem{{Name: "0", Value: "0"}},
	})
	suite.Error(err)

	_, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"test_etcd"},
	})
	suite.Error(err)
}
