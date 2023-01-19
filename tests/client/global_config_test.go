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
	"strconv"
	"testing"
	"time"

	pd "github.com/tikv/pd/client"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const globalConfigPath = "global/config/"

type testReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

func (s testReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		switch change.GetKind() {
		case pdpb.EventType_PUT:
			s.re.Contains(change.Name, globalConfigPath+change.Value)
		case pdpb.EventType_DELETE:
			s.re.Empty(change.Value)
		}
	}
	return nil
}

type globalConfigTestSuite struct {
	suite.Suite
	server  *server.GrpcServer
	client  pd.Client
	cleanup server.CleanupFunc
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
	suite.client, err = pd.NewClientWithContext(suite.server.Context(), []string{addr}, pd.SecurityOption{})
	suite.NoError(err)
}

func (suite *globalConfigTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.cleanup()
}

func (suite *globalConfigTestSuite) GetEtcdPath(configPath string) string {
	return suite.server.GetFinalPathWithinPD(globalConfigPath + configPath)
}

func (suite *globalConfigTestSuite) TestLoad() {
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
		suite.NoError(err)
	}()
	r, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("test"), "test")
	suite.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	suite.NoError(err)
	suite.Len(res.Items, 1)
	suite.Equal(r.Header.GetRevision(), res.Revision)
	suite.Equal("test", res.Items[0].Value)
}

func (suite *globalConfigTestSuite) TestStore() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+"test")
			suite.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Value: "0"}, {Kind: pdpb.EventType_PUT, Name: "1", Value: "1"}, {Kind: pdpb.EventType_PUT, Name: "2", Value: "2"}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    changes,
	})
	suite.NoError(err)
	for i := 0; i < 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		suite.NoError(err)
		suite.Equal(suite.GetEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestWatch() {
	defer func() {
		for i := 0; i < 3; i++ {
			// clean up
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	server := testReceiver{re: suite.Require()}
	go suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Revision:   0,
	}, server)
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
}

func (suite *globalConfigTestSuite) TestClientLoad() {
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), globalConfigPath+"test")
		suite.NoError(err)
	}()
	r, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("test"), "test")
	suite.NoError(err)
	res, revision, err := suite.client.LoadGlobalConfig(suite.server.Context(), globalConfigPath)
	suite.NoError(err)
	suite.Len(res, 1)
	suite.Equal(r.Header.GetRevision(), revision)
	suite.Equal(pd.GlobalConfigItem{Name: suite.GetEtcdPath("test"), Value: "test", EventType: pdpb.EventType_PUT}, res[0])
}

func (suite *globalConfigTestSuite) TestClientStore() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	err := suite.client.StoreGlobalConfig(suite.server.Context(), globalConfigPath,
		[]pd.GlobalConfigItem{{Name: "0", Value: "0"}, {Name: "1", Value: "1"}, {Name: "2", Value: "2"}})
	suite.NoError(err)
	for i := 0; i < 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		suite.NoError(err)
		suite.Equal(suite.GetEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestClientWatchWithRevision() {
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
		suite.NoError(err)

		for i := 0; i < 9; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	// Mock get revision by loading
	r, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("test"), "test")
	suite.NoError(err)
	res, revision, err := suite.client.LoadGlobalConfig(suite.server.Context(), globalConfigPath)
	suite.NoError(err)
	suite.Len(res, 1)
	suite.Equal(r.Header.GetRevision(), revision)
	suite.Equal(pd.GlobalConfigItem{Name: suite.GetEtcdPath("test"), Value: "test", EventType: pdpb.EventType_PUT}, res[0])
	// Mock when start watcher there are existed some keys, will load firstly
	for i := 3; i < 6; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	// Start watcher at next revision
	configChan, err := suite.client.WatchGlobalConfig(suite.server.Context(), globalConfigPath, revision)
	suite.NoError(err)
	// Mock put
	for i := 6; i < 9; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	for {
		select {
		case <-time.After(time.Second):
			return
		case res := <-configChan:
			for _, r := range res {
				suite.Equal(suite.GetEtcdPath(r.Value), r.Name)
			}
		}
	}
}
