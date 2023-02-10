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
	"path"
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

const globalConfigPath = "/global/config/"

type testReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

func (s testReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		s.re.Contains(change.Name, globalConfigPath+string(change.Payload))
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
	gsi, suite.cleanup, err = server.NewTestServer(suite.Require(), checker)
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
	return globalConfigPath + configPath
}

func (suite *globalConfigTestSuite) TestLoadWithoutNames() {
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
	suite.Equal("test", string(res.Items[0].Payload))
}

func (suite *globalConfigTestSuite) TestLoadWithoutConfigPath() {
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("source_id"))
		suite.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("source_id"), "1")
	suite.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"source_id"},
	})
	suite.NoError(err)
	suite.Len(res.Items, 1)
	suite.Equal([]byte("1"), res.Items[0].Payload)
}

func (suite *globalConfigTestSuite) TestLoadOtherConfigPath() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), path.Join("OtherConfigPath", strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names:      []string{"0", "1"},
		ConfigPath: "OtherConfigPath",
	})
	suite.NoError(err)
	suite.Len(res.Items, 2)
	for i, item := range res.Items {
		suite.Equal(&pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: strconv.Itoa(i), Payload: []byte(strconv.Itoa(i))}, item)
	}
}

func (suite *globalConfigTestSuite) TestLoadAndStore() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
			suite.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    changes,
	})
	suite.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	suite.Len(res.Items, 3)
	suite.NoError(err)
	for i, item := range res.Items {
		suite.Equal(&pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: suite.GetEtcdPath(strconv.Itoa(i)), Payload: []byte(strconv.Itoa(i))}, item)
	}
}

func (suite *globalConfigTestSuite) TestStore() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
			suite.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
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
	for i := 0; i < 6; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	for i := 3; i < 6; i++ {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		suite.NoError(err)
	}
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	suite.Len(res.Items, 3)
	suite.NoError(err)
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutNames() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), nil, globalConfigPath)
	suite.NoError(err)
	suite.Len(res, 3)
	for i, item := range res {
		suite.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: suite.GetEtcdPath(strconv.Itoa(i)), PayLoad: []byte(strconv.Itoa(i)), Value: strconv.Itoa(i)}, item)
	}
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutConfigPath() {
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("source_id"))
		suite.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("source_id"), "1")
	suite.NoError(err)
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"source_id"}, "")
	suite.NoError(err)
	suite.Len(res, 1)
	suite.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: "source_id", PayLoad: []byte("1"), Value: "1"}, res[0])
}

func (suite *globalConfigTestSuite) TestClientLoadOtherConfigPath() {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), path.Join("OtherConfigPath", strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"0", "1"}, "OtherConfigPath")
	suite.NoError(err)
	suite.Len(res, 2)
	for i, item := range res {
		suite.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: strconv.Itoa(i), PayLoad: []byte(strconv.Itoa(i)), Value: strconv.Itoa(i)}, item)
	}
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

		for i := 3; i < 9; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			suite.NoError(err)
		}
	}()
	// Mock get revision by loading
	r, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("test"), "test")
	suite.NoError(err)
	res, revision, err := suite.client.LoadGlobalConfig(suite.server.Context(), nil, globalConfigPath)
	suite.NoError(err)
	suite.Len(res, 1)
	suite.Equal(r.Header.GetRevision(), revision)
	suite.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: suite.GetEtcdPath("test"), PayLoad: []byte("test"), Value: "test"}, res[0])
	// Mock when start watcher there are existed some keys, will load firstly
	for i := 0; i < 6; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		suite.NoError(err)
	}
	// Start watcher at next revision
	configChan, err := suite.client.WatchGlobalConfig(suite.server.Context(), globalConfigPath, revision)
	suite.NoError(err)
	// Mock delete
	for i := 0; i < 3; i++ {
		_, err = suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		suite.NoError(err)
	}
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
