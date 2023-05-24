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
	"encoding/json"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type gcClientTestReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

func (s gcClientTestReceiver) Send(m *pdpb.WatchGCSafePointV2Response) error {
	log.Info("received", zap.Any("received", m.GetEvents()))
	for _, change := range m.GetEvents() {
		s.re.Equal(change.SafePoint, uint64(change.KeyspaceId))
	}
	return nil
}

type gcClientTestSuite struct {
	suite.Suite
	server  *server.GrpcServer
	client  pd.Client
	cleanup testutil.CleanupFunc
}

func TestGcClientTestSuite(t *testing.T) {
	suite.Run(t, new(gcClientTestSuite))
}

func (suite *gcClientTestSuite) SetupSuite() {
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
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/gc/checkKeyspace", "return(true)"))
}

func (suite *gcClientTestSuite) TearDownSuite() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gc/checkKeyspace"))
	suite.cleanup()
}

func (suite *gcClientTestSuite) GetEtcdPathPrefix() string {
	rootPath := path.Join("/pd", strconv.FormatUint(suite.server.ClusterID(), 10))
	return path.Join(rootPath, endpoint.GCSafePointV2Prefix())
}

func (suite *gcClientTestSuite) GetKeyspaceGCEtcdPath(keyspaceID uint32) string {
	path := suite.GetEtcdPathPrefix() + "/" + endpoint.EncodeKeyspaceID(keyspaceID)
	log.Info("test etcd path", zap.Any("path", path))
	return path
}

func (suite *gcClientTestSuite) TestWatch1() {
	defer func() {
		for i := 0; i < 6; i++ {
			// clean up
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(uint32(i)))
			suite.NoError(err)
		}
	}()
	server := gcClientTestReceiver{re: suite.Require()}
	go suite.server.WatchGCSafePointV2(&pdpb.WatchGCSafePointV2Request{
		Revision: 0,
	}, server)

	// Init gc safe points as index value of keyspace 0 ~ 5.
	for i := 0; i < 6; i++ {
		gcSafePointV2, err := suite.makerGCSafePointV2(uint32(i), uint64(i))
		suite.NoError(err)
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(uint32(i)), string(gcSafePointV2))
		suite.NoError(err)
	}

	// delete gc safe points of keyspace 3 ~ 5.
	for i := 3; i < 6; i++ {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(uint32(i)))
		suite.NoError(err)
	}

	// check gc safe point not equals 0 of keyspace 0 ~ 2 .
	for i := 0; i < 3; i++ {
		res, err := suite.server.GetSafePointV2Manager().LoadGCSafePoint(uint32(i))
		suite.NoError(err)
		suite.Equal(uint64(i), res.SafePoint)
	}

	// check gc safe point is 0 of keyspace 3 ~ 5 after delete.
	for i := 3; i < 6; i++ {
		res, err := suite.server.GetSafePointV2Manager().LoadGCSafePoint(uint32(i))
		suite.NoError(err)
		suite.Equal(uint64(0), res.SafePoint)
	}
}

func (suite *gcClientTestSuite) TestClientWatchWithRevision() {
	suite.testClientWatchWithRevision(false)
	suite.testClientWatchWithRevision(true)
}

func (suite *gcClientTestSuite) testClientWatchWithRevision(isNewRevision bool) {
	testKeyspaceID := uint32(1)
	initGCSafePoint := uint64(999)
	newestGCSafePoint := uint64(1)

	defer func() {
		for i := 0; i < 6; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(uint32(i)))
			suite.NoError(err)
		}
	}()

	// Init gc safe point.
	gcSafePointV2, err := suite.makerGCSafePointV2(testKeyspaceID, initGCSafePoint)
	suite.NoError(err)
	_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(testKeyspaceID), string(gcSafePointV2))
	suite.NoError(err)

	res, err := suite.server.GetClient().Get(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(testKeyspaceID))
	suite.NoError(err)
	revision := res.Header.GetRevision()

	// Mock when start watcher there are existed some keys, will load firstly
	gcSafePointV2, err = suite.makerGCSafePointV2(testKeyspaceID, newestGCSafePoint)
	suite.NoError(err)
	putResp, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetKeyspaceGCEtcdPath(testKeyspaceID), string(gcSafePointV2))
	suite.NoError(err)

	if isNewRevision {
		revision = putResp.Header.GetRevision()
	}
	watchChan, err := suite.client.WatchGCSafePointV2(suite.server.Context(), revision)
	suite.NoError(err)

	// IF there is an old revision,it needed to ignore check, we just need get the newest data of all keyspace.
	var isOldestValue bool

	for {
		select {
		case <-time.After(time.Second):
			return
		case res := <-watchChan:
			for _, r := range res {
				if isNewRevision {
					suite.Equal(uint64(r.KeyspaceId), r.SafePoint)
				} else {
					if !isOldestValue {
						isOldestValue = true
					} else {
						suite.Equal(newestGCSafePoint, r.GetSafePoint())
					}
				}
			}
		}
	}
}

func (suite *gcClientTestSuite) makerGCSafePointV2(keyspaceID uint32, gcSafePoint uint64) ([]byte, error) {
	gcSafePointV2 := &endpoint.GCSafePointV2{
		KeyspaceID: keyspaceID,
		SafePoint:  gcSafePoint,
	}
	value, err := json.Marshal(gcSafePointV2)
	if err != nil {
		return nil, errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return value, nil
}
