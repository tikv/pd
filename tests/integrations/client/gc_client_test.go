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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// gcClientTestReceiver is the pdpb.PD_WatchGCSafePointV2Server mock for testing.
type gcClientTestReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

// Send is the mock implementation for pdpb.PD_WatchGCSafePointV2Server's Send.
// Instead of sending the response to the client, it will check the response.
// In testing, we will set all keyspace's safe point to be equal to its id,
// and this mock verifies that the response is correct.
func (s gcClientTestReceiver) Send(m *pdpb.WatchGCSafePointV2Response) error {
	log.Info("received", zap.Any("received", m.GetEvents()))
	for _, change := range m.GetEvents() {
		s.re.Equal(change.SafePoint, uint64(change.KeyspaceId))
	}
	return nil
}

type gcClientTestSuite struct {
	suite.Suite
	server              *server.GrpcServer
	client              pd.Client
	cleanup             testutil.CleanupFunc
	gcSafePointV2Prefix string
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
	rootPath := path.Join("/pd", strconv.FormatUint(suite.server.ClusterID(), 10))
	suite.gcSafePointV2Prefix = path.Join(rootPath, endpoint.GCSafePointV2Prefix())
	// Enable the fail-point to skip checking keyspace validity.
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/gc/checkKeyspace", "return(true)"))
}

func (suite *gcClientTestSuite) TearDownSuite() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gc/checkKeyspace"))
	suite.cleanup()
}

func (suite *gcClientTestSuite) TearDownTest() {
	suite.CleanupEtcdGCPath()
}

func (suite *gcClientTestSuite) CleanupEtcdGCPath() {
	_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.gcSafePointV2Prefix, clientv3.WithPrefix())
	suite.NoError(err)
}

func (suite *gcClientTestSuite) TestWatch1() {
	receiver := gcClientTestReceiver{re: suite.Require()}
	go suite.server.WatchGCSafePointV2(&pdpb.WatchGCSafePointV2Request{
		Revision: 0,
	}, receiver)

	// Init gc safe points as index value of keyspace 0 ~ 5.
	for i := 0; i < 6; i++ {
		suite.mustUpdateSafePoint(uint32(i), uint64(i))
	}

	// delete gc safe points of keyspace 3 ~ 5.
	for i := 3; i < 6; i++ {
		suite.mustDeleteSafePoint(uint32(i))
	}

	// check gc safe point equal to keyspace id for keyspace 0 ~ 2 .
	for i := 0; i < 3; i++ {
		suite.Equal(uint64(i), suite.mustLoadSafePoint(uint32(i)))
	}

	// check gc safe point is 0 for keyspace 3 ~ 5 after delete.
	for i := 3; i < 6; i++ {
		suite.Equal(uint64(0), suite.mustLoadSafePoint(uint32(i)))
	}
}

func (suite *gcClientTestSuite) TestClientWatchWithRevision() {
	suite.testClientWatchWithRevision(false)
	suite.testClientWatchWithRevision(true)
}

func (suite *gcClientTestSuite) testClientWatchWithRevision(fromNewRevision bool) {
	testKeyspaceID := uint32(100)
	initGCSafePoint := uint64(50)
	updatedGCSafePoint := uint64(100)

	// Init gc safe point.
	suite.mustUpdateSafePoint(testKeyspaceID, initGCSafePoint)

	// Get the initial revision.
	initRevision := suite.mustGetRevision(testKeyspaceID)

	// Update the gc safe point.
	suite.mustUpdateSafePoint(testKeyspaceID, updatedGCSafePoint)

	// Get the revision of the updated gc safe point.
	updatedRevision := suite.mustGetRevision(testKeyspaceID)

	// Set the start revision of the watch request based on fromNewRevision.
	startRevision := initRevision
	if fromNewRevision {
		startRevision = updatedRevision
	}
	watchChan, err := suite.client.WatchGCSafePointV2(suite.server.Context(), startRevision)
	suite.NoError(err)

	timeout := time.After(time.Second)

	isFirstUpdate := true
	for {
		select {
		case <-timeout:
			return
		case res := <-watchChan:
			for _, r := range res {
				suite.Equal(r.GetKeyspaceId(), testKeyspaceID)
				if fromNewRevision {
					// If fromNewRevision, first response should be the updated gc safe point.
					suite.Equal(r.GetSafePoint(), updatedGCSafePoint)
				} else if isFirstUpdate {
					isFirstUpdate = false
					suite.Equal(r.GetSafePoint(), initGCSafePoint)
				} else {
					suite.Equal(r.GetSafePoint(), updatedGCSafePoint)
					continue
				}
			}
		}
	}
}

// mustUpdateSafePoint updates the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustUpdateSafePoint(keyspaceID uint32, safePoint uint64) {
	_, err := suite.client.UpdateGCSafePointV2(suite.server.Context(), keyspaceID, safePoint)
	suite.NoError(err)
}

// mustLoadSafePoint loads the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustLoadSafePoint(keyspaceID uint32) uint64 {
	res, err := suite.server.GetSafePointV2Manager().LoadGCSafePoint(keyspaceID)
	suite.NoError(err)
	return res.SafePoint
}

// mustDeleteSafePoint deletes the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustDeleteSafePoint(keyspaceID uint32) {
	safePointPath := path.Join(suite.gcSafePointV2Prefix, endpoint.EncodeKeyspaceID(keyspaceID))
	log.Info("test etcd path", zap.Any("path", safePointPath)) // TODO: Delete
	_, err := suite.server.GetClient().Delete(suite.server.Context(), safePointPath)
	suite.NoError(err)
}

// mustGetRevision gets the revision of the given keyspace's gc safe point.
func (suite *gcClientTestSuite) mustGetRevision(keyspaceID uint32) int64 {
	safePointPath := path.Join(suite.gcSafePointV2Prefix, endpoint.EncodeKeyspaceID(keyspaceID))
	res, err := suite.server.GetClient().Get(suite.server.Context(), safePointPath)
	suite.NoError(err)
	return res.Header.GetRevision()
}
