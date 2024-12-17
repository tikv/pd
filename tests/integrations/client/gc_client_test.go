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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
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
	re := suite.Require()
	var (
		err error
		gsi *server.Server
	)
	checker := assertutil.NewChecker()
	checker.FailNow = func() {}
	gsi, suite.cleanup, err = server.NewTestServer(re, checker)
	suite.server = &server.GrpcServer{Server: gsi}
	re.NoError(err)
	addr := suite.server.GetAddr()
	suite.client, err = pd.NewClientWithContext(suite.server.Context(),
		caller.TestComponent,
		[]string{addr}, pd.SecurityOption{},
	)
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(keypath.ClusterID(), 10))
	suite.gcSafePointV2Prefix = path.Join(rootPath, keypath.GCSafePointV2Prefix())
	// Enable the fail-point to skip checking keyspace validity.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/gc/checkKeyspace", "return(true)"))
}

func (suite *gcClientTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gc/checkKeyspace"))
	suite.cleanup()
	suite.client.Close()
}

func (suite *gcClientTestSuite) TearDownTest() {
	suite.CleanupEtcdGCPath()
}

func (suite *gcClientTestSuite) CleanupEtcdGCPath() {
	re := suite.Require()
	_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.gcSafePointV2Prefix, clientv3.WithPrefix())
	re.NoError(err)
}

func (suite *gcClientTestSuite) TestWatch1() {
	re := suite.Require()
	receiver := gcClientTestReceiver{re: suite.Require()}
	go suite.server.WatchGCSafePointV2(&pdpb.WatchGCSafePointV2Request{
		Revision: 0,
	}, receiver)

	// Init gc safe points as index value of keyspace 0 ~ 5.
	for i := range 6 {
		suite.mustUpdateSafePoint(re, uint32(i), uint64(i))
	}

	// delete gc safe points of keyspace 3 ~ 5.
	for i := 3; i < 6; i++ {
		suite.mustDeleteSafePoint(re, uint32(i))
	}

	// check gc safe point equal to keyspace id for keyspace 0 ~ 2 .
	for i := range 3 {
		re.Equal(uint64(i), suite.mustLoadSafePoint(re, uint32(i)))
	}

	// check gc safe point is 0 for keyspace 3 ~ 5 after delete.
	for i := 3; i < 6; i++ {
		re.Equal(uint64(0), suite.mustLoadSafePoint(re, uint32(i)))
	}
}

func (suite *gcClientTestSuite) TestClientWatchWithRevision() {
	suite.testClientWatchWithRevision(false)
	suite.testClientWatchWithRevision(true)
}

// nolint:revive
func (suite *gcClientTestSuite) testClientWatchWithRevision(fromNewRevision bool) {
	re := suite.Require()
	testKeyspaceID := uint32(100)
	initGCSafePoint := uint64(50)
	updatedGCSafePoint := uint64(100)

	// Init gc safe point.
	suite.mustUpdateSafePoint(re, testKeyspaceID, initGCSafePoint)

	// Get the initial revision.
	initRevision := suite.mustGetRevision(re, testKeyspaceID)

	// Update the gc safe point.
	suite.mustUpdateSafePoint(re, testKeyspaceID, updatedGCSafePoint)

	// Get the revision of the updated gc safe point.
	updatedRevision := suite.mustGetRevision(re, testKeyspaceID)

	// Set the start revision of the watch request based on fromNewRevision.
	startRevision := initRevision
	if fromNewRevision {
		startRevision = updatedRevision
	}
	watchChan, err := suite.client.WatchGCSafePointV2(suite.server.Context(), startRevision)
	re.NoError(err)

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	isFirstUpdate := true
	runTest := false
	for {
		select {
		case <-timer.C:
			re.True(runTest)
			return
		case res := <-watchChan:
			for _, r := range res {
				re.Equal(r.GetKeyspaceId(), testKeyspaceID)
				if fromNewRevision {
					// If fromNewRevision, first response should be the updated gc safe point.
					re.Equal(r.GetSafePoint(), updatedGCSafePoint)
				} else if isFirstUpdate {
					isFirstUpdate = false
					re.Equal(r.GetSafePoint(), initGCSafePoint)
				} else {
					re.Equal(r.GetSafePoint(), updatedGCSafePoint)
					continue
				}
			}
			runTest = true
		}
	}
}

// mustUpdateSafePoint updates the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustUpdateSafePoint(re *require.Assertions, keyspaceID uint32, safePoint uint64) {
	_, err := suite.client.UpdateGCSafePointV2(suite.server.Context(), keyspaceID, safePoint)
	re.NoError(err)
}

// mustLoadSafePoint loads the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustLoadSafePoint(re *require.Assertions, keyspaceID uint32) uint64 {
	res, err := suite.server.GetSafePointV2Manager().LoadGCSafePoint(keyspaceID)
	re.NoError(err)
	return res.SafePoint
}

// mustDeleteSafePoint deletes the gc safe point of the given keyspace id.
func (suite *gcClientTestSuite) mustDeleteSafePoint(re *require.Assertions, keyspaceID uint32) {
	safePointPath := path.Join(suite.gcSafePointV2Prefix, keypath.EncodeKeyspaceID(keyspaceID))
	log.Info("test etcd path", zap.Any("path", safePointPath)) // TODO: Delete
	_, err := suite.server.GetClient().Delete(suite.server.Context(), safePointPath)
	re.NoError(err)
}

// mustGetRevision gets the revision of the given keyspace's gc safe point.
func (suite *gcClientTestSuite) mustGetRevision(re *require.Assertions, keyspaceID uint32) int64 {
	safePointPath := path.Join(suite.gcSafePointV2Prefix, keypath.EncodeKeyspaceID(keyspaceID))
	res, err := suite.server.GetClient().Get(suite.server.Context(), safePointPath)
	re.NoError(err)
	return res.Header.GetRevision()
}
