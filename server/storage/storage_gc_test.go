// Copyright 2022 TiKV Project Authors.
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

package storage

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestStorageGCTestSuite(t *testing.T) {
	suite.Run(t, new(StorageGCTestSuite))
}

type StorageGCTestSuite struct {
	suite.Suite
	cfg     *embed.Config
	etcd    *embed.Etcd
	storage Storage
}

func (suite *StorageGCTestSuite) SetupTest() {
	var err error
	suite.cfg = newTestSingleConfig()
	suite.etcd, err = embed.StartEtcd(suite.cfg)
	suite.Require().Nil(err)
	ep := suite.cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	suite.Require().Nil(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	suite.storage = NewStorageWithEtcdBackend(client, rootPath)
}

func (suite *StorageGCTestSuite) TearDownTest() {
	if suite.etcd != nil {
		suite.etcd.Close()
	}
	suite.Require().NoError(os.RemoveAll(suite.cfg.Dir))
}

func (suite *StorageGCTestSuite) TestSaveLoadServiceSafePoint() {
	storage := suite.storage
	testSpaceID, testSafePoints, testTTLs := testServiceSafePoints()
	for i := range testSpaceID {
		suite.NoError(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i], testTTLs[i]))
	}
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		suite.Nil(err)
		suite.Equal(testSafePoints[i], loadedSafePoint)
	}
}

func (suite *StorageGCTestSuite) TestLoadMinServiceSafePoint() {
	storage := suite.storage
	testTTLs := []int64{2, 6}
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "0", SafePoint: 100},
		{ServiceID: "1", SafePoint: 200},
	}
	testKeySpace := uint32(100)
	for i := range serviceSafePoints {
		suite.NoError(storage.SaveServiceSafePoint(testKeySpace, serviceSafePoints[i], testTTLs[i]))
	}

	minSafePoint, err := storage.LoadMinServiceSafePoint(testKeySpace)
	suite.Nil(err)
	suite.Equal(serviceSafePoints[0], minSafePoint)

	time.Sleep(4 * time.Second)
	// the safePoint with ServiceID 0 should be removed due to expiration
	// now min should be safePoint with ServiceID 1
	minSafePoint2, err := storage.LoadMinServiceSafePoint(testKeySpace)
	suite.Nil(err)
	suite.Equal(serviceSafePoints[1], minSafePoint2)

	// verify that service safe point with ServiceID 0 has been removed
	ssp, err := storage.LoadServiceSafePoint(testKeySpace, "0")
	suite.Nil(err)
	suite.Nil(ssp)

	time.Sleep(4 * time.Second)
	// all remaining service safePoints should be removed due to expiration
	ssp, err = storage.LoadMinServiceSafePoint(testKeySpace)
	suite.Nil(err)
	suite.Nil(ssp)
}

func (suite *StorageGCTestSuite) TestRemoveServiceSafePoint() {
	storage := suite.storage
	testSpaceID, testSafePoints, testTTLs := testServiceSafePoints()
	// save service safe points
	for i := range testSpaceID {
		suite.NoError(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i], testTTLs[i]))
	}
	// remove saved service safe points
	for i := range testSpaceID {
		suite.NoError(storage.RemoveServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID))
	}
	// check that service safe points are empty
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		suite.Nil(err)
		suite.Nil(loadedSafePoint)
	}
}

func (suite *StorageGCTestSuite) TestSaveLoadGCSafePoint() {
	storage := suite.storage
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		testSpaceID := testSpaceIDs[i]
		testSafePoint := testSafePoints[i]
		suite.NoError(storage.SaveKeySpaceGCSafePoint(testSpaceID, testSafePoint))
		loaded, err := storage.LoadKeySpaceGCSafePoint(testSpaceID)
		suite.Nil(err)
		suite.Equal(testSafePoint, loaded)
	}
}

func (suite *StorageGCTestSuite) TestLoadAllKeySpaceGCSafePoints() {
	storage := suite.storage
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		suite.NoError(storage.SaveKeySpaceGCSafePoint(testSpaceIDs[i], testSafePoints[i]))
	}
	loadedSafePoints, err := storage.LoadAllKeySpaceGCSafePoints(true)
	suite.Nil(err)
	sort.Slice(loadedSafePoints, func(a, b int) bool {
		return loadedSafePoints[a].SpaceID < loadedSafePoints[b].SpaceID
	})
	for i := range loadedSafePoints {
		suite.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		suite.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}

	// saving some service safe points.
	spaceIDs, safePoints, TTLs := testServiceSafePoints()
	for i := range spaceIDs {
		suite.NoError(storage.SaveServiceSafePoint(spaceIDs[i], safePoints[i], TTLs[i]))
	}

	// verify that service safe points do not interfere with gc safe points.
	loadedSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(true)
	suite.Nil(err)
	sort.Slice(loadedSafePoints, func(a, b int) bool {
		return loadedSafePoints[a].SpaceID < loadedSafePoints[b].SpaceID
	})
	for i := range loadedSafePoints {
		suite.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		suite.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}

	// verify that when withGCSafePoint set to false, returned safePoints is 0
	loadedSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(false)
	suite.Nil(err)
	sort.Slice(loadedSafePoints, func(a, b int) bool {
		return loadedSafePoints[a].SpaceID < loadedSafePoints[b].SpaceID
	})
	for i := range loadedSafePoints {
		suite.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		suite.Equal(uint64(0), loadedSafePoints[i].SafePoint)
	}
}
func (suite *StorageGCTestSuite) TestRevision() {
	storage := suite.storage
	keySpace1 := uint32(100)
	keySpace2 := uint32(200)
	// Touching key space 200 should not change revision of key space 100
	suite.NoError(storage.TouchKeySpaceRevision(keySpace1))
	oldRevision, err := storage.LoadKeySpaceRevision(keySpace1)
	suite.Nil(err)
	suite.NoError(storage.TouchKeySpaceRevision(keySpace2))
	newRevision, err := storage.LoadKeySpaceRevision(keySpace1)
	suite.Nil(err)
	suite.Equal(oldRevision, newRevision)

	// Touching the same key space should change revision
	suite.NoError(storage.TouchKeySpaceRevision(keySpace1))
	newRevision, err = storage.LoadKeySpaceRevision(keySpace1)
	suite.Nil(err)
	suite.NotEqual(oldRevision, newRevision)
}

func (suite *StorageGCTestSuite) TestLoadEmpty() {
	storage := suite.storage
	testKeySpace := uint32(100)
	// loading non-existing GC safepoint should return 0
	gcSafePoint, err := storage.LoadKeySpaceGCSafePoint(testKeySpace)
	suite.Nil(err)
	suite.Equal(uint64(0), gcSafePoint)

	// loading non-existing service safepoint should return nil
	serviceSafePoint, err := storage.LoadServiceSafePoint(testKeySpace, "testService")
	suite.Nil(err)
	suite.Nil(serviceSafePoint)

	// loading empty key spaces should return empty slices
	safePoints, err := storage.LoadAllKeySpaceGCSafePoints(true)
	suite.Nil(err)
	suite.Empty(safePoints)

	// Loading untouched key spaces should return unavailable revision
	revision, err := storage.LoadKeySpaceRevision(testKeySpace)
	suite.Nil(err)
	suite.Equal(kv.RevisionUnavailable, revision)
}

func newTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = os.MkdirTemp("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func testGCSafePoints() ([]uint32, []uint64) {
	spaceIDs := []uint32{
		100,
		200,
		300,
		400,
		500,
	}
	safePoints := []uint64{
		0,
		1,
		4396,
		23333333333,
		math.MaxUint64,
	}
	return spaceIDs, safePoints
}

func testServiceSafePoints() ([]uint32, []*endpoint.ServiceSafePoint, []int64) {
	spaceIDs := []uint32{
		100,
		100,
		100,
		200,
		200,
		200,
		300,
		300,
		300,
	}
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "service1", SafePoint: 1},
		{ServiceID: "service2", SafePoint: 2},
		{ServiceID: "service3", SafePoint: 3},
		{ServiceID: "service1", SafePoint: 1},
		{ServiceID: "service2", SafePoint: 2},
		{ServiceID: "service3", SafePoint: 3},
		{ServiceID: "service1", SafePoint: 1},
		{ServiceID: "service2", SafePoint: 2},
		{ServiceID: "service3", SafePoint: 3},
	}
	testTTls := make([]int64, 9)
	for i := range testTTls {
		testTTls[i] = 10
	}
	return spaceIDs, serviceSafePoints, testTTls
}
