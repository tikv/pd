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
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

var _ = Suite(&testStorageGCSuite{})

type testStorageGCSuite struct {
	cfg     *embed.Config
	etcd    *embed.Etcd
	storage Storage
}

func (s *testStorageGCSuite) SetUpTest(c *C) {
	s.cfg = newTestSingleConfig()
	s.etcd = mustNewEmbedEtcd(c, s.cfg)

	ep := s.cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	s.storage = NewStorageWithEtcdBackend(client, rootPath)
}

func (s *testStorageGCSuite) TearDownTest(c *C) {
	if s.etcd != nil {
		s.etcd.Close()
	}
	c.Assert(cleanConfig(s.cfg), IsNil)
}

func testGCSafePoints() ([]string, []uint64) {
	spaceIDs := []string{
		"keySpace1",
		"keySpace2",
		"keySpace3",
		"keySpace4",
		"keySpace5",
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

func testServiceSafePoints() ([]string, []*endpoint.ServiceSafePoint, []int64) {
	spaceIDs := []string{
		"keySpace1",
		"keySpace1",
		"keySpace1",
		"keySpace2",
		"keySpace2",
		"keySpace2",
		"keySpace3",
		"keySpace3",
		"keySpace3",
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

func (s *testStorageGCSuite) TestSaveLoadServiceSafePoint(c *C) {
	storage := s.storage
	testSpaceID, testSafePoints, testTTLs := testServiceSafePoints()
	for i := range testSpaceID {
		c.Assert(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i], testTTLs[i]), IsNil)
	}
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		c.Assert(err, IsNil)
		c.Assert(loadedSafePoint, DeepEquals, testSafePoints[i])
	}
}

func (s *testStorageGCSuite) TestLoadMinServiceSafePoint(c *C) {
	storage := s.storage
	testTTLs := []int64{2, 6}
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "0", SafePoint: 100},
		{ServiceID: "1", SafePoint: 200},
	}
	testKeySpace := "test"
	for i := range serviceSafePoints {
		c.Assert(storage.SaveServiceSafePoint(testKeySpace, serviceSafePoints[i], testTTLs[i]), IsNil)
	}

	minSafePoint, err := storage.LoadMinServiceSafePoint(testKeySpace)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, DeepEquals, serviceSafePoints[0])

	time.Sleep(4 * time.Second)
	// the safePoint with ServiceID 0 should be removed due to expiration
	// now min should be safePoint with ServiceID 1
	minSafePoint2, err := storage.LoadMinServiceSafePoint(testKeySpace)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint2, DeepEquals, serviceSafePoints[1])

	// verify that service safe point with ServiceID 0 has been removed
	ssp, err := storage.LoadServiceSafePoint(testKeySpace, "0")
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)

	time.Sleep(4 * time.Second)
	// all remaining service safePoints should be removed due to expiration
	ssp, err = storage.LoadMinServiceSafePoint(testKeySpace)
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)
}

func (s *testStorageGCSuite) TestRemoveServiceSafePoint(c *C) {
	storage := s.storage
	testSpaceID, testSafePoints, testTTLs := testServiceSafePoints()
	// save service safe points
	for i := range testSpaceID {
		c.Assert(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i], testTTLs[i]), IsNil)
	}
	// remove saved service safe points
	for i := range testSpaceID {
		c.Assert(storage.RemoveServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID), IsNil)
	}
	// check that service safe points are empty
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		c.Assert(err, IsNil)
		c.Assert(loadedSafePoint, IsNil)
	}
}

func (s *testStorageGCSuite) TestSaveLoadGCSafePoint(c *C) {
	storage := s.storage
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		testSpaceID := testSpaceIDs[i]
		testSafePoint := testSafePoints[i]
		err := storage.SaveKeySpaceGCSafePoint(testSpaceID, testSafePoint)
		c.Assert(err, IsNil)
		loaded, err := storage.LoadKeySpaceGCSafePoint(testSpaceID)
		c.Assert(err, IsNil)
		c.Assert(loaded, Equals, testSafePoint)
	}
}

func (s *testStorageGCSuite) TestLoadAllKeySpaceGCSafePoints(c *C) {
	storage := s.storage
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		err := storage.SaveKeySpaceGCSafePoint(testSpaceIDs[i], testSafePoints[i])
		c.Assert(err, IsNil)
	}
	loadedSafePoints, err := storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	for i := range loadedSafePoints {
		c.Assert(loadedSafePoints[i].SpaceID, Equals, testSpaceIDs[i])
		c.Assert(loadedSafePoints[i].SafePoint, Equals, testSafePoints[i])
	}

	// saving some service safe points.
	spaceIDs, safePoints, TTLs := testServiceSafePoints()
	for i := range spaceIDs {
		c.Assert(storage.SaveServiceSafePoint(spaceIDs[i], safePoints[i], TTLs[i]), IsNil)
	}

	// verify that service safe points do not interfere with gc safe points.
	loadedSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	for i := range loadedSafePoints {
		c.Assert(loadedSafePoints[i].SpaceID, Equals, testSpaceIDs[i])
		c.Assert(loadedSafePoints[i].SafePoint, Equals, testSafePoints[i])
	}

	// verify that when withGCSafePoint set to false, returned safePoints is 0
	loadedSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(false)
	c.Assert(err, IsNil)
	for i := range loadedSafePoints {
		c.Assert(loadedSafePoints[i].SpaceID, Equals, testSpaceIDs[i])
		c.Assert(loadedSafePoints[i].SafePoint, Equals, uint64(0))
	}
}

func (s *testStorageGCSuite) TestLoadEmpty(c *C) {
	storage := s.storage

	// loading non-existing GC safepoint should return 0
	gcSafePoint, err := storage.LoadKeySpaceGCSafePoint("testKeySpace")
	c.Assert(err, IsNil)
	c.Assert(gcSafePoint, Equals, uint64(0))

	// loading non-existing service safepoint should return nil
	serviceSafePoint, err := storage.LoadServiceSafePoint("testKeySpace", "testService")
	c.Assert(err, IsNil)
	c.Assert(serviceSafePoint, IsNil)

	// loading empty key spaces should return empty slices
	safePoints, err := storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	c.Assert(safePoints, HasLen, 0)
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

func cleanConfig(cfg *embed.Config) error {
	// Clean data directory
	return os.RemoveAll(cfg.Dir)
}

func mustNewEmbedEtcd(c *C, cfg *embed.Config) *embed.Etcd {
	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)
	return etcd
}
