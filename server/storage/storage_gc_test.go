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
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/storage/endpoint"
)

var _ = Suite(&testStorageGCSuite{})

type testStorageGCSuite struct {
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

func testServiceSafePoints() ([]string, []*endpoint.ServiceSafePoint) {
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
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
		{ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
		{ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
	}
	return spaceIDs, serviceSafePoints
}

func (s *testStorageGCSuite) TestSaveLoadServiceSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints := testServiceSafePoints()
	for i := range testSpaceID {
		c.Assert(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i]), IsNil)
	}
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		c.Assert(err, IsNil)
		c.Assert(loadedSafePoint, DeepEquals, testSafePoints[i])
	}
}

func (s *testStorageGCSuite) TestLoadMinServiceSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	currentTime := time.Now()
	expireAt1 := currentTime.Add(100 * time.Second).Unix()
	expireAt2 := currentTime.Add(200 * time.Second).Unix()
	expireAt3 := currentTime.Add(300 * time.Second).Unix()

	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "0", ExpiredAt: expireAt1, SafePoint: 100},
		{ServiceID: "1", ExpiredAt: expireAt2, SafePoint: 200},
		{ServiceID: "2", ExpiredAt: expireAt3, SafePoint: 300},
	}

	testKeySpace := "test"
	for _, serviceSafePoint := range serviceSafePoints {
		c.Assert(storage.SaveServiceSafePoint(testKeySpace, serviceSafePoint), IsNil)
	}
	minSafePoint, err := storage.LoadMinServiceSafePoint(testKeySpace, currentTime)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, DeepEquals, serviceSafePoints[0])

	// this should remove safePoint with ServiceID 0 due to expiration
	// and find the safePoint with ServiceID 1
	minSafePoint2, err := storage.LoadMinServiceSafePoint(testKeySpace, currentTime.Add(150*time.Second))
	c.Assert(err, IsNil)
	c.Assert(minSafePoint2, DeepEquals, serviceSafePoints[1])

	// verify that service safe point with ServiceID 0 has been removed
	// removal might take some time
	time.Sleep(time.Millisecond * 100)
	ssp, err := storage.LoadServiceSafePoint(testKeySpace, "0")
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)

	// this should remove all service safe points
	// and return nil
	ssp, err = storage.LoadMinServiceSafePoint(testKeySpace, currentTime.Add(500*time.Second))
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)
}

func (s *testStorageGCSuite) TestRemoveServiceSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints := testServiceSafePoints()
	// save service safe points
	for i := range testSpaceID {
		c.Assert(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i]), IsNil)
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
	storage := NewStorageWithMemoryBackend()
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
	storage := NewStorageWithMemoryBackend()
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
	spaceIDs, safePoints := testServiceSafePoints()
	for i := range spaceIDs {
		c.Assert(storage.SaveServiceSafePoint(spaceIDs[i], safePoints[i]), IsNil)
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
	storage := NewStorageWithMemoryBackend()

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
