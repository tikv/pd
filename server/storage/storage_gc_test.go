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
	"encoding/json"
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/storage/endpoint"
)

var _ = Suite(&testStorageGCSuite{})

type testStorageGCSuite struct {
}

func testGCSafePoints() []*endpoint.KeySpaceGCSafePoint {
	return []*endpoint.KeySpaceGCSafePoint{
		{
			SpaceID:   "KeySpace1",
			SafePoint: 0,
		},
		{
			SpaceID:   "KeySpace2",
			SafePoint: 1,
		},
		{
			SpaceID:   "KeySpace3",
			SafePoint: 4396,
		},
		{
			SpaceID:   "KeySpace4",
			SafePoint: 23333333333,
		},
		{
			SpaceID:   "KeySpace5",
			SafePoint: math.MaxUint64,
		},
	}
}

func (s *testStorageGCSuite) TestLoadGCSafePointByKeySpace(c *C) {
	storage := NewStorageWithMemoryBackend()
	testData := testGCSafePoints()
	r, e := storage.LoadGCSafePointByKeySpace("testKeySpace")
	c.Assert(r, IsNil)
	c.Assert(e, IsNil)
	for _, safePoint := range testData {
		err := storage.SaveGCSafePointByKeySpace(safePoint)
		c.Assert(err, IsNil)
		loaded, err := storage.LoadGCSafePointByKeySpace(safePoint.SpaceID)
		c.Assert(err, IsNil)
		c.Assert(safePoint, DeepEquals, loaded)
	}
}

func (s *testStorageGCSuite) TestLoadAllKeySpaceGCSafePoints(c *C) {
	storage := NewStorageWithMemoryBackend()
	testData := testGCSafePoints()
	for _, safePoint := range testData {
		err := storage.SaveGCSafePointByKeySpace(safePoint)
		c.Assert(err, IsNil)
	}
	gcSafePoints, err := storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	for i, safePoint := range testData {
		c.Assert(gcSafePoints[i], DeepEquals, safePoint)
	}

	// saving some service safe points.
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}
	spaceIDs := []string{
		"keySpace1",
		"keySpace2",
		"keySpace3",
	}

	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.SaveServiceSafePointByKeySpace(spaceID, serviceSafePoint), IsNil)
		}
	}

	// verify that service safe points does not interfere with gc safe points.
	gcSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	for i, safePoint := range testData {
		c.Assert(gcSafePoints[i], DeepEquals, safePoint)
	}

	// verify that when withGCSafePoint set to false, returned safe points are 0s.
	gcSafePoints, err = storage.LoadAllKeySpaceGCSafePoints(false)
	c.Assert(err, IsNil)
	for i, safePoint := range testData {
		safePoint.SafePoint = 0
		c.Assert(gcSafePoints[i], DeepEquals, safePoint)
	}
}

func (s *testStorageGCSuite) TestLoadAllKeySpaces(c *C) {
	storage := NewStorageWithMemoryBackend()
	keySpaces, err := storage.LoadAllKeySpaceGCSafePoints(true)
	c.Assert(err, IsNil)
	c.Assert(keySpaces, DeepEquals, []*endpoint.KeySpaceGCSafePoint{})
}

func (s *testStorageGCSuite) TestLoadServiceSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}
	spaceIDs := []string{
		"keySpace1",
		"keySpace2",
		"keySpace3",
	}

	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.SaveServiceSafePointByKeySpace(spaceID, serviceSafePoint), IsNil)
		}
	}
	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			key := endpoint.KeySpaceServiceSafePointPath(spaceID, serviceSafePoint.ServiceID)
			value, err := storage.Load(key)
			c.Assert(err, IsNil)
			ssp := &endpoint.ServiceSafePoint{}
			c.Assert(json.Unmarshal([]byte(value), ssp), IsNil)
			c.Assert(ssp, DeepEquals, serviceSafePoint)
		}
	}
}

func (s *testStorageGCSuite) TestRemoveServiceSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()

	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}
	spaceIDs := []string{
		"keySpace1",
		"keySpace2",
		"keySpace3",
	}
	// save service safe points
	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.SaveServiceSafePointByKeySpace(spaceID, serviceSafePoint), IsNil)
		}
	}

	// remove service safe points
	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.RemoveServiceSafePointByKeySpace(spaceID, serviceSafePoint.ServiceID), IsNil)
		}
	}

	// check that service safe points are empty
	for _, spaceID := range spaceIDs {
		for _, serviceSafePoint := range serviceSafePoints {
			safepoint, err := storage.LoadServiceSafePointByKeySpace(spaceID, serviceSafePoint.ServiceID)
			c.Assert(err, IsNil)
			c.Assert(safepoint, IsNil)
		}
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
		c.Assert(storage.SaveServiceSafePointByKeySpace(testKeySpace, serviceSafePoint), IsNil)
	}
	minSafePoint, err := storage.LoadMinServiceSafePointByKeySpace(testKeySpace, currentTime)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, DeepEquals, serviceSafePoints[0])

	// this should remove safePoint with ServiceID 0 due to expiration
	// and find the safePoint with ServiceID 1
	minSafePoint2, err := storage.LoadMinServiceSafePointByKeySpace(testKeySpace, currentTime.Add(150*time.Second))
	c.Assert(err, IsNil)
	c.Assert(minSafePoint2, DeepEquals, serviceSafePoints[1])

	// verify that service safe point with ServiceID 0 has been removed
	ssp, err := storage.LoadServiceSafePointByKeySpace(testKeySpace, "0")
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)

	// this should remove all service safe points
	// and return nil
	ssp, err = storage.LoadMinServiceSafePointByKeySpace(testKeySpace, currentTime.Add(500*time.Second))
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)
}
