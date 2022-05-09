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

var _ = Suite(&testStorageFopGCSuite{})

type testStorageFopGCSuite struct {
}

func testGCSafePoints() []*endpoint.ServiceGroupGCSafePoint {
	return []*endpoint.ServiceGroupGCSafePoint{
		{
			ServiceGroupID: "testServiceGroup1",
			SafePoint:      0,
		},
		{
			ServiceGroupID: "testServiceGroup2",
			SafePoint:      1,
		},
		{
			ServiceGroupID: "testServiceGroup3",
			SafePoint:      4396,
		},
		{
			ServiceGroupID: "testServiceGroup4",
			SafePoint:      23333333333,
		},
		{
			ServiceGroupID: "testServiceGroup5",
			SafePoint:      math.MaxUint64,
		},
	}
}

func (s *testStorageFopGCSuite) TestLoadGCWorkerSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	testData := testGCSafePoints()
	r, e := storage.LoadGCSafePointByServiceGroup("testServiceGroup")
	c.Assert(r, IsNil)
	c.Assert(e, IsNil)
	for _, safePoint := range testData {
		err := storage.SaveGCSafePointByServiceGroup(safePoint)
		c.Assert(err, IsNil)
		loaded, err := storage.LoadGCSafePointByServiceGroup(safePoint.ServiceGroupID)
		c.Assert(err, IsNil)
		c.Assert(safePoint, DeepEquals, loaded)
	}
}

func (s *testStorageFopGCSuite) TestLoadAllServiceGroupGCSafePoints(c *C) {
	storage := NewStorageWithMemoryBackend()
	testData := testGCSafePoints()
	for _, safePoint := range testData {
		err := storage.SaveGCSafePointByServiceGroup(safePoint)
		c.Assert(err, IsNil)
	}
	safePoints, err := storage.LoadAllServiceGroupGCSafePoints()
	c.Assert(err, IsNil)
	for i, safePoint := range testData {
		c.Assert(safePoints[i], DeepEquals, safePoint)
	}
}

func (s *testStorageFopGCSuite) TestLoadAllServiceGroup(c *C) {
	storage := NewStorageWithMemoryBackend()
	serviceGroups, err := storage.LoadAllServiceGroups()
	c.Assert(err, IsNil)
	c.Assert(serviceGroups, DeepEquals, []string{"default_rawkv"})
}

func (s *testStorageFopGCSuite) TestLoadServiceSafePointByServiceGroup(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}
	serviceGroups := []string{
		"serviceGroup1",
		"serviceGroup2",
		"serviceGroup3",
	}

	for _, serviceGroup := range serviceGroups {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.SaveServiceSafePointByServiceGroup(serviceGroup, serviceSafePoint), IsNil)
		}
	}
	for _, serviceGroup := range serviceGroups {
		for _, serviceSafePoint := range serviceSafePoints {
			key := endpoint.GCServiceSafePointPathByServiceGroup(serviceGroup, serviceSafePoint.ServiceID)
			value, err := storage.Load(key)
			c.Assert(err, IsNil)
			ssp := &endpoint.ServiceSafePoint{}
			c.Assert(json.Unmarshal([]byte(value), ssp), IsNil)
			c.Assert(ssp, DeepEquals, serviceSafePoint)
		}
	}
}

func (s *testStorageFopGCSuite) TestRemoveServiceSafePointByServiceGroup(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()

	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}
	serviceGroups := []string{
		"serviceGroup1",
		"serviceGroup2",
		"serviceGroup3",
	}
	// save service safe points
	for _, serviceGroup := range serviceGroups {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.SaveServiceSafePointByServiceGroup(serviceGroup, serviceSafePoint), IsNil)
		}
	}

	// remove service safe points
	for _, serviceGroup := range serviceGroups {
		for _, serviceSafePoint := range serviceSafePoints {
			c.Assert(storage.RemoveServiceSafePointByServiceGroup(serviceGroup, serviceSafePoint.ServiceID), IsNil)
		}
	}

	// check that service safe points are empty
	for _, serviceGroup := range serviceGroups {
		for _, serviceSafePoint := range serviceSafePoints {
			safepoint, err := storage.LoadServiceSafePointByServiceGroup(serviceGroup, serviceSafePoint.ServiceID)
			c.Assert(err, IsNil)
			c.Assert(safepoint, IsNil)
		}
	}
}

func (s *testStorageFopGCSuite) TestLoadMinServiceSafePointByServiceGroup(c *C) {
	storage := NewStorageWithMemoryBackend()
	currentTime := time.Now()
	expireAt1 := currentTime.Add(100 * time.Second).Unix()
	expireAt2 := currentTime.Add(200 * time.Second).Unix()
	expireAt3 := currentTime.Add(300 * time.Second).Unix()

	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt1, SafePoint: 100},
		{ServiceID: "2", ExpiredAt: expireAt2, SafePoint: 200},
		{ServiceID: "3", ExpiredAt: expireAt3, SafePoint: 300},
	}

	for _, serviceSafePoint := range serviceSafePoints {
		c.Assert(storage.SaveServiceSafePointByServiceGroup("testServiceGroup1", serviceSafePoint), IsNil)
	}
	minSafePoint, err := storage.LoadMinServiceSafePointByServiceGroup("testServiceGroup1", currentTime)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, DeepEquals, serviceSafePoints[0])

	// this should remove safePoint with ServiceID 1 due to expiration
	// and find the safePoint with ServiceID 2
	minSafePoint2, err := storage.LoadMinServiceSafePointByServiceGroup("testServiceGroup1", currentTime.Add(150*time.Second))
	c.Assert(err, IsNil)
	c.Assert(minSafePoint2, DeepEquals, serviceSafePoints[1])

	// verify that one with ServiceID 1 has been removed
	ssp, err := storage.LoadServiceSafePointByServiceGroup("testServiceGroup1", "1")
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)

	// this should remove all service safe points
	// and return nil
	ssp, err = storage.LoadMinServiceSafePointByServiceGroup("testServiceGroup1", currentTime.Add(500*time.Second))
	c.Assert(err, IsNil)
	c.Assert(ssp, IsNil)
}
