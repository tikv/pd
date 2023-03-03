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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

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

func TestSaveLoadServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints := testServiceSafePoints()
	for i := range testSpaceID {
		re.NoError(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i]))
	}
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		re.NoError(err)
		re.Equal(testSafePoints[i], loadedSafePoint)
	}
}

func TestLoadMinServiceSafePoint(t *testing.T) {
	re := require.New(t)
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

	testKeyspace := "test"
	for _, serviceSafePoint := range serviceSafePoints {
		re.NoError(storage.SaveServiceSafePoint(testKeyspace, serviceSafePoint))
	}
	// enabling failpoint to make expired key removal immediately observable
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/endpoint/removeExpiredKeys", "return(true)"))
	minSafePoint, err := storage.LoadMinServiceSafePoint(testKeyspace, currentTime)
	re.NoError(err)
	re.Equal(serviceSafePoints[0], minSafePoint)

	// the safePoint with ServiceID 0 should be removed due to expiration
	minSafePoint2, err := storage.LoadMinServiceSafePoint(testKeyspace, currentTime.Add(150*time.Second))
	re.NoError(err)
	re.Equal(serviceSafePoints[1], minSafePoint2)

	// verify that service safe point with ServiceID 0 has been removed
	ssp, err := storage.LoadServiceSafePoint(testKeyspace, "0")
	re.NoError(err)
	re.Nil(ssp)

	// all remaining service safePoints should be removed due to expiration
	ssp, err = storage.LoadMinServiceSafePoint(testKeyspace, currentTime.Add(500*time.Second))
	re.NoError(err)
	re.Nil(ssp)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/endpoint/removeExpiredKeys"))
}

func TestRemoveServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints := testServiceSafePoints()
	// save service safe points
	for i := range testSpaceID {
		re.NoError(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i]))
	}
	// remove saved service safe points
	for i := range testSpaceID {
		re.NoError(storage.RemoveServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID))
	}
	// check that service safe points are empty
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID)
		re.NoError(err)
		re.Nil(loadedSafePoint)
	}
}

func TestSaveLoadGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		testSpaceID := testSpaceIDs[i]
		testSafePoint := testSafePoints[i]
		err := storage.SaveKeyspaceGCSafePoint(testSpaceID, testSafePoint)
		re.NoError(err)
		loaded, err := storage.LoadKeyspaceGCSafePoint(testSpaceID)
		re.NoError(err)
		re.Equal(testSafePoint, loaded)
	}
}

func TestLoadAllKeyspaceGCSafePoints(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		err := storage.SaveKeyspaceGCSafePoint(testSpaceIDs[i], testSafePoints[i])
		re.NoError(err)
	}
	loadedSafePoints, err := storage.LoadAllKeyspaceGCSafePoints(true)
	re.NoError(err)
	for i := range loadedSafePoints {
		re.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		re.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}

	// saving some service safe points.
	spaceIDs, safePoints := testServiceSafePoints()
	for i := range spaceIDs {
		re.NoError(storage.SaveServiceSafePoint(spaceIDs[i], safePoints[i]))
	}

	// verify that service safe points do not interfere with gc safe points.
	loadedSafePoints, err = storage.LoadAllKeyspaceGCSafePoints(true)
	re.NoError(err)
	for i := range loadedSafePoints {
		re.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		re.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}

	// verify that when withGCSafePoint set to false, returned safePoints is 0
	loadedSafePoints, err = storage.LoadAllKeyspaceGCSafePoints(false)
	re.NoError(err)
	for i := range loadedSafePoints {
		re.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		re.Equal(uint64(0), loadedSafePoints[i].SafePoint)
	}
}

func TestLoadEmpty(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	// loading non-existing GC safepoint should return 0
	gcSafePoint, err := storage.LoadKeyspaceGCSafePoint("testKeyspace")
	re.NoError(err)
	re.Equal(uint64(0), gcSafePoint)

	// loading non-existing service safepoint should return nil
	serviceSafePoint, err := storage.LoadServiceSafePoint("testKeyspace", "testService")
	re.NoError(err)
	re.Nil(serviceSafePoint)

	// loading empty key spaces should return empty slices
	safePoints, err := storage.LoadAllKeyspaceGCSafePoints(true)
	re.NoError(err)
	re.Empty(safePoints)
}
