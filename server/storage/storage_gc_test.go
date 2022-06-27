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

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/storage/endpoint"
)

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

func testServiceSafePoints() ([]uint32, []*endpoint.ServiceSafePoint, time.Time) {
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
	now := time.Now()
	expireAt := now.Add(100 * time.Second).Unix()
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
	return spaceIDs, serviceSafePoints, now
}

func TestSaveLoadServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints, now := testServiceSafePoints()
	for i := range testSpaceID {
		re.NoError(storage.SaveServiceSafePoint(testSpaceID[i], testSafePoints[i]))
	}
	for i := range testSpaceID {
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID, now)
		re.NoError(err)
		re.Equal(testSafePoints[i], loadedSafePoint)
	}
}

func TestLoadMinServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	now := time.Now()
	expireAt1 := now.Add(100 * time.Second).Unix()
	expireAt2 := now.Add(200 * time.Second).Unix()
	expireAt3 := now.Add(300 * time.Second).Unix()

	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "0", ExpiredAt: expireAt1, SafePoint: 100},
		{ServiceID: "1", ExpiredAt: expireAt2, SafePoint: 200},
		{ServiceID: "2", ExpiredAt: expireAt3, SafePoint: 300},
	}
	expectedGCWorker := &endpoint.ServiceSafePoint{
		ServiceID: "gc_worker",
		ExpiredAt: math.MaxInt64,
		SafePoint: 100,
	}
	testKeySpace := uint32(100)
	for _, serviceSafePoint := range serviceSafePoints {
		re.NoError(storage.SaveServiceSafePoint(testKeySpace, serviceSafePoint))
	}
	// first load should init gc worker to safe point 100
	minSafePoint, err := storage.LoadMinServiceSafePoint(testKeySpace, now)
	re.NoError(err)
	re.Equal(expectedGCWorker, minSafePoint)

	// increment time by 150 seconds
	// the safePoint with ServiceID 0 should now be removed due to expiration
	// but min safe point should still be 100 due to gc-worker inited during 1st load
	now = now.Add(150 * time.Second)
	minSafePoint2, err := storage.LoadMinServiceSafePoint(testKeySpace, now)
	re.NoError(err)
	re.Equal(expectedGCWorker, minSafePoint2)
	// verify that service safe point with ServiceID 0 has been removed
	ssp, err := storage.LoadServiceSafePoint(testKeySpace, "0", now)
	re.NoError(err)
	re.Nil(ssp)

	// increment time by another 300 seconds
	// the only remaining service safe point should be gc worker
	now = now.Add(300 * time.Second)
	minSafePoint3, err := storage.LoadMinServiceSafePoint(testKeySpace, now)
	re.NoError(err)
	re.Equal(expectedGCWorker, minSafePoint3)
	ssp, err = storage.LoadServiceSafePoint(testKeySpace, "2", now)
	re.NoError(err)
	re.Nil(ssp)
}

func TestRemoveServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceID, testSafePoints, now := testServiceSafePoints()
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
		loadedSafePoint, err := storage.LoadServiceSafePoint(testSpaceID[i], testSafePoints[i].ServiceID, now)
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

func TestLoadAllKeySpaceGCSafePoints(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testSpaceIDs, testSafePoints := testGCSafePoints()
	for i := range testSpaceIDs {
		err := storage.SaveKeyspaceGCSafePoint(testSpaceIDs[i], testSafePoints[i])
		re.NoError(err)
	}
	loadedSafePoints, err := storage.LoadAllKeyspaceGCSafePoints()
	re.NoError(err)
	for i := range loadedSafePoints {
		re.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		re.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}

	// saving some service safe points.
	spaceIDs, safePoints, _ := testServiceSafePoints()
	for i := range spaceIDs {
		re.NoError(storage.SaveServiceSafePoint(spaceIDs[i], safePoints[i]))
	}

	// verify that service safe points do not interfere with gc safe points.
	loadedSafePoints, err = storage.LoadAllKeyspaceGCSafePoints()
	re.NoError(err)
	for i := range loadedSafePoints {
		re.Equal(testSpaceIDs[i], loadedSafePoints[i].SpaceID)
		re.Equal(testSafePoints[i], loadedSafePoints[i].SafePoint)
	}
}

func TestLoadEmpty(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	// loading non-existing GC safepoint should return 0
	gcSafePoint, err := storage.LoadKeyspaceGCSafePoint(100)
	re.NoError(err)
	re.Equal(uint64(0), gcSafePoint)

	// loading non-existing service safepoint should return nil
	serviceSafePoint, err := storage.LoadServiceSafePoint(100, "testService", time.Now())
	re.NoError(err)
	re.Nil(serviceSafePoint)

	// loading empty keyspaces should return empty slices
	safePoints, err := storage.LoadAllKeyspaceGCSafePoints()
	re.NoError(err)
	re.Empty(safePoints)
}
