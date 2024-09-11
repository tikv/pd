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
	"github.com/tikv/pd/pkg/utils/keypath"
)

func testGCSafePoints() []*endpoint.GCSafePointV2 {
	gcSafePoint := []*endpoint.GCSafePointV2{
		{KeyspaceID: uint32(1), SafePoint: 0},
		{KeyspaceID: uint32(2), SafePoint: 1},
		{KeyspaceID: uint32(3), SafePoint: 4396},
		{KeyspaceID: uint32(4), SafePoint: 23333333333},
		{KeyspaceID: uint32(5), SafePoint: math.MaxUint64},
	}

	return gcSafePoint
}

func testServiceSafePoints() []*endpoint.ServiceSafePointV2 {
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePointV2{
		{KeyspaceID: uint32(1), ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{KeyspaceID: uint32(1), ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{KeyspaceID: uint32(1), ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
		{KeyspaceID: uint32(2), ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{KeyspaceID: uint32(2), ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{KeyspaceID: uint32(2), ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
		{KeyspaceID: uint32(3), ServiceID: "service1", ExpiredAt: expireAt, SafePoint: 1},
		{KeyspaceID: uint32(3), ServiceID: "service2", ExpiredAt: expireAt, SafePoint: 2},
		{KeyspaceID: uint32(3), ServiceID: "service3", ExpiredAt: expireAt, SafePoint: 3},
	}
	return serviceSafePoints
}

func TestSaveLoadServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testServiceSafepoints := testServiceSafePoints()
	for i := range testServiceSafepoints {
		re.NoError(storage.SaveServiceSafePointV2(testServiceSafepoints[i]))
	}
	for i := range testServiceSafepoints {
		loadedServiceSafePoint, err := storage.LoadServiceSafePointV2(testServiceSafepoints[i].KeyspaceID, testServiceSafepoints[i].ServiceID)
		re.NoError(err)
		re.Equal(testServiceSafepoints[i].SafePoint, loadedServiceSafePoint.SafePoint)
	}
}

func TestLoadMinServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	currentTime := time.Now()
	expireAt1 := currentTime.Add(1000 * time.Second).Unix()
	expireAt2 := currentTime.Add(2000 * time.Second).Unix()
	expireAt3 := currentTime.Add(3000 * time.Second).Unix()

	testKeyspaceID := uint32(1)
	serviceSafePoints := []*endpoint.ServiceSafePointV2{
		{KeyspaceID: testKeyspaceID, ServiceID: "0", ExpiredAt: expireAt1, SafePoint: 300},
		{KeyspaceID: testKeyspaceID, ServiceID: "1", ExpiredAt: expireAt2, SafePoint: 400},
		{KeyspaceID: testKeyspaceID, ServiceID: "2", ExpiredAt: expireAt3, SafePoint: 500},
	}

	for _, serviceSafePoint := range serviceSafePoints {
		re.NoError(storage.SaveServiceSafePointV2(serviceSafePoint))
	}
	// enabling failpoint to make expired key removal immediately observable
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/endpoint/removeExpiredKeys", "return(true)"))
	minSafePoint, err := storage.LoadMinServiceSafePointV2(testKeyspaceID, currentTime)
	re.NoError(err)
	re.Equal(serviceSafePoints[0].SafePoint, minSafePoint.SafePoint)

	// gc_worker service safepoint will not be removed.
	ssp, err := storage.LoadMinServiceSafePointV2(testKeyspaceID, currentTime.Add(5000*time.Second))
	re.NoError(err)
	re.Equal(keypath.GCWorkerServiceSafePointID, ssp.ServiceID)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/endpoint/removeExpiredKeys"))
}

func TestRemoveServiceSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testServiceSafepoint := testServiceSafePoints()
	// save service safe points
	for _, serviceSafePoint := range testServiceSafepoint {
		re.NoError(storage.SaveServiceSafePointV2(serviceSafePoint))
	}
	// remove saved service safe points
	for _, serviceSafePoint := range testServiceSafepoint {
		re.NoError(storage.RemoveServiceSafePointV2(serviceSafePoint.KeyspaceID, serviceSafePoint.ServiceID))
	}
	// check that service safe points are empty
	for i := range testServiceSafepoint {
		loadedSafePoint, err := storage.LoadServiceSafePointV2(testServiceSafepoint[i].KeyspaceID, testServiceSafepoint[i].ServiceID)
		re.NoError(err)
		re.Nil(loadedSafePoint)
	}
}

func TestSaveLoadGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testGCSafePoints := testGCSafePoints()
	for _, testGCSafePoint := range testGCSafePoints {
		testSpaceID := testGCSafePoint.KeyspaceID
		testSafePoint := testGCSafePoint.SafePoint
		err := storage.SaveGCSafePointV2(testGCSafePoint)
		re.NoError(err)
		loadGCSafePoint, err := storage.LoadGCSafePointV2(testSpaceID)
		re.NoError(err)
		re.Equal(testSafePoint, loadGCSafePoint.SafePoint)
	}

	_, err2 := storage.LoadGCSafePointV2(999)
	re.NoError(err2)
}

func TestLoadEmpty(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	// loading non-existing GC safepoint should return 0
	gcSafePoint, err := storage.LoadGCSafePointV2(1)
	re.NoError(err)
	re.Equal(uint64(0), gcSafePoint.SafePoint)

	// loading non-existing service safepoint should return nil
	serviceSafePoint, err := storage.LoadServiceSafePointV2(1, "testService")
	re.NoError(err)
	re.Nil(serviceSafePoint)
}
