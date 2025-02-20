// Copyright 2024 TiKV Project Authors.
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

package endpoint

import (
	"math"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func newStorageEndpoint(t *testing.T) (se *StorageEndpoint, clean func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	kvBase := kv.NewEtcdKVBase(client, rootPath)

	s := NewStorageEndpoint(kvBase, nil)
	return s, clean
}

func TestGCBarriersConversions(t *testing.T) {
	re := require.New(t)

	t1 := time.Date(2025, 2, 20, 15, 30, 00, 0, time.Local)
	t2 := t1.Add(time.Minute)
	t3 := t1.Add(time.Millisecond)
	t4 := t1.Add(time.Millisecond * 999)

	gcBarriers := []*GCBarrier{
		NewGCBarrier(constant.NullKeyspaceID, "a", 1, nil),
		NewGCBarrier(0, "b", 2, &t1),
		NewGCBarrier(1000, "c", uint64(t1.UnixMilli())<<18, &t2),
		NewGCBarrier(1000, "d", math.MaxUint64-1, &t3),
		NewGCBarrier(constant.NullKeyspaceID, "e", 456139133457530881, &t4),
	}

	// Check t3 & t4 are rounded
	t3Rounded := time.Date(2025, 2, 20, 15, 30, 01, 0, time.Local)
	re.Equal(t3Rounded, *gcBarriers[3].ExpirationTime)
	re.Equal(t3Rounded, *gcBarriers[4].ExpirationTime)

	serviceSafePoints := []*ServiceSafePoint{
		{ServiceID: "a", ExpiredAt: math.MaxInt64, SafePoint: 1, KeyspaceID: constant.NullKeyspaceID},
		{ServiceID: "b", ExpiredAt: t1.Unix(), SafePoint: 2, KeyspaceID: 0},
		{ServiceID: "c", ExpiredAt: t2.Unix(), SafePoint: uint64(t1.UnixMilli()) << 18, KeyspaceID: 1000},
		{ServiceID: "d", ExpiredAt: t3Rounded.Unix(), SafePoint: math.MaxUint64 - 1, KeyspaceID: 1000},
		{ServiceID: "e", ExpiredAt: t3Rounded.Unix(), SafePoint: 456139133457530881, KeyspaceID: constant.NullKeyspaceID},
	}

	// Test representing GC barriers by service safe points.
	for i, gcBarrier := range gcBarriers {
		expectedServiceSafePoint := serviceSafePoints[i]
		serviceSafePoint := gcBarrier.toServiceSafePoint()
		re.Equal(expectedServiceSafePoint, serviceSafePoint)
	}

	for i, serviceSafePoint := range serviceSafePoints {
		expectedGCBarrier := gcBarriers[i]
		gcBarrier := gcBarrierFromServiceSafePoint(serviceSafePoint)
		re.Equal(expectedGCBarrier, gcBarrier)
	}
}

func TestGCStateJSONUtil(t *testing.T) {
	se, clean := newStorageEndpoint(t)
	defer clean()
	p := newGCStateProvider(se)
	re := require.New(t)

	writeJSON := func(key string, value any) {
		err := p.RunInGCMetaTransaction(func(wb *GCStateWriteBatch) error {
			return wb.writeJson(key, value)
		})
		re.NoError(err)
	}

	loadValue := func(key string) string {
		v, err := se.Load(key)
		re.NoError(err)
		return v
	}

	writeJSON("dir1/k1", int64(1))
	writeJSON("dir1/k2", int64(2))
	re.Equal("1", loadValue("dir1/k1"))
	re.Equal("2", loadValue("dir1/k2"))
	vint, err := loadJSON[int64](se, "dir1/k1")
	re.NoError(err)
	re.Equal(int64(1), vint)
	vint, err = loadJSON[int64](se, "dir1/k2")
	re.NoError(err)
	re.Equal(int64(2), vint)
	keys, vints, err := loadJSONByPrefix[int64](se, "dir1/", 0)
	re.NoError(err)
	re.Equal([]string{"dir1/k1", "dir1/k2"}, keys)
	re.Equal([]int64{1, 2}, vints)
	// Non-zero limit takes effect
	keys, vints, err = loadJSONByPrefix[int64](se, "dir1/", 1)
	re.NoError(err)
	re.Equal([]string{"dir1/k1"}, keys)
	re.Equal([]int64{1}, vints)

	writeJSON("dir2/k1", "str")
	re.Equal(`"str"`, loadValue("dir2/k1"))
	vstr, err := loadJSON[string](se, "dir2/k1")
	re.NoError(err)
	re.Equal("str", vstr)

	writeJSON("dir3/k1", new(int64))
	writeJSON("dir3/k2", nil)
	re.Equal("0", loadValue("dir3/k1"))
	re.Equal("null", loadValue("dir3/k2"))
	vpint, err := loadJSON[*int64](se, "dir3/k1")
	re.NoError(err)
	re.Equal(int64(0), *vpint)
	vpint, err = loadJSON[*int64](se, "dir3/k2")
	re.NoError(err)
	re.Nil(vpint)
	keys, vpints, err := loadJSONByPrefix[*int64](se, "dir3/", 0)
	re.NoError(err)
	re.Equal([]string{"dir3/k1", "dir3/k2"}, keys)
	re.Equal([]*int64{new(int64), nil}, vpints)

	ssp := &ServiceSafePoint{
		ServiceID:  "testsvc",
		ExpiredAt:  math.MaxInt64,
		SafePoint:  456139133457530881,
		KeyspaceID: constant.NullKeyspaceID,
	}
	writeJSON("dir4/k1", ssp)
	re.Equal(`{"service_id":"testsvc","expired_at":9223372036854775807,"safe_point":456139133457530881,"keyspace_id":4294967295}`, loadValue("dir4/k1"))
	loadedSsp, err := loadJSON[*ServiceSafePoint](se, "dir4/k1")
	re.NoError(err)
	re.Equal(ssp, loadedSsp)
}
