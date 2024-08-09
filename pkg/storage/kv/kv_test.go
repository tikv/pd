// Copyright 2017 TiKV Project Authors.
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

package kv

import (
	"context"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	kv := NewEtcdKVBase(client, rootPath)
	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv, 20)
	testLoadConflict(re, kv)
}

func TestLevelDB(t *testing.T) {
	re := require.New(t)
	dir := t.TempDir()
	kv, err := NewLevelDBKV(dir)
	re.NoError(err)

	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv, 20)
}

func TestMemKV(t *testing.T) {
	re := require.New(t)
	kv := NewMemoryKV()
	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv, 20)
}

func testReadWrite(re *require.Assertions, kv Base) {
	v, err := kv.Load("key")
	re.NoError(err)
	re.Equal("", v)
	err = kv.Save("key", "value")
	re.NoError(err)
	v, err = kv.Load("key")
	re.NoError(err)
	re.Equal("value", v)
	err = kv.Remove("key")
	re.NoError(err)
	v, err = kv.Load("key")
	re.NoError(err)
	re.Equal("", v)
	err = kv.Remove("key")
	re.NoError(err)
}

func testRange(re *require.Assertions, kv Base) {
	keys := []string{
		"test-a", "test-a/a", "test-a/ab",
		"test", "test/a", "test/ab",
		"testa", "testa/a", "testa/ab",
	}
	for _, k := range keys {
		err := kv.Save(k, k)
		re.NoError(err)
	}
	sortedKeys := append(keys[:0:0], keys...)
	sort.Strings(sortedKeys)

	testCases := []struct {
		start, end string
		limit      int
		expect     []string
	}{
		{start: "", end: "z", limit: 100, expect: sortedKeys},
		{start: "", end: "z", limit: 3, expect: sortedKeys[:3]},
		{start: "testa", end: "z", limit: 3, expect: []string{"testa", "testa/a", "testa/ab"}},
		{start: "test/", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test/a", "test/ab"}},
		{start: "test-a/", end: clientv3.GetPrefixRangeEnd("test-a/"), limit: 100, expect: []string{"test-a/a", "test-a/ab"}},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test"), limit: 100, expect: sortedKeys},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test", "test-a", "test-a/a", "test-a/ab", "test/a", "test/ab"}},
	}

	for _, testCase := range testCases {
		ks, vs, err := kv.LoadRange(testCase.start, testCase.end, testCase.limit)
		re.NoError(err)
		re.Equal(testCase.expect, ks)
		re.Equal(testCase.expect, vs)
	}
}

func testSaveMultiple(re *require.Assertions, kv Base, count int) {
	err := kv.RunInTxn(context.Background(), func(txn Txn) error {
		var saveErr error
		for i := 0; i < count; i++ {
			saveErr = txn.Save("key"+strconv.Itoa(i), "val"+strconv.Itoa(i))
			if saveErr != nil {
				return saveErr
			}
		}
		return nil
	})
	re.NoError(err)
	for i := 0; i < count; i++ {
		val, loadErr := kv.Load("key" + strconv.Itoa(i))
		re.NoError(loadErr)
		re.Equal("val"+strconv.Itoa(i), val)
	}
}

// testLoadConflict checks that if any value loaded during the current transaction
// has been modified by another transaction before the current one commit,
// then the current transaction must fail.
func testLoadConflict(re *require.Assertions, kv Base) {
	re.NoError(kv.Save("testKey", "initialValue"))
	// loader loads the test key value.
	loader := func(txn Txn) error {
		_, err := txn.Load("testKey")
		if err != nil {
			return err
		}
		return nil
	}
	// When no other writer, loader must succeed.
	re.NoError(kv.RunInTxn(context.Background(), loader))

	conflictLoader := func(txn Txn) error {
		_, err := txn.Load("testKey")
		// update key after load.
		re.NoError(kv.Save("testKey", "newValue"))
		if err != nil {
			return err
		}
		return nil
	}
	// When other writer exists, loader must error.
	re.Error(kv.RunInTxn(context.Background(), conflictLoader))
}
