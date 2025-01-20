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
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/utils/etcdutil"
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
	testLowLevelTxn(re, kv)
}

func TestLevelDB(t *testing.T) {
	re := require.New(t)
	dir := t.TempDir()
	kv, err := NewLevelDBKV(dir)
	re.NoError(err)

	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv, 20)
	testLowLevelTxn(re, kv)
}

func TestMemKV(t *testing.T) {
	re := require.New(t)
	kv := NewMemoryKV()
	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv, 20)
	testLowLevelTxn(re, kv)
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
		for i := range count {
			saveErr = txn.Save("key"+strconv.Itoa(i), "val"+strconv.Itoa(i))
			if saveErr != nil {
				return saveErr
			}
		}
		return nil
	})
	re.NoError(err)
	for i := range count {
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

func mustHaveKeys(re *require.Assertions, kv Base, prefix string, expected ...KeyValuePair) {
	keys, values, err := kv.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
	re.NoError(err)
	re.Equal(len(expected), len(keys))
	for i, key := range keys {
		re.Equal(expected[i].Key, key)
		re.Equal(expected[i].Value, values[i])
	}
}

func testLowLevelTxn(re *require.Assertions, kv Base) {
	// Test NotExists condition, putting in transaction.
	res, err := kv.CreateLowLevelTxn().If(
		LowLevelTxnCondition{
			Key:     "txn-k1",
			CmpType: LowLevelCmpNotExists,
		},
	).Then(
		LowLevelTxnOp{
			Key:    "txn-k1",
			OpType: LowLevelOpPut,
			Value:  "v1",
		},
		LowLevelTxnOp{
			Key:    "txn-k2",
			OpType: LowLevelOpPut,
			Value:  "v2",
		},
	).Else(
		LowLevelTxnOp{
			Key:    "txn-unexpected",
			OpType: LowLevelOpPut,
			Value:  "unexpected",
		},
	).Commit(context.Background())

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Items, 2)
	re.Len(res.Items[0].KeyValuePairs, 0)
	re.Len(res.Items[1].KeyValuePairs, 0)

	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k1", Value: "v1"}, KeyValuePair{Key: "txn-k2", Value: "v2"})

	// Test Equal condition; reading in transaction.
	res, err = kv.CreateLowLevelTxn().If(
		LowLevelTxnCondition{
			Key:     "txn-k1",
			CmpType: LowLevelCmpEqual,
			Value:   "v1",
		},
	).Then(
		LowLevelTxnOp{
			Key:    "txn-k2",
			OpType: LowLevelOpGet,
		},
	).Else(
		LowLevelTxnOp{
			Key:    "txn-unexpected",
			OpType: LowLevelOpPut,
			Value:  "unexpected",
		},
	).Commit(context.Background())

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Items, 1)
	re.Len(res.Items[0].KeyValuePairs, 1)
	re.Equal("v2", res.Items[0].KeyValuePairs[0].Value)
	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k1", Value: "v1"}, KeyValuePair{Key: "txn-k2", Value: "v2"})

	// Test NotEqual condition, else branch, reading range in transaction, reading & writing mixed.
	res, err = kv.CreateLowLevelTxn().If(
		LowLevelTxnCondition{
			Key:     "txn-k1",
			CmpType: LowLevelCmpNotEqual,
			Value:   "v1",
		},
	).Then(
		LowLevelTxnOp{
			Key:    "txn-unexpected",
			OpType: LowLevelOpPut,
			Value:  "unexpected",
		},
	).Else(
		LowLevelTxnOp{
			Key:    "txn-k1",
			OpType: LowLevelOpGetRange,
			EndKey: "txn-k2\x00",
		},
		LowLevelTxnOp{
			Key:    "txn-k3",
			OpType: LowLevelOpPut,
			Value:  "k3",
		},
	).Commit(context.Background())

	re.NoError(err)
	re.False(res.Succeeded)
	re.Len(res.Items, 2)
	re.Len(res.Items[0].KeyValuePairs, 2)
	re.Equal([]KeyValuePair{{Key: "txn-k1", Value: "v1"}, {Key: "txn-k2", Value: "v2"}}, res.Items[0].KeyValuePairs)
	re.Len(res.Items[1].KeyValuePairs, 0)

	mustHaveKeys(re, kv, "txn-",
		KeyValuePair{Key: "txn-k1", Value: "v1"},
		KeyValuePair{Key: "txn-k2", Value: "v2"},
		KeyValuePair{Key: "txn-k3", Value: "k3"})

	// Test Exists condition, deleting, overwriting.
	res, err = kv.CreateLowLevelTxn().If(
		LowLevelTxnCondition{
			Key:     "txn-k1",
			CmpType: LowLevelCmpExists,
		},
	).Then(
		LowLevelTxnOp{
			Key:    "txn-k1",
			OpType: LowLevelOpDelete,
		},
		LowLevelTxnOp{
			Key:    "txn-k2",
			OpType: LowLevelOpPut,
			Value:  "v22",
		},
		// Delete not existing key.
		LowLevelTxnOp{
			Key:    "txn-k4",
			OpType: LowLevelOpDelete,
		},
	).Else(
		LowLevelTxnOp{
			Key:    "txn-unexpected",
			OpType: LowLevelOpPut,
			Value:  "unexpected",
		},
	).Commit(context.Background())

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Items, 3)
	for _, item := range res.Items {
		re.Len(item.KeyValuePairs, 0)
	}

	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k2", Value: "v22"}, KeyValuePair{Key: "txn-k3", Value: "k3"})

	// Deleted keys can be regarded as not existing correctly.
	res, err = kv.CreateLowLevelTxn().If(
		LowLevelTxnCondition{
			Key:     "txn-k1",
			CmpType: LowLevelCmpNotExists,
		},
	).Then(
		LowLevelTxnOp{
			Key:    "txn-k2",
			OpType: LowLevelOpDelete,
		},
		LowLevelTxnOp{
			Key:    "txn-k3",
			OpType: LowLevelOpDelete,
		},
	).Commit(context.Background())

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Items, 2)
	for _, item := range res.Items {
		re.Len(item.KeyValuePairs, 0)
	}
	mustHaveKeys(re, kv, "txn-")

	// The following tests only check the correctness of the conditions.
	check := func(conditions []LowLevelTxnCondition, shouldSuccess bool) {

		res, err := kv.CreateLowLevelTxn().If(conditions...).Commit(context.Background())
		re.NoError(err)
		re.Equal(shouldSuccess, res.Succeeded)
	}

	// "txn-k1" doesn't exist at this point.
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpExists}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpNotExists}}, true)

	err = kv.Save("txn-k1", "v1")
	re.NoError(err)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpExists}}, true)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpNotExists}}, false)

	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v1"}}, true)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpNotEqual, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v2"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpNotEqual, Value: "v2"}}, true)

	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpLess, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpLess, Value: "v0"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpLess, Value: "v2"}}, true)

	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpGreater, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpGreater, Value: "v2"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpGreater, Value: "v0"}}, true)

	// Test comparing with not-existing key.
	err = kv.Remove("txn-k1")
	re.NoError(err)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpNotEqual, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpLess, Value: "v1"}}, false)
	check([]LowLevelTxnCondition{{Key: "txn-k1", CmpType: LowLevelCmpGreater, Value: "v1"}}, false)

	// Test the conditions are conjunctions.
	err = kv.Save("txn-k1", "v1")
	re.NoError(err)
	err = kv.Save("txn-k2", "v2")
	re.NoError(err)
	check([]LowLevelTxnCondition{
		{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v1"},
		{Key: "txn-k2", CmpType: LowLevelCmpEqual, Value: "v2"},
	}, true)
	check([]LowLevelTxnCondition{
		{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v1"},
		{Key: "txn-k2", CmpType: LowLevelCmpEqual, Value: "v0"},
	}, false)
	check([]LowLevelTxnCondition{
		{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v0"},
		{Key: "txn-k2", CmpType: LowLevelCmpEqual, Value: "v2"},
	}, false)
	check([]LowLevelTxnCondition{
		{Key: "txn-k1", CmpType: LowLevelCmpEqual, Value: "v0"},
		{Key: "txn-k2", CmpType: LowLevelCmpEqual, Value: "v0"},
	}, false)
}
