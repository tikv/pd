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
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestEtcd(t *testing.T) {
	re := require.New(t)
	cfg := newTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	kv := NewEtcdKVBase(client, rootPath)
	testReadWrite(re, kv)
	testRange(re, kv)
}

func TestLevelDB(t *testing.T) {
	re := require.New(t)
	dir := t.TempDir()
	kv, err := NewLevelDBKV(dir)
	re.NoError(err)

	testReadWrite(re, kv)
	testRange(re, kv)
}

func TestMemKV(t *testing.T) {
	re := require.New(t)
	kv := NewMemoryKV()
	testReadWrite(re, kv)
	testRange(re, kv)
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

func newTestSingleConfig(t *testing.T) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir = t.TempDir()
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.ListenPeerUrls = []url.URL{*pu}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.ListenClientUrls = []url.URL{*cu}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.ListenPeerUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}
