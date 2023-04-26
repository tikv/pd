// Copyright 2023 TiKV Project Authors.
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
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)

	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	storage := NewStorageWithEtcdBackend(client, rootPath)

	key := "timestamp"
	expectedTS := time.Now().Round(0)
	err = storage.SaveTimestamp(key, expectedTS)
	re.NoError(err)
	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestGlobalLocalTimestamp(t *testing.T) {
	re := require.New(t)

	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	storage := NewStorageWithEtcdBackend(client, rootPath)

	ltaKey := "lta"
	timestampKey := "timestamp"
	dc1LocationKey, dc2LocationKey := "dc1", "dc2"
	localTS1 := time.Now().Round(0)
	l1 := path.Join(ltaKey, dc1LocationKey, timestampKey)
	l2 := path.Join(ltaKey, dc2LocationKey, timestampKey)

	err = storage.SaveTimestamp(l1, localTS1)
	re.NoError(err)
	globalTS := time.Now().Round(0)
	err = storage.SaveTimestamp(timestampKey, globalTS)
	re.NoError(err)
	localTS2 := time.Now().Round(0)
	err = storage.SaveTimestamp(l2, localTS2)
	re.NoError(err)
	// return the max ts between global and local
	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(localTS2, ts)
	// return the local ts for a given dc location
	ts, err = storage.LoadTimestamp(l1)
	re.NoError(err)
	re.Equal(localTS1, ts)
}

func TestTimestampTxn(t *testing.T) {
	re := require.New(t)

	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	storage := NewStorageWithEtcdBackend(client, rootPath)

	timestampKey := "timestamp"

	globalTS1 := time.Now().Round(0)
	err = storage.SaveTimestamp(timestampKey, globalTS1)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(timestampKey, globalTS2)
	re.Error(err)

	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(globalTS1, ts)
}
