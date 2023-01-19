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

	keyspaceGroupName := "test"
	expectedTS := time.Now().Round(0)
	err = storage.SaveTimestamp(keyspaceGroupName, expectedTS)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(keyspaceGroupName)
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

	keyspaceGroupName := "test"
	dc1LocationKey, dc2LocationKey := "lts/dc1", "lts/dc2"
	localTS1 := time.Now().Round(0)
	err = storage.SaveTimestamp(keyspaceGroupName, localTS1, dc1LocationKey)
	re.NoError(err)
	globalTS := time.Now().Round(0)
	err = storage.SaveTimestamp(keyspaceGroupName, globalTS)
	re.NoError(err)
	localTS2 := time.Now().Round(0)
	err = storage.SaveTimestamp(keyspaceGroupName, localTS2, dc2LocationKey)
	re.NoError(err)
	// return the max ts between global and local
	ts, err := storage.LoadTimestamp(keyspaceGroupName)
	re.NoError(err)
	re.Equal(localTS2, ts)
	// return the local ts for a given dc location
	ts, err = storage.LoadTimestamp(keyspaceGroupName, dc1LocationKey)
	re.NoError(err)
	re.Equal(localTS1, ts)
}
