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
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	expectedTS := time.Now().Round(0)
	err := storage.SaveTimestamp(keypath.TimestampKey, expectedTS)
	re.NoError(err)
	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestTimestampTxn(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	globalTS1 := time.Now().Round(0)
	err := storage.SaveTimestamp(keypath.TimestampKey, globalTS1)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(keypath.TimestampKey, globalTS2)
	re.Error(err)

	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(globalTS1, ts)
}

func TestSaveTimestampCheckTSOPrimary(t *testing.T) {
	re := require.New(t)
	storage, client, clean := newTestStorageWithClient(t)
	defer clean()

	tsoPrimaryPath := keypath.LeaderPath(&keypath.MsParam{
		ServiceName: constant.TSOServiceName,
		GroupID:     constant.DefaultKeyspaceGroupID,
	})
	globalTS := time.Now().Round(0)
	re.NoError(storage.SaveTimestamp(keypath.TimestampKey, globalTS, tsoPrimaryPath))
	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(globalTS, ts)

	_, err = client.Put(context.Background(), tsoPrimaryPath, "tso-primary")
	re.NoError(err)

	nextTS := globalTS.Add(time.Second)
	err = storage.SaveTimestamp(keypath.TimestampKey, nextTS, tsoPrimaryPath)
	re.Error(err)
	re.Contains(err.Error(), "tso microservice primary exists, pd must yield embedded tso")
	ts, err = storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(globalTS, ts)
}

func newTestStorage(t *testing.T) (Storage, func()) {
	storage, _, clean := newTestStorageWithClient(t)
	return storage, clean
}

func newTestStorageWithClient(t *testing.T) (Storage, *clientv3.Client, func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	return NewStorageWithEtcdBackend(client, rootPath), client, clean
}
