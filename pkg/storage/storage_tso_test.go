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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const testGroupID = uint32(1)

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	expectedTS := time.Now().Round(0)
	err := storage.SaveTimestamp(testGroupID, expectedTS)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(keypath.TimestampPath(testGroupID))
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestTimestampTxn(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	globalTS1 := time.Now().Round(0)
	err := storage.SaveTimestamp(testGroupID, globalTS1)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(testGroupID, globalTS2)
	re.Error(err)

	ts, err := storage.LoadTimestamp(keypath.TimestampPath(testGroupID))
	re.NoError(err)
	re.Equal(globalTS1, ts)
}

func newTestStorage(t *testing.T) (Storage, func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	return NewStorageWithEtcdBackend(client), clean
}
