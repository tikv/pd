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
<<<<<<< HEAD
	"path"
	"strconv"
=======
	"context"
>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
	"testing"
	"time"

	"github.com/stretchr/testify/require"
<<<<<<< HEAD
=======

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

<<<<<<< HEAD
=======
const (
	testGroupID     = uint32(1)
	testLeaderKey   = "test-leader-key"
	testLeaderValue = "test-leader-value"
)

var defaultContext = context.Background()

func prepare(t *testing.T) (storage Storage, clean func(), leadership *election.Leadership) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	storage = NewStorageWithEtcdBackend(client)
	leadership = election.NewLeadership(client, testLeaderKey, "storage_tso_test")
	err := leadership.Campaign(60, testLeaderValue)
	require.NoError(t, err)
	return storage, clean, leadership
}

func TestSaveTimestampWithTimeout(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/kv/slowTxn", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/kv/slowTxn"))
	}()
	storage, clean, leadership := prepare(t)
	defer clean()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := storage.SaveTimestamp(ctx, testGroupID, time.Now().Round(0), leadership)
	re.ErrorIs(err, context.DeadlineExceeded)
}

>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	expectedTS := time.Now().Round(0)
<<<<<<< HEAD
	err := storage.SaveTimestamp(keypath.TimestampKey, expectedTS)
=======
	err := storage.SaveTimestamp(defaultContext, testGroupID, expectedTS, leadership)
>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
	re.NoError(err)
	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestGlobalLocalTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestStorage(t)
	defer clean()
	ltaKey := "lta"
	dc1LocationKey, dc2LocationKey := "dc1", "dc2"
	localTS1 := time.Now().Round(0)
	l1 := path.Join(ltaKey, dc1LocationKey, keypath.TimestampKey)
	l2 := path.Join(ltaKey, dc2LocationKey, keypath.TimestampKey)

	err := storage.SaveTimestamp(l1, localTS1)
	re.NoError(err)
	globalTS := time.Now().Round(0)
	err = storage.SaveTimestamp(keypath.TimestampKey, globalTS)
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
	storage, clean := newTestStorage(t)
	defer clean()
	globalTS1 := time.Now().Round(0)
<<<<<<< HEAD
	err := storage.SaveTimestamp(keypath.TimestampKey, globalTS1)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(keypath.TimestampKey, globalTS2)
=======
	err := storage.SaveTimestamp(defaultContext, testGroupID, globalTS1, leadership)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS2, leadership)
>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
	re.Error(err)

	ts, err := storage.LoadTimestamp("")
	re.NoError(err)
	re.Equal(globalTS1, ts)
}

<<<<<<< HEAD
func newTestStorage(t *testing.T) (Storage, func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	return NewStorageWithEtcdBackend(client, rootPath), clean
=======
func TestSaveTimestampWithLeaderCheck(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()

	// testLeaderKey -> testLeaderValue
	globalTS := time.Now().Round(0)
	err := storage.SaveTimestamp(defaultContext, testGroupID, globalTS, leadership)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	err = storage.SaveTimestamp(context.Background(), testGroupID, globalTS.Add(time.Second), &election.Leadership{})
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> ""
	err = storage.Save(leadership.GetLeaderKey(), "")
	re.NoError(err)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS.Add(time.Second), leadership)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> non-existent
	err = storage.Remove(leadership.GetLeaderKey())
	re.NoError(err)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS.Add(time.Second), leadership)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> testLeaderValue
	err = storage.Save(leadership.GetLeaderKey(), testLeaderValue)
	re.NoError(err)
	globalTS = globalTS.Add(time.Second)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS, leadership)
	re.NoError(err)
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)
>>>>>>> f75df33d1d (tso: improve the high availability of etcd client for etcd save timestamp (#9986))
}
