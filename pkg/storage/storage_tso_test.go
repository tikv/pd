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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	testGroupID     = uint32(1)
	testLeaderKey   = "test-leader-key"
	testLeaderValue = "test-leader-value"
)

var defaultContext = context.Background()

func prepare(t *testing.T) (storage Storage, clean func(), leadership *election.Leadership) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	storage = NewStorageWithEtcdBackend(client)
	require.NoError(t, storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return storage.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{ID: testGroupID})
	}))
	leadership = election.NewLeadership(client, testLeaderKey, "storage_tso_test", "test_member")
	err := leadership.Campaign(60, testLeaderValue)
	require.NoError(t, err)
	return storage, clean, leadership
}

func TestSaveTimestampWithTimeout(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/kv/slowTxn", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/kv/slowTxn"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := storage.SaveTimestamp(ctx, testGroupID, time.Now().Round(0), leadership)
	re.ErrorIs(err, context.DeadlineExceeded)
}

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()
	expectedTS := time.Now().Round(0)
	err := storage.SaveTimestamp(defaultContext, testGroupID, expectedTS, leadership)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestSaveTimestampRequiresGroupMembership(t *testing.T) {
	storage, clean, leadership := prepare(t)
	defer clean()
	initialTS := time.Now().Round(0)
	require.NoError(t, storage.SaveTimestamp(t.Context(), testGroupID, initialTS, leadership))
	require.NoError(t, storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return storage.DeleteKeyspaceGroup(txn, testGroupID)
	}))

	nextTS := initialTS.Add(time.Second)
	tests := []struct {
		name    string
		groupID uint32
		wantErr error
		wantTS  time.Time
	}{
		{name: "deleted group", groupID: testGroupID, wantErr: errs.ErrEtcdTxnConflict, wantTS: initialTS},
		{name: "missing group", groupID: testGroupID + 1, wantErr: errs.ErrEtcdTxnConflict, wantTS: typeutil.ZeroTime},
		{name: "default group before bootstrap", groupID: constant.DefaultKeyspaceGroupID, wantTS: nextTS},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			re.ErrorIs(storage.SaveTimestamp(t.Context(), tt.groupID, nextTS, leadership), tt.wantErr)
			ts, err := storage.LoadTimestamp(tt.groupID)
			re.NoError(err)
			re.Equal(tt.wantTS, ts)
		})
	}
}

func TestTimestampTxn(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()
	globalTS1 := time.Now().Round(0)
	err := storage.SaveTimestamp(defaultContext, testGroupID, globalTS1, leadership)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS2, leadership)
	re.Error(err)

	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS1, ts)
}

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
}
