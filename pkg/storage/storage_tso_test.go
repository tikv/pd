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

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

const (
	testGroupID     = uint32(1)
	testLeaderKey   = "test-leader-key"
	testLeaderValue = "test-leader-value"
)

func prepare(t *testing.T) (storage Storage, clean func(), leadership *election.Leadership) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	storage = NewStorageWithEtcdBackend(client)
	leadership = election.NewLeadership(client, testLeaderKey, "storage_tso_test")
	err := leadership.Campaign(60, testLeaderValue)
	require.NoError(t, err)
	return storage, clean, leadership
}

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()
	expectedTS := time.Now().Round(0)
	err := storage.SaveTimestamp(testGroupID, expectedTS, leadership)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(expectedTS, ts)
}

func TestTimestampTxn(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()
	globalTS1 := time.Now().Round(0)
	err := storage.SaveTimestamp(testGroupID, globalTS1, leadership)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(testGroupID, globalTS2, leadership)
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
	err := storage.SaveTimestamp(testGroupID, globalTS, leadership)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	err = storage.SaveTimestamp(testGroupID, globalTS.Add(time.Second), &election.Leadership{})
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> ""
	err = storage.Save(leadership.GetLeaderKey(), "")
	re.NoError(err)
	err = storage.SaveTimestamp(testGroupID, globalTS.Add(time.Second), leadership)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> non-existent
	err = storage.Remove(leadership.GetLeaderKey())
	re.NoError(err)
	err = storage.SaveTimestamp(testGroupID, globalTS.Add(time.Second), leadership)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> testLeaderValue
	err = storage.Save(leadership.GetLeaderKey(), testLeaderValue)
	re.NoError(err)
	globalTS = globalTS.Add(time.Second)
	err = storage.SaveTimestamp(testGroupID, globalTS, leadership)
	re.NoError(err)
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)
}
