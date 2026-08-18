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
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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
	err := storage.SaveTimestamp(ctx, testGroupID, time.Now().Round(0), leadership, false)
	re.ErrorIs(err, context.DeadlineExceeded)
}

func TestSaveLoadTimestamp(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()
	expectedTS := time.Now().Round(0)
	err := storage.SaveTimestamp(defaultContext, testGroupID, expectedTS, leadership, false)
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
	err := storage.SaveTimestamp(defaultContext, testGroupID, globalTS1, leadership, false)
	re.NoError(err)

	globalTS2 := globalTS1.Add(-time.Millisecond).Round(0)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS2, leadership, false)
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
	err := storage.SaveTimestamp(defaultContext, testGroupID, globalTS, leadership, false)
	re.NoError(err)
	ts, err := storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	err = storage.SaveTimestamp(context.Background(), testGroupID, globalTS.Add(time.Second), &election.Leadership{}, false)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> ""
	err = storage.Save(leadership.GetLeaderKey(), "")
	re.NoError(err)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS.Add(time.Second), leadership, false)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> non-existent
	err = storage.Remove(leadership.GetLeaderKey())
	re.NoError(err)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS.Add(time.Second), leadership, false)
	re.True(errs.IsLeaderChanged(err))
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// testLeaderKey -> testLeaderValue
	err = storage.Save(leadership.GetLeaderKey(), testLeaderValue)
	re.NoError(err)
	globalTS = globalTS.Add(time.Second)
	err = storage.SaveTimestamp(defaultContext, testGroupID, globalTS, leadership, false)
	re.NoError(err)
	ts, err = storage.LoadTimestamp(testGroupID)
	re.NoError(err)
	re.Equal(globalTS, ts)
}

func TestSaveTimestampCheckTSOPrimary(t *testing.T) {
	re := require.New(t)
	storage, clean, leadership := prepare(t)
	defer clean()

	// No TSO primary exists: checkTSOPrimary=true should succeed.
	globalTS := time.Now().Round(0)
	re.NoError(storage.SaveTimestamp(defaultContext, 0, globalTS, leadership, true))
	ts, err := storage.LoadTimestamp(0)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// Simulate a TSO microservice primary by writing to the primary key.
	tsoPrimaryPath := keypath.ElectionPath(&keypath.MsParam{ServiceName: constant.TSOServiceName})
	re.NoError(storage.Save(tsoPrimaryPath, "tso-primary-member-info"))

	// Now checkTSOPrimary=true should fail.
	nextTS := globalTS.Add(time.Second)
	err = storage.SaveTimestamp(defaultContext, 0, nextTS, leadership, true)
	re.Error(err)
	re.Contains(err.Error(), "tso microservice primary exists, PD in PD service mode must yield")
	// Timestamp must not advance.
	ts, err = storage.LoadTimestamp(0)
	re.NoError(err)
	re.Equal(globalTS, ts)

	// checkTSOPrimary=false should still succeed (microservice-aware PD or TSO itself).
	re.NoError(storage.SaveTimestamp(defaultContext, 0, nextTS, leadership, false))
	ts, err = storage.LoadTimestamp(0)
	re.NoError(err)
	re.Equal(nextTS, ts)
}
