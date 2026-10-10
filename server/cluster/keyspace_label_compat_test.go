// Copyright 2026 TiKV Project Authors.
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

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

// TestCreateKeyspaceWritesCompatibilityLabelRule verifies that creating a
// keyspace still writes the keyspaces/{id} region label rule, purely as a
// rolling-upgrade fallback for a scheduling-server primary that predates the
// keyspace-meta watcher and so has no other way to learn about the keyspace.
func TestCreateKeyspaceWritesCompatibilityLabelRule(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	tc.regionLabeler, err = labeler.NewRegionLabeler(ctx, tc.storage, time.Second*5)
	re.NoError(err)

	keyspaceGroupManager := keyspace.NewKeyspaceGroupManager(ctx, tc.storage, tc.etcdClient)
	re.NoError(keyspaceGroupManager.Bootstrap(ctx))
	keyspaceManager := keyspace.NewKeyspaceManager(ctx, tc.storage, tc, mockid.NewIDAllocator(), &config.KeyspaceConfig{}, keyspaceGroupManager, nil)

	created, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "compat-test",
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)

	want := keyspace.MakeTxnLabelRule(created.GetId())
	got := tc.regionLabeler.GetLabelRule(want.ID)
	re.NotNil(got, "keyspace creation must still write the compatibility region label rule")
	re.Equal(want.Labels, got.Labels)
	re.Equal(want.RuleType, got.RuleType)
	gotRanges, ok := got.Data.([]*labeler.KeyRangeRule)
	re.True(ok)
	re.Len(gotRanges, 1)
	regionBounds := keyspace.MakeRegionBound(created.GetId())
	re.Equal(regionBounds.TxnLeftBound, gotRanges[0].StartKey)
	re.Equal(regionBounds.TxnRightBound, gotRanges[0].EndKey)
}

// TestCreateKeyspaceCleansUpCompatibilityLabelRuleOnFailure verifies that if
// keyspace creation fails after the compatibility label rule has already been
// written (here: waitKeyspaceRegionSplit times out, since no real split ever
// happens in this test's storage), the label rule is rolled back along with
// the keyspace's storage entries rather than being left behind as an orphan
// pointing at an ID that no longer exists.
func TestCreateKeyspaceCleansUpCompatibilityLabelRuleOnFailure(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	tc.regionLabeler, err = labeler.NewRegionLabeler(ctx, tc.storage, time.Second*5)
	re.NoError(err)

	keyspaceGroupManager := keyspace.NewKeyspaceGroupManager(ctx, tc.storage, tc.etcdClient)
	re.NoError(keyspaceGroupManager.Bootstrap(ctx))
	cfg := &config.KeyspaceConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.NewDuration(10 * time.Millisecond),
		CheckRegionSplitInterval: typeutil.NewDuration(time.Millisecond),
	}
	keyspaceManager := keyspace.NewKeyspaceManager(ctx, tc.storage, tc, mockid.NewIDAllocator(), cfg, keyspaceGroupManager, nil)

	_, err = keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "compat-rollback-test",
		CreateTime: time.Now().Unix(),
	})
	re.Error(err, "region split should time out since no matching region ever appears in this test's storage")

	// The keyspace never existed as far as callers can tell (creation
	// failed), so its compatibility label rule must not linger either.
	labelID := keyspace.MakeTxnLabelRule(1).ID
	re.Nil(tc.regionLabeler.GetLabelRule(labelID))
}
