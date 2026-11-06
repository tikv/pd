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

package affinity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

type targetReplyStorage struct {
	endpoint.AffinityStorage
	beforeCommit func()
	loseReply    bool
}

func (s *targetReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.AffinityStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		return nil
	})
	if err == nil && s.loseReply {
		return errors.New("committed reply lost")
	}
	return err
}

func TestTargetCASAndLostReplyWithEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	m := newTargetTestManagerWithStorage(t, store)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	other := newTargetTestManagerWithStorage(t, store)
	stale := targetFor(m, "g", 1, 1, 2, 3)
	wrapped := &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
		_, err := other.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
		re.NoError(err)
	}}
	m.storage = wrapped
	written, err := m.UpdateAffinityGroupTargets([]GroupTarget{stale})
	re.Error(err, "another Manager wrote after the read, before commit")
	re.Empty(written)
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Zero(m.GetAffinityGroupState("g").LeaderStoreID)
	wrapped.beforeCommit = nil
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{stale})
	re.NoError(err)
	re.Empty(written)
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)

	target := targetFor(m, "g", 3, 1, 2, 3)
	wrapped.loseReply = true
	_, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.ErrorContains(err, "committed reply lost")
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
	re.Equal(uint64(3), loadStoredGroup(re, store, "g").LeaderStoreID)
	wrapped.loseReply = false
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(*loadStoredGroup(re, store, "g"), m.GetAffinityGroupState("g").Group)
}
