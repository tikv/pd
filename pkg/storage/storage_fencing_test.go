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

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestLeaderLeaseStorageView(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	ctx := context.Background()
	const leaderKey = "/test-leader"
	lease, err := client.Grant(ctx, 60)
	re.NoError(err)
	_, err = client.Put(ctx, leaderKey, "member-a", clientv3.WithLease(lease.ID))
	re.NoError(err)
	local := NewStorageWithMemoryBackend()
	base := NewCoreStorage(NewStorageWithEtcdBackend(client), local)
	view, err := WithLeaderLease(base, leaderKey, lease.ID)
	re.NoError(err)
	re.NotSame(base, view)
	re.Same(local, RetrieveRegionStorage(view))
	re.Same(local, TrySwitchRegionStorage(base, true))
	region := &metapb.Region{Id: 1}
	re.NoError(view.SaveRegion(region))
	re.NoError(TryLoadRegionsOnce(ctx, view, func(*core.RegionInfo) []*core.RegionInfo { return nil }))
	re.True(AreRegionsLoaded(base))

	store := &metapb.Store{Id: 42, Address: "old"}
	re.NoError(view.SaveStoreMeta(store))
	_, err = client.Revoke(ctx, lease.ID)
	re.NoError(err)
	store.Address = "new"
	re.NoError(base.SaveStoreMeta(store))
	store.Address = "stale"
	re.ErrorIs(view.SaveStoreMeta(store), errs.ErrEtcdTxnConflict)
	re.ErrorIs(view.DeleteStoreMeta(store), errs.ErrEtcdTxnConflict)
	loaded := &metapb.Store{}
	ok, err := view.LoadStoreMeta(store.Id, loaded)
	re.NoError(err)
	re.True(ok)
	re.Equal("new", loaded.Address)

	// Local region writes and the shared switch remain usable after lease loss.
	re.NoError(view.SaveRegion(&metapb.Region{Id: 2}))
	re.NotNil(TrySwitchRegionStorage(base, false))
	re.ErrorIs(view.SaveRegion(region), errs.ErrEtcdTxnConflict)
	_, err = WithLeaderLease(base, leaderKey, clientv3.NoLease)
	re.ErrorIs(err, errs.ErrEtcdTxnConflict)
}
