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

package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// TestDeleteExpectedPrimaryFlagRevokesLease reconstructs the corner case from issue
// #10875. The expected primary flag is bound to an etcd lease; if the key were
// deleted while its lease lingered, a later campaign would read an empty flag, skip
// the affinity guard, and then fail with ErrEtcdTxnConflict against the still-present
// leader key. DeleteExpectedPrimaryFlag must therefore remove both the key and its
// lease, so the "key deleted but lease persists" state can never exist.
func TestDeleteExpectedPrimaryFlagRevokesLease(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)
	const value = "http://127.0.0.1:2379"

	// Mark the flag bound to a fresh lease, exactly as TransferPrimary does.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	leaseID := grantResp.ID
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: value, output: value}, leaseID))

	// Sanity: the key exists and is bound to the lease.
	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)
	re.Equal(int64(leaseID), getResp.Kvs[0].Lease)

	// The newly elected primary cleans up the flag once it wins.
	DeleteExpectedPrimaryFlag(client, msParam, value)

	// The key is gone...
	getResp, err = client.Get(ctx, path)
	re.NoError(err)
	re.Empty(getResp.Kvs)
	// ...and so is its lease: TimeToLive returns -1 for a revoked/expired lease, which
	// is the guarantee that the #10875 "key deleted but lease persists" state is gone.
	ttlResp, err := client.TimeToLive(ctx, leaseID)
	re.NoError(err)
	re.Equal(int64(-1), ttlResp.TTL)
}

// TestDeleteExpectedPrimaryFlagSkipsOnValueMismatch ensures the conditional delete
// does not clobber a newer transfer. If a second transfer has already rewritten the
// flag (with its own lease) while this primary was winning, the stale winner must
// leave both the key and the newer lease intact.
func TestDeleteExpectedPrimaryFlagSkipsOnValueMismatch(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)

	// A newer transfer points the flag at "newer" with its own lease.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	leaseID := grantResp.ID
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "newer", output: "newer"}, leaseID))

	// A primary that campaigned for the older value tries to clean up; it must not.
	DeleteExpectedPrimaryFlag(client, msParam, "older")

	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)
	re.Equal("newer", string(getResp.Kvs[0].Value))
	ttlResp, err := client.TimeToLive(ctx, leaseID)
	re.NoError(err)
	re.Positive(ttlResp.TTL)
}

// TestIsSamePrimary covers the matching used by TransferPrimary to skip a
// self-transfer (#10970): a member matches by either its name or its service
// address, and an empty target never matches.
func TestIsSamePrimary(t *testing.T) {
	re := require.New(t)
	entry := discovery.ServiceRegistryEntry{Name: "tso-1", ServiceAddr: "http://127.0.0.1:2379"}
	re.True(isSamePrimary(entry, "tso-1"))                  // match by name
	re.True(isSamePrimary(entry, "http://127.0.0.1:2379"))  // match by service address
	re.False(isSamePrimary(entry, "tso-2"))                 // different name
	re.False(isSamePrimary(entry, "http://127.0.0.1:2380")) // different address
	re.False(isSamePrimary(entry, ""))                      // empty target never matches
}
