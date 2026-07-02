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

package utils_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestTransferPrimaryToSelfKeepsExpectedPrimaryLease(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	keypath.SetClusterID(uint64(time.Now().UnixNano()))
	defer keypath.ResetClusterID()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const (
		primaryName   = "tso-primary"
		primaryAddr   = "http://127.0.0.1:10001"
		secondaryName = "tso-secondary"
		secondaryAddr = "http://127.0.0.1:10002"
	)
	putRegistryEntry(re, ctx, client, primaryName, primaryAddr)
	putRegistryEntry(re, ctx, client, secondaryName, secondaryAddr)

	lease := election.NewLease(client, "tso expected primary 00000")
	re.NoError(lease.Grant(constant.DefaultLease))
	defer func() {
		_ = lease.Close()
	}()

	msParam := &keypath.MsParam{
		ServiceName: constant.TSOServiceName,
		GroupID:     0,
	}
	expectedPrimaryPath := keypath.ExpectedPrimaryPath(msParam)
	_, err := client.Put(ctx, expectedPrimaryPath, primaryAddr, clientv3.WithLease(leaseID(lease)))
	re.NoError(err)

	beforeResp, err := client.Get(ctx, expectedPrimaryPath)
	re.NoError(err)
	re.Len(beforeResp.Kvs, 1)
	oldExpectedPrimary := beforeResp.Kvs[0]
	re.Equal(int64(leaseID(lease)), oldExpectedPrimary.Lease)

	err = mcsutils.TransferPrimary(client, lease, constant.TSOServiceName,
		primaryName, primaryName, 0, map[string]bool{
			primaryAddr:   true,
			secondaryAddr: true,
		})
	re.NoError(err)

	afterResp, err := client.Get(ctx, expectedPrimaryPath)
	re.NoError(err)
	re.Len(afterResp.Kvs, 1)
	currentExpectedPrimary := afterResp.Kvs[0]
	re.Equal(string(oldExpectedPrimary.Value), string(currentExpectedPrimary.Value))
	re.Equal(oldExpectedPrimary.ModRevision, currentExpectedPrimary.ModRevision)
	re.Equal(oldExpectedPrimary.Lease, currentExpectedPrimary.Lease)
}

func TestTransferPrimaryRandomKeepsExistingBehavior(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	keypath.SetClusterID(uint64(time.Now().UnixNano()))
	defer keypath.ResetClusterID()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const (
		primaryName   = "tso-primary"
		primaryAddr   = "http://127.0.0.1:10001"
		secondaryName = "tso-secondary"
		secondaryAddr = "http://127.0.0.1:10002"
	)
	putRegistryEntry(re, ctx, client, primaryName, primaryAddr)
	putRegistryEntry(re, ctx, client, secondaryName, secondaryAddr)

	lease := election.NewLease(client, "tso expected primary 00000")
	re.NoError(lease.Grant(constant.DefaultLease))

	expectedPrimaryPath := keypath.ExpectedPrimaryPath(&keypath.MsParam{
		ServiceName: constant.TSOServiceName,
		GroupID:     0,
	})
	_, err := client.Put(ctx, expectedPrimaryPath, primaryAddr, clientv3.WithLease(leaseID(lease)))
	re.NoError(err)

	beforeResp, err := client.Get(ctx, expectedPrimaryPath)
	re.NoError(err)
	re.Len(beforeResp.Kvs, 1)

	err = mcsutils.TransferPrimary(client, lease, constant.TSOServiceName,
		primaryName, "", 0, map[string]bool{
			primaryAddr:   true,
			secondaryAddr: true,
		})
	re.NoError(err)

	afterResp, err := client.Get(ctx, expectedPrimaryPath)
	re.NoError(err)
	re.Len(afterResp.Kvs, 1)
	re.Equal(secondaryAddr, string(afterResp.Kvs[0].Value))
	re.NotEqual(beforeResp.Kvs[0].ModRevision, afterResp.Kvs[0].ModRevision)
	re.NotEqual(beforeResp.Kvs[0].Lease, afterResp.Kvs[0].Lease)
}

func putRegistryEntry(
	re *require.Assertions,
	ctx context.Context,
	client *clientv3.Client,
	name string,
	serviceAddr string,
) {
	entry := discovery.ServiceRegistryEntry{
		Name:        name,
		ServiceAddr: serviceAddr,
	}
	serializedValue, err := entry.Serialize()
	re.NoError(err)
	_, err = client.Put(ctx, keypath.RegistryPath(constant.TSOServiceName, serviceAddr), serializedValue)
	re.NoError(err)
}

func leaseID(lease *election.Lease) clientv3.LeaseID {
	return lease.ID.Load().(clientv3.LeaseID)
}
