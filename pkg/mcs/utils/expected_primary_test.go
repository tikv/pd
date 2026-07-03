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
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const (
	testPrimaryName   = "tso-primary"
	testPrimaryAddr   = "http://127.0.0.1:10001"
	testSecondaryName = "tso-secondary"
	testSecondaryAddr = "http://127.0.0.1:10002"
)

func TestTransferPrimary(t *testing.T) {
	tests := []struct {
		name                string
		oldPrimary          string
		newPrimary          string
		expectedPrimary     string
		keepExpectedPrimary bool
	}{
		{
			name:                "self transfer by name keeps expected primary lease",
			oldPrimary:          testPrimaryName,
			newPrimary:          testPrimaryName,
			expectedPrimary:     testPrimaryAddr,
			keepExpectedPrimary: true,
		},
		{
			name:                "self transfer by address keeps expected primary lease",
			oldPrimary:          testPrimaryName,
			newPrimary:          testPrimaryAddr,
			expectedPrimary:     testPrimaryAddr,
			keepExpectedPrimary: true,
		},
		{
			name:            "random transfer excludes old primary by name",
			oldPrimary:      testPrimaryName,
			expectedPrimary: testSecondaryAddr,
		},
		{
			name:            "random transfer excludes old primary by address",
			oldPrimary:      testPrimaryAddr,
			expectedPrimary: testSecondaryAddr,
		},
		{
			name:            "explicit transfer selects target by name",
			oldPrimary:      testPrimaryName,
			newPrimary:      testSecondaryName,
			expectedPrimary: testSecondaryAddr,
		},
		{
			name:            "explicit transfer selects target by address",
			oldPrimary:      testPrimaryName,
			newPrimary:      testSecondaryAddr,
			expectedPrimary: testSecondaryAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTransferPrimaryTestEnv(t)
			before := env.getExpectedPrimary(t)
			re := require.New(t)
			re.Equal(testPrimaryAddr, before.value)
			re.Equal(int64(env.lease.GetID()), before.lease)

			err := mcsutils.TransferPrimary(env.client, env.lease, constant.TSOServiceName,
				tt.oldPrimary, tt.newPrimary, 0, map[string]bool{
					testPrimaryAddr:   true,
					testSecondaryAddr: true,
				})
			re.NoError(err)

			after := env.getExpectedPrimary(t)
			re.Equal(tt.expectedPrimary, after.value)
			if tt.keepExpectedPrimary {
				re.Equal(before.modRevision, after.modRevision)
				re.Equal(before.lease, after.lease)
				return
			}
			re.NotEqual(before.modRevision, after.modRevision)
			re.NotEqual(before.lease, after.lease)
		})
	}
}

type transferPrimaryTestEnv struct {
	ctx                 context.Context
	client              *clientv3.Client
	lease               *election.Lease
	expectedPrimaryPath string
}

type expectedPrimary struct {
	value       string
	modRevision int64
	lease       int64
}

func newTransferPrimaryTestEnv(t *testing.T) *transferPrimaryTestEnv {
	t.Helper()
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)

	keypath.SetClusterID(uint64(time.Now().UnixNano()))
	t.Cleanup(keypath.ResetClusterID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	putRegistryEntry(t, ctx, client, testPrimaryName, testPrimaryAddr)
	putRegistryEntry(t, ctx, client, testSecondaryName, testSecondaryAddr)

	lease := election.NewLease(client, "tso expected primary 00000")
	re.NoError(lease.Grant(constant.DefaultLease))
	t.Cleanup(func() {
		_ = lease.Close()
	})

	expectedPrimaryPath := keypath.ExpectedPrimaryPath(&keypath.MsParam{
		ServiceName: constant.TSOServiceName,
		GroupID:     0,
	})
	_, err := client.Put(ctx, expectedPrimaryPath, testPrimaryAddr, clientv3.WithLease(lease.GetID()))
	re.NoError(err)

	return &transferPrimaryTestEnv{
		ctx:                 ctx,
		client:              client,
		lease:               lease,
		expectedPrimaryPath: expectedPrimaryPath,
	}
}

func (env *transferPrimaryTestEnv) getExpectedPrimary(t *testing.T) expectedPrimary {
	t.Helper()
	re := require.New(t)
	resp, err := env.client.Get(env.ctx, env.expectedPrimaryPath)
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	kv := resp.Kvs[0]
	return expectedPrimary{
		value:       string(kv.Value),
		modRevision: kv.ModRevision,
		lease:       kv.Lease,
	}
}

func putRegistryEntry(
	t *testing.T,
	ctx context.Context,
	client *clientv3.Client,
	name string,
	serviceAddr string,
) {
	t.Helper()
	entry := discovery.ServiceRegistryEntry{
		Name:        name,
		ServiceAddr: serviceAddr,
	}
	serializedValue, err := entry.Serialize()
	require.NoError(t, err)
	_, err = client.Put(ctx, keypath.RegistryPath(constant.TSOServiceName, serviceAddr), serializedValue)
	require.NoError(t, err)
}
