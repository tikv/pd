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

package server

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestCleanupMicroserviceMetadataInPDMode(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12345)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
	staleMembers := []endpoint.KeyspaceGroupMember{{
		Address:  "http://127.0.0.1:3379",
		Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
	}}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Members:   staleMembers,
			Keyspaces: []uint32{1},
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
			Name:     "keyspace-1",
			Config: map[string]string{
				keyspace.TSOKeyspaceGroupIDKey: strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10),
				"gc_life_time":                 "10m",
			},
		})
	}))

	registryPath := keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379")
	electionPath := keypath.ElectionPath(&keypath.MsParam{
		ServiceName: mcs.TSOServiceName,
		GroupID:     constant.DefaultKeyspaceGroupID,
	})
	timestampPath := keypath.TimestampPath(constant.DefaultKeyspaceGroupID)
	for path, value := range map[string]string{
		registryPath:  "tso",
		electionPath:  "primary",
		timestampPath: "timestamp",
	} {
		_, err := client.Put(ctx, path, value)
		re.NoError(err)
	}

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx, term))
	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx, term))

	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 1)
	re.Equal(constant.DefaultKeyspaceGroupID, groups[0].ID)
	re.Equal(endpoint.Basic.String(), groups[0].UserKind)
	re.Empty(groups[0].Members)
	re.Equal([]uint32{1}, groups[0].Keyspaces)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.NotNil(meta)
		re.Equal("0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		re.Equal("10m", meta.GetConfig()["gc_life_time"])
		return nil
	}))
	for path, value := range map[string]string{
		registryPath:  "tso",
		electionPath:  "primary",
		timestampPath: "timestamp",
	} {
		resp, err := etcdutil.EtcdKVGet(client, path)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Equal(value, string(resp.Kvs[0].Value))
	}
}

func TestCleanupMicroserviceMetadataIgnoresLeasedRegistryKeys(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12351)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
			Keyspaces: []uint32{1},
		})
	}))

	registryPath := keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379")
	leaseID, err := etcdutil.EtcdKVPutWithTTL(ctx, client, registryPath, "tso", 60)
	re.NoError(err)
	re.NotZero(leaseID)
	defer func() {
		_, _ = client.Revoke(ctx, leaseID)
	}()

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx, term))
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 1)
	re.Empty(groups[0].Members)
	resp, err := etcdutil.EtcdKVGet(client, registryPath)
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	re.Equal(leaseID, clientv3.LeaseID(resp.Kvs[0].Lease))
}

func TestPrepareMicroserviceMetadataCleanupBlocksUntilCommitted(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12348)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, _ := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
		})
	}))
	blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- svr.prepareMicroserviceMetadataCleanup(ctx)
	}()
	blocker.wait(t)
	select {
	case err := <-resultCh:
		re.NoError(err)
		t.Fatal("PD leader preparation completed before microservice metadata cleanup committed")
	default:
	}
	blocker.releaseCleanup()
	blocker.disable(t)
	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(10 * time.Second):
		t.Fatal("microservice metadata cleanup did not return")
	}
}

func TestPrepareMicroserviceMetadataCleanupStopsOnRejectedState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12353)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, _ := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       1,
			UserKind: endpoint.Standard.String(),
		})
	}))

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- svr.prepareMicroserviceMetadataCleanup(ctx)
	}()
	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(10 * time.Second):
		cancel()
		<-resultCh
		t.Fatal("cleanup kept retrying a rejected metadata state")
	}
}

func TestMicroserviceMetadataCleanupTransitionDetection(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12350)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	candidate, err := svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.Nil(candidate)

	_, err = client.Put(ctx, keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379"), "tso")
	re.NoError(err)
	candidate, err = svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.Nil(candidate)

	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		})
	}))
	candidate, err = svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.Nil(candidate)

	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		group, err := store.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		if err != nil {
			return err
		}
		group.Members = []endpoint.KeyspaceGroupMember{{
			Address:  "http://127.0.0.1:3379",
			Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
		}}
		return store.SaveKeyspaceGroup(txn, group)
	}))
	candidate, err = svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.NotNil(candidate)
}

func TestPrepareMicroserviceMetadataCleanupStopsOnContextCancellation(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12356)
	t.Cleanup(keypath.ResetClusterID)

	store := storage.NewStorageWithEtcdBackend(client)
	svr, _ := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
		})
	}))
	cancel()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- svr.prepareMicroserviceMetadataCleanup(ctx)
	}()
	select {
	case err := <-resultCh:
		re.ErrorIs(err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup did not stop after its context was canceled")
	}
}

func TestCleanupMicroserviceMetadataInPDModeRejectsNonDefaultGroup(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12346)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
	staleMembers := []endpoint.KeyspaceGroupMember{{
		Address:  "http://127.0.0.1:3379",
		Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
	}}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members:  staleMembers,
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       1,
			UserKind: endpoint.Standard.String(),
		})
	}))

	err := svr.cleanupMicroserviceMetadataInPDMode(ctx, term)
	re.ErrorContains(err, "non-default TSO keyspace group 1")
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 2)
	re.Equal(staleMembers, groups[0].Members)
}

func TestCleanupMicroserviceMetadataInPDModePreservesAssignmentMarkers(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12347)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceMeta(txn, newKeyspaceMetaWithTSOGroup(1, "0"))
	}))

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx, term))
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.Equal("0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		return nil
	}))
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 1)
	re.Empty(groups[0].Members)
}

func TestCleanupMicroserviceMetadataInPDModeRejectsGroupTransition(t *testing.T) {
	testCases := []struct {
		name       string
		transition func(*endpoint.KeyspaceGroup)
		errText    string
	}{
		{
			name: "splitting",
			transition: func(group *endpoint.KeyspaceGroup) {
				group.SplitState = &endpoint.SplitState{SplitSource: group.ID}
			},
			errText: "splitting",
		},
		{
			name: "merging",
			transition: func(group *endpoint.KeyspaceGroup) {
				group.MergeState = &endpoint.MergeState{MergeList: []uint32{1}}
			},
			errText: "merging",
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
			t.Cleanup(clean)
			keypath.SetClusterID(uint64(12400 + i))
			defer keypath.ResetClusterID()

			store := storage.NewStorageWithEtcdBackend(client)
			svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
			group := &endpoint.KeyspaceGroup{
				ID:       constant.DefaultKeyspaceGroupID,
				UserKind: endpoint.Basic.String(),
				Members: []endpoint.KeyspaceGroupMember{{
					Address:  "http://127.0.0.1:3379",
					Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
				}},
			}
			testCase.transition(group)
			re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
				return store.SaveKeyspaceGroup(txn, group)
			}))

			err := svr.cleanupMicroserviceMetadataInPDMode(ctx, term)
			re.ErrorContains(err, testCase.errText)
			groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
			re.NoError(err)
			re.Len(groups, 1)
			re.Equal(group, groups[0])
		})
	}
}

func TestClearDefaultTSOKeyspaceGroupMembersIsFencedByLeadershipTerm(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12354)
	t.Cleanup(keypath.ResetClusterID)

	store := storage.NewStorageWithEtcdBackend(client)
	svr, oldTerm := newMicroserviceMetadataCleanupTestServer(t, client, store)
	staleMembers := []endpoint.KeyspaceGroupMember{{
		Address:  "http://127.0.0.1:3379",
		Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
	}}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Members:   staleMembers,
			Keyspaces: []uint32{1},
		})
	}))

	candidate, err := svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.NotNil(candidate)
	blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
	resultCh := make(chan clearDefaultTSOKeyspaceGroupMembersResult, 1)
	go func() {
		cleared, err := svr.clearDefaultTSOKeyspaceGroupMembers(ctx, oldTerm, candidate)
		resultCh <- clearDefaultTSOKeyspaceGroupMembersResult{cleared: cleared, err: err}
	}()
	blocker.wait(t)

	// Campaign again with the same member value to prove that comparing only the
	// leader value would allow an old-term cleanup to pass.
	svr.member.Resign()
	re.NoError(svr.member.GetLeadership().Campaign(
		testMicroserviceMetadataCleanupLeaseTimeout,
		svr.member.MemberValue(),
	))
	svr.member.PromoteSelf()
	newTerm, ok := svr.captureMicroserviceMetadataCleanupTerm()
	re.True(ok)
	re.Equal(oldTerm.leaderValue, newTerm.leaderValue)
	re.NotEqual(oldTerm.leaseID, newTerm.leaseID)

	blocker.releaseCleanup()
	blocker.disable(t)
	result := waitClearDefaultTSOKeyspaceGroupMembersResult(t, resultCh)
	re.False(result.cleared)
	re.ErrorIs(result.err, errs.ErrEtcdTxnConflict)

	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 1)
	re.Equal(staleMembers, groups[0].Members)
}

func TestClearDefaultTSOKeyspaceGroupMembersPreservesConcurrentUpdate(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(12355)
	t.Cleanup(keypath.ResetClusterID)

	store := storage.NewStorageWithEtcdBackend(client)
	svr, term := newMicroserviceMetadataCleanupTestServer(t, client, store)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
			Members: []endpoint.KeyspaceGroupMember{{
				Address:  "http://127.0.0.1:3379",
				Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
			}},
			Keyspaces: []uint32{1},
		})
	}))

	candidate, err := svr.loadMicroserviceMetadataCleanupCandidate(ctx)
	re.NoError(err)
	re.NotNil(candidate)
	blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
	resultCh := make(chan clearDefaultTSOKeyspaceGroupMembersResult, 1)
	go func() {
		cleared, err := svr.clearDefaultTSOKeyspaceGroupMembers(ctx, term, candidate)
		resultCh <- clearDefaultTSOKeyspaceGroupMembersResult{cleared: cleared, err: err}
	}()
	blocker.wait(t)

	newMembers := []endpoint.KeyspaceGroupMember{{
		Address:  "http://127.0.0.1:3380",
		Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
	}}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		group, err := store.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		if err != nil {
			return err
		}
		group.Members = newMembers
		group.Keyspaces = append(group.Keyspaces, 2)
		return store.SaveKeyspaceGroup(txn, group)
	}))

	blocker.releaseCleanup()
	blocker.disable(t)
	result := waitClearDefaultTSOKeyspaceGroupMembersResult(t, resultCh)
	re.False(result.cleared)
	re.ErrorIs(result.err, errs.ErrEtcdTxnConflict)

	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 1)
	re.Equal(newMembers, groups[0].Members)
	re.Equal([]uint32{1, 2}, groups[0].Keyspaces)
}

const testMicroserviceMetadataCleanupLeaseTimeout = 60

type clearDefaultTSOKeyspaceGroupMembersResult struct {
	cleared bool
	err     error
}

type microserviceMetadataCleanupCommitBlocker struct {
	name        string
	reached     chan struct{}
	release     chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
	disableOnce sync.Once
}

func newMicroserviceMetadataCleanupTestServer(
	t *testing.T,
	client *clientv3.Client,
	store storage.Storage,
) (*Server, microserviceMetadataCleanupTerm) {
	t.Helper()
	pdMember := member.NewMember(nil, client, 1)
	pdMember.InitMemberInfo("http://127.0.0.1:2379", "http://127.0.0.1:2380", "pd-test")
	re := require.New(t)
	re.NoError(pdMember.GetLeadership().Campaign(testMicroserviceMetadataCleanupLeaseTimeout, pdMember.MemberValue()))
	pdMember.PromoteSelf()
	t.Cleanup(pdMember.Resign)

	svr := &Server{storage: store, client: client, member: pdMember}
	term, ok := svr.captureMicroserviceMetadataCleanupTerm()
	re.True(ok)
	return svr, term
}

func enableMicroserviceMetadataCleanupCommitBlocker(t *testing.T) *microserviceMetadataCleanupCommitBlocker {
	t.Helper()
	blocker := &microserviceMetadataCleanupCommitBlocker{
		name:    "github.com/tikv/pd/server/beforeClearDefaultTSOKeyspaceGroupMembersCommit",
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, failpoint.EnableCall(blocker.name, func() {
		blocker.reachedOnce.Do(func() {
			close(blocker.reached)
		})
		<-blocker.release
	}))
	t.Cleanup(func() {
		blocker.releaseCleanup()
		blocker.disable(t)
	})
	return blocker
}

func (b *microserviceMetadataCleanupCommitBlocker) wait(t *testing.T) {
	t.Helper()
	select {
	case <-b.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("microservice metadata cleanup did not reach the commit hook")
	}
}

func (b *microserviceMetadataCleanupCommitBlocker) releaseCleanup() {
	b.releaseOnce.Do(func() {
		close(b.release)
	})
}

func (b *microserviceMetadataCleanupCommitBlocker) disable(t *testing.T) {
	t.Helper()
	b.disableOnce.Do(func() {
		require.NoError(t, failpoint.Disable(b.name))
	})
}

func waitClearDefaultTSOKeyspaceGroupMembersResult(
	t *testing.T,
	resultCh <-chan clearDefaultTSOKeyspaceGroupMembersResult,
) clearDefaultTSOKeyspaceGroupMembersResult {
	t.Helper()
	select {
	case result := <-resultCh:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("microservice metadata cleanup did not return")
		return clearDefaultTSOKeyspaceGroupMembersResult{}
	}
}

func newKeyspaceMetaWithTSOGroup(id uint32, groupID string) *keyspacepb.KeyspaceMeta {
	return &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
		Name:     "keyspace-" + strconv.FormatUint(uint64(id), 10),
		Config: map[string]string{
			keyspace.TSOKeyspaceGroupIDKey: groupID,
		},
	}
}
