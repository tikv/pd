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
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

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

func TestCleanupMicroserviceMetadataPreservesDefaultGroup(t *testing.T) {
	svr, store := newMicroserviceMetadataCleanupTestServer(t, 13001)
	ctx := context.Background()
	staleMembers := []endpoint.KeyspaceGroupMember{
		{Address: "http://127.0.0.1:3379", Priority: mcs.DefaultKeyspaceGroupReplicaPriority},
		{Address: "http://127.0.0.1:3380", Priority: mcs.DefaultKeyspaceGroupReplicaPriority},
	}
	saveMicroserviceMetadataCleanupTestGroup(t, store, &endpoint.KeyspaceGroup{
		ID:        constant.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Standard.String(),
		Members:   staleMembers,
		Keyspaces: []uint32{1, 2, 3},
	})
	require.NoError(t, store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
			Name:     "keyspace-1",
			Config: map[string]string{
				keyspace.TSOKeyspaceGroupIDKey: "0",
				"gc_life_time":                 "10m",
			},
		})
	}))
	const timestampValue = "group-0-timestamp"
	timestampPath := keypath.TimestampPath(constant.DefaultKeyspaceGroupID)
	_, err := svr.client.Put(ctx, timestampPath, timestampValue)
	require.NoError(t, err)

	changed, err := svr.CleanupMicroserviceMetadata(ctx)
	require.NoError(t, err)
	require.True(t, changed)

	group := loadMicroserviceMetadataCleanupTestGroup(t, store, constant.DefaultKeyspaceGroupID)
	require.Equal(t, constant.DefaultKeyspaceGroupID, group.ID)
	require.Equal(t, endpoint.Standard.String(), group.UserKind)
	require.Empty(t, group.Members)
	require.Equal(t, []uint32{1, 2, 3}, group.Keyspaces)
	require.Nil(t, group.SplitState)
	require.Nil(t, group.MergeState)

	changed, err = svr.CleanupMicroserviceMetadata(ctx)
	require.NoError(t, err)
	require.False(t, changed)

	require.NoError(t, store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.Equal(t, "0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		require.Equal(t, "10m", meta.GetConfig()["gc_life_time"])
		return nil
	}))
	resp, err := etcdutil.EtcdKVGet(svr.client, timestampPath)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, timestampValue, string(resp.Kvs[0].Value))
}

func TestCleanupMicroserviceMetadataPreservesUnknownGroupFields(t *testing.T) {
	svr, _ := newMicroserviceMetadataCleanupTestServer(t, 13015)
	ctx := context.Background()
	const (
		futureField = `{"large-id":9007199254740993,"nested":{"enabled":true}}`
		groupJSON   = `{"id":0,"user-kind":"basic","members":[{"address":"http://127.0.0.1:3379","priority":0}],"keyspaces":[1],"future-field":` + futureField + `}`
	)
	groupKey := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID)
	_, err := svr.client.Put(ctx, groupKey, groupJSON)
	require.NoError(t, err)

	changed, err := svr.CleanupMicroserviceMetadata(ctx)
	require.NoError(t, err)
	require.True(t, changed)

	resp, err := etcdutil.EtcdKVGet(svr.client, groupKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	persistedGroup := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(resp.Kvs[0].Value, &persistedGroup))
	require.Equal(t, "null", string(persistedGroup["members"]))
	require.True(t, bytes.Equal(json.RawMessage(futureField), persistedGroup["future-field"]))

	value := string(resp.Kvs[0].Value)
	modRevision := resp.Kvs[0].ModRevision
	changed, err = svr.CleanupMicroserviceMetadata(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	resp, err = etcdutil.EtcdKVGet(svr.client, groupKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, value, string(resp.Kvs[0].Value))
	require.Equal(t, modRevision, resp.Kvs[0].ModRevision)
}

func TestCleanupMicroserviceMetadataRejectsInvalidGroupJSONObjects(t *testing.T) {
	testCases := []struct {
		name      string
		groupJSON string
	}{
		{name: "null", groupJSON: "null"},
		{name: "missing-id", groupJSON: `{"members":[]}`},
		{name: "non-canonical-members", groupJSON: `{"id":0,"Members":[{"address":"http://127.0.0.1:3379","priority":0}]}`},
		{name: "duplicate-members", groupJSON: `{"id":0,"members":[{"address":"http://127.0.0.1:3379","priority":0}],"members":[]}`},
		{name: "duplicate-transition", groupJSON: `{"id":0,"split-state":{"split-source":0},"split-state":null,"members":[]}`},
	}
	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			svr, _ := newMicroserviceMetadataCleanupTestServer(t, uint64(13016+i))
			ctx := context.Background()
			groupKey := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID)
			_, err := svr.client.Put(ctx, groupKey, testCase.groupJSON)
			require.NoError(t, err)

			changed, err := svr.CleanupMicroserviceMetadata(ctx)
			require.False(t, changed)
			require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
			resp, err := etcdutil.EtcdKVGet(svr.client, groupKey)
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1)
			require.Equal(t, testCase.groupJSON, string(resp.Kvs[0].Value))
		})
	}
}

func TestCleanupMicroserviceMetadataRejectsUnsafeState(t *testing.T) {
	t.Run("missing-default-group", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13014)
		ctx := context.Background()
		require.NoError(t, store.RunInTxn(ctx, func(txn kv.Txn) error {
			return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
				Name:     "keyspace-1",
				Config: map[string]string{
					keyspace.TSOKeyspaceGroupIDKey: "0",
				},
			})
		}))

		changed, err := svr.CleanupMicroserviceMetadata(ctx)
		require.False(t, changed)
		require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
		require.NoError(t, store.RunInTxn(ctx, func(txn kv.Txn) error {
			meta, err := store.LoadKeyspaceMeta(txn, 1)
			require.NoError(t, err)
			require.NotNil(t, meta)
			require.Equal(t, "0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
			return nil
		}))
	})

	t.Run("non-default-group", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13002)
		defaultGroup := newMicroserviceMetadataCleanupTestDefaultGroup()
		saveMicroserviceMetadataCleanupTestGroup(t, store, defaultGroup)
		saveMicroserviceMetadataCleanupTestGroup(t, store, &endpoint.KeyspaceGroup{
			ID:       1,
			UserKind: endpoint.Basic.String(),
		})

		changed, err := svr.CleanupMicroserviceMetadata(context.Background())
		require.False(t, changed)
		require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
		require.Equal(t, defaultGroup, loadMicroserviceMetadataCleanupTestGroup(
			t, store, constant.DefaultKeyspaceGroupID))
	})

	testCases := []struct {
		name       string
		clusterID  uint64
		transition func(*endpoint.KeyspaceGroup)
	}{
		{
			name:      "splitting",
			clusterID: 13003,
			transition: func(group *endpoint.KeyspaceGroup) {
				group.SplitState = &endpoint.SplitState{SplitSource: group.ID}
			},
		},
		{
			name:      "merging",
			clusterID: 13004,
			transition: func(group *endpoint.KeyspaceGroup) {
				group.MergeState = &endpoint.MergeState{MergeList: []uint32{1}}
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			svr, store := newMicroserviceMetadataCleanupTestServer(t, testCase.clusterID)
			group := newMicroserviceMetadataCleanupTestDefaultGroup()
			testCase.transition(group)
			saveMicroserviceMetadataCleanupTestGroup(t, store, group)

			changed, err := svr.CleanupMicroserviceMetadata(context.Background())
			require.False(t, changed)
			require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
			require.Equal(t, group, loadMicroserviceMetadataCleanupTestGroup(
				t, store, constant.DefaultKeyspaceGroupID))
		})
	}
}

func TestCleanupMicroserviceMetadataRequiresPDModeLeader(t *testing.T) {
	t.Run("microservice-mode", func(t *testing.T) {
		svr, _ := newMicroserviceMetadataCleanupTestServer(t, 13005)
		svr.isKeyspaceGroupEnabled = true

		changed, err := svr.CleanupMicroserviceMetadata(context.Background())
		require.False(t, changed)
		require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
	})

	t.Run("not-serving", func(t *testing.T) {
		svr, _ := newMicroserviceMetadataCleanupTestServer(t, 13006)
		svr.member.Resign()

		changed, err := svr.CleanupMicroserviceMetadata(context.Background())
		require.False(t, changed)
		require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupUnavailable)
	})
}

func TestCleanupMicroserviceMetadataIsFencedByExactLeadershipTerm(t *testing.T) {
	svr, store := newMicroserviceMetadataCleanupTestServer(t, 13007)
	saveMicroserviceMetadataCleanupTestGroup(t, store, newMicroserviceMetadataCleanupTestDefaultGroup())
	oldTerm, err := svr.captureMicroserviceMetadataCleanupTerm()
	require.NoError(t, err)

	blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
	resultCh := runMicroserviceMetadataCleanup(t, svr)
	blocker.wait(t)

	// Re-campaign with the same member value. The new lease is what distinguishes
	// this leadership term from the one captured by the blocked request.
	svr.member.Resign()
	require.NoError(t, svr.member.GetLeadership().Campaign(
		testMicroserviceMetadataCleanupLeaseTimeout,
		svr.member.MemberValue(),
	))
	svr.member.PromoteSelf()
	newTerm, err := svr.captureMicroserviceMetadataCleanupTerm()
	require.NoError(t, err)
	require.Equal(t, oldTerm.leaderValue, newTerm.leaderValue)
	require.NotEqual(t, oldTerm.leaseID, newTerm.leaseID)

	blocker.releaseCleanup()
	result := waitMicroserviceMetadataCleanupResult(t, resultCh)
	require.False(t, result.changed)
	require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
	require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")
	require.NotEmpty(t, loadMicroserviceMetadataCleanupTestGroup(
		t, store, constant.DefaultKeyspaceGroupID).Members)
}

func TestCleanupMicroserviceMetadataPreservesConcurrentGroupUpdate(t *testing.T) {
	svr, store := newMicroserviceMetadataCleanupTestServer(t, 13008)
	saveMicroserviceMetadataCleanupTestGroup(t, store, newMicroserviceMetadataCleanupTestDefaultGroup())

	blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
	resultCh := runMicroserviceMetadataCleanup(t, svr)
	blocker.wait(t)

	newMembers := []endpoint.KeyspaceGroupMember{{
		Address:  "http://127.0.0.1:3380",
		Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
	}}
	updateMicroserviceMetadataCleanupTestGroup(t, store, constant.DefaultKeyspaceGroupID, func(group *endpoint.KeyspaceGroup) {
		group.Members = newMembers
		group.Keyspaces = append(group.Keyspaces, 2)
	})

	blocker.releaseCleanup()
	result := waitMicroserviceMetadataCleanupResult(t, resultCh)
	require.False(t, result.changed)
	require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
	require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")
	group := loadMicroserviceMetadataCleanupTestGroup(t, store, constant.DefaultKeyspaceGroupID)
	require.Equal(t, newMembers, group.Members)
	require.Equal(t, []uint32{1, 2}, group.Keyspaces)
}

func TestCleanupMicroserviceMetadataFencesNoOp(t *testing.T) {
	t.Run("empty-members-leadership-change", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13010)
		group := newMicroserviceMetadataCleanupTestDefaultGroup()
		group.Members = nil
		saveMicroserviceMetadataCleanupTestGroup(t, store, group)

		blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
		resultCh := runMicroserviceMetadataCleanup(t, svr)
		blocker.wait(t)
		svr.member.Resign()
		blocker.releaseCleanup()

		result := waitMicroserviceMetadataCleanupResult(t, resultCh)
		require.False(t, result.changed)
		require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
		require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")
	})

	t.Run("empty-members-group-update", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13011)
		group := newMicroserviceMetadataCleanupTestDefaultGroup()
		group.Members = nil
		saveMicroserviceMetadataCleanupTestGroup(t, store, group)

		blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
		resultCh := runMicroserviceMetadataCleanup(t, svr)
		blocker.wait(t)
		updateMicroserviceMetadataCleanupTestGroup(t, store, constant.DefaultKeyspaceGroupID, func(group *endpoint.KeyspaceGroup) {
			group.Keyspaces = append(group.Keyspaces, 2)
		})
		blocker.releaseCleanup()

		result := waitMicroserviceMetadataCleanupResult(t, resultCh)
		require.False(t, result.changed)
		require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
		require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")
		require.Equal(t, []uint32{1, 2}, loadMicroserviceMetadataCleanupTestGroup(
			t, store, constant.DefaultKeyspaceGroupID).Keyspaces)
	})

	t.Run("empty-members-group-deleted", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13012)
		group := newMicroserviceMetadataCleanupTestDefaultGroup()
		group.Members = nil
		saveMicroserviceMetadataCleanupTestGroup(t, store, group)

		blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
		resultCh := runMicroserviceMetadataCleanup(t, svr)
		blocker.wait(t)
		require.NoError(t, store.RunInTxn(context.Background(), func(txn kv.Txn) error {
			return store.DeleteKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		}))
		blocker.releaseCleanup()

		result := waitMicroserviceMetadataCleanupResult(t, resultCh)
		require.False(t, result.changed)
		require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
		require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")

		changed, err := svr.CleanupMicroserviceMetadata(context.Background())
		require.False(t, changed)
		require.ErrorIs(t, err, ErrMicroserviceMetadataCleanupRejected)
	})

	t.Run("non-default-created", func(t *testing.T) {
		svr, store := newMicroserviceMetadataCleanupTestServer(t, 13013)
		group := newMicroserviceMetadataCleanupTestDefaultGroup()
		group.Members = nil
		saveMicroserviceMetadataCleanupTestGroup(t, store, group)

		blocker := enableMicroserviceMetadataCleanupCommitBlocker(t)
		resultCh := runMicroserviceMetadataCleanup(t, svr)
		blocker.wait(t)
		saveMicroserviceMetadataCleanupTestGroup(t, store, &endpoint.KeyspaceGroup{
			ID:       1,
			UserKind: endpoint.Basic.String(),
		})
		blocker.releaseCleanup()

		result := waitMicroserviceMetadataCleanupResult(t, resultCh)
		require.False(t, result.changed)
		require.ErrorIs(t, result.err, ErrMicroserviceMetadataCleanupUnavailable)
		require.ErrorContains(t, result.err, "leadership or keyspace-group metadata changed during cleanup")
		require.NotNil(t, loadMicroserviceMetadataCleanupTestGroup(t, store, 1))
	})
}

const testMicroserviceMetadataCleanupLeaseTimeout = 60

type microserviceMetadataCleanupResult struct {
	changed bool
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
	clusterID uint64,
) (*Server, storage.Storage) {
	t.Helper()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	keypath.SetClusterID(clusterID)
	t.Cleanup(keypath.ResetClusterID)

	store := storage.NewStorageWithEtcdBackend(client)
	pdMember := member.NewMember(nil, client, 1)
	pdMember.InitMemberInfo("http://127.0.0.1:2379", "http://127.0.0.1:2380", "pd-test")
	require.NoError(t, pdMember.GetLeadership().Campaign(
		testMicroserviceMetadataCleanupLeaseTimeout,
		pdMember.MemberValue(),
	))
	pdMember.PromoteSelf()
	t.Cleanup(pdMember.Resign)

	return &Server{
		storage: store,
		client:  client,
		member:  pdMember,
	}, store
}

func newMicroserviceMetadataCleanupTestDefaultGroup() *endpoint.KeyspaceGroup {
	return &endpoint.KeyspaceGroup{
		ID:       constant.DefaultKeyspaceGroupID,
		UserKind: endpoint.Basic.String(),
		Members: []endpoint.KeyspaceGroupMember{{
			Address:  "http://127.0.0.1:3379",
			Priority: mcs.DefaultKeyspaceGroupReplicaPriority,
		}},
		Keyspaces: []uint32{1},
	}
}

func saveMicroserviceMetadataCleanupTestGroup(
	t *testing.T,
	store storage.Storage,
	group *endpoint.KeyspaceGroup,
) {
	t.Helper()
	require.NoError(t, store.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, group)
	}))
}

func updateMicroserviceMetadataCleanupTestGroup(
	t *testing.T,
	store storage.Storage,
	id uint32,
	update func(*endpoint.KeyspaceGroup),
) {
	t.Helper()
	require.NoError(t, store.RunInTxn(context.Background(), func(txn kv.Txn) error {
		group, err := store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		require.NotNil(t, group)
		update(group)
		return store.SaveKeyspaceGroup(txn, group)
	}))
}

func loadMicroserviceMetadataCleanupTestGroup(
	t *testing.T,
	store storage.Storage,
	id uint32,
) *endpoint.KeyspaceGroup {
	t.Helper()
	var group *endpoint.KeyspaceGroup
	require.NoError(t, store.RunInTxn(context.Background(), func(txn kv.Txn) error {
		var err error
		group, err = store.LoadKeyspaceGroup(txn, id)
		return err
	}))
	return group
}

func runMicroserviceMetadataCleanup(t *testing.T, svr *Server) <-chan microserviceMetadataCleanupResult {
	t.Helper()
	resultCh := make(chan microserviceMetadataCleanupResult, 1)
	go func() {
		changed, err := svr.CleanupMicroserviceMetadata(context.Background())
		resultCh <- microserviceMetadataCleanupResult{changed: changed, err: err}
	}()
	return resultCh
}

func enableMicroserviceMetadataCleanupCommitBlocker(t *testing.T) *microserviceMetadataCleanupCommitBlocker {
	t.Helper()
	blocker := &microserviceMetadataCleanupCommitBlocker{
		name:    "github.com/tikv/pd/server/beforeCleanupMicroserviceMetadataCommit",
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

func waitMicroserviceMetadataCleanupResult(
	t *testing.T,
	resultCh <-chan microserviceMetadataCleanupResult,
) microserviceMetadataCleanupResult {
	t.Helper()
	select {
	case result := <-resultCh:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("microservice metadata cleanup did not return")
		return microserviceMetadataCleanupResult{}
	}
}
