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

package keyspace

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

var errSaveKeyspaceGroup = errors.New("save keyspace group error")

type errorKeyspaceGroupStorage struct {
	*endpoint.StorageEndpoint
	failOnSaveID uint32
}

func (s *errorKeyspaceGroupStorage) SaveKeyspaceGroup(txn kv.Txn, kg *endpoint.KeyspaceGroup) error {
	if s.failOnSaveID != 0 && kg.ID == s.failOnSaveID {
		return errSaveKeyspaceGroup
	}
	return s.StorageEndpoint.SaveKeyspaceGroup(txn, kg)
}

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
	kgm    *GroupManager
	kg     *Manager
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	suite.kgm = NewKeyspaceGroupManager(suite.ctx, store, nil)
	idAllocator := mockid.NewIDAllocator()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	suite.kg = NewKeyspaceManager(suite.ctx, store, cluster, idAllocator, &mockConfig{}, suite.kgm, nil)
	re.NoError(suite.kgm.Bootstrap(suite.ctx))
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupOperations() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
		},
		{
			ID:       uint32(3),
			UserKind: endpoint.Standard.String(),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// list all keyspace groups
	kgs, err := suite.kgm.GetKeyspaceGroups(uint32(0), 0)
	re.NoError(err)
	re.Len(kgs, 4)
	// list part of keyspace groups
	kgs, err = suite.kgm.GetKeyspaceGroups(uint32(1), 2)
	re.NoError(err)
	re.Len(kgs, 2)
	// get the default keyspace group
	kg, err := suite.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(uint32(0), kg.ID)
	re.Equal(endpoint.Basic.String(), kg.UserKind)
	re.False(kg.IsSplitting())
	// get the keyspace group 3
	kg, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Equal(uint32(3), kg.ID)
	re.Equal(endpoint.Standard.String(), kg.UserKind)
	re.False(kg.IsSplitting())
	// remove the keyspace group 3
	kg, err = suite.kgm.DeleteKeyspaceGroupByID(3)
	re.NoError(err)
	re.Equal(uint32(3), kg.ID)
	// get non-existing keyspace group
	kg, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Empty(kg)
	// create an existing keyspace group
	keyspaceGroups = []*endpoint.KeyspaceGroup{{ID: uint32(1), UserKind: endpoint.Standard.String()}}
	err = suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.Error(err)
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceAssignment() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(3),
			UserKind: endpoint.Standard.String(),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// list all keyspace groups
	kgs, err := suite.kgm.GetKeyspaceGroups(uint32(0), 0)
	re.NoError(err)
	re.Len(kgs, 4)

	for i := range 99 {
		_, err := suite.kg.CreateKeyspace(&CreateKeyspaceRequest{
			Name: fmt.Sprintf("test%d", i),
			Config: map[string]string{
				UserKindKey: endpoint.Standard.String(),
			},
			CreateTime: time.Now().Unix(),
		})
		re.NoError(err)
	}

	for i := 1; i <= 3; i++ {
		kg, err := suite.kgm.GetKeyspaceGroupByID(uint32(i))
		re.NoError(err)
		re.Len(kg.Keyspaces, 33)
	}
}

func (suite *keyspaceGroupTestSuite) TestUpdateKeyspace() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Basic.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(3),
			UserKind: endpoint.Enterprise.String(),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// list all keyspace groups
	_, err = suite.kgm.GetKeyspaceGroups(uint32(0), 0)
	re.NoError(err)
	re.Equal(2, suite.kgm.groups[endpoint.Basic].Len())
	re.Equal(1, suite.kgm.groups[endpoint.Standard].Len())
	re.Equal(1, suite.kgm.groups[endpoint.Enterprise].Len())

	_, err = suite.kg.CreateKeyspace(&CreateKeyspaceRequest{
		Name: "test",
		Config: map[string]string{
			UserKindKey: endpoint.Standard.String(),
		},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	kg2, err := suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Len(kg2.Keyspaces, 1)
	kg3, err := suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Empty(kg3.Keyspaces)

	_, err = suite.kg.UpdateKeyspaceConfig("test", []*Mutation{
		{
			Op:    OpPut,
			Key:   UserKindKey,
			Value: endpoint.Enterprise.String(),
		},
		{
			Op:    OpPut,
			Key:   TSOKeyspaceGroupIDKey,
			Value: "2",
		},
	})
	re.Error(err)
	kg2, err = suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Len(kg2.Keyspaces, 1)
	kg3, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Empty(kg3.Keyspaces)
	_, err = suite.kg.UpdateKeyspaceConfig("test", []*Mutation{
		{
			Op:    OpPut,
			Key:   UserKindKey,
			Value: endpoint.Enterprise.String(),
		},
		{
			Op:    OpPut,
			Key:   TSOKeyspaceGroupIDKey,
			Value: "3",
		},
	})
	re.NoError(err)
	kg2, err = suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Empty(kg2.Keyspaces)
	kg3, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Len(kg3.Keyspaces, 1)
}

func (suite *keyspaceGroupTestSuite) TestUpdateKeyspaceGroupRollbackOnSaveError() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	errorStore := &errorKeyspaceGroupStorage{StorageEndpoint: store}
	kgm := NewKeyspaceGroupManager(ctx, errorStore, nil)
	re.NoError(kgm.Bootstrap(ctx))

	keyspaceID := uint32(111)
	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{keyspaceID},
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{222},
		},
	}
	re.NoError(kgm.CreateKeyspaceGroups(keyspaceGroups))

	errorStore.failOnSaveID = 2
	err := kgm.UpdateKeyspaceGroup("1", "2", endpoint.Standard, endpoint.Standard, keyspaceID)
	re.ErrorIs(err, errSaveKeyspaceGroup)

	oldKG := kgm.groups[endpoint.Standard].Get(1)
	newKG := kgm.groups[endpoint.Standard].Get(2)
	re.NotNil(oldKG)
	re.NotNil(newKG)
	re.Equal([]uint32{keyspaceID}, oldKG.Keyspaces)
	re.Equal([]uint32{222}, newKG.Keyspaces)

	storedOld, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Equal([]uint32{keyspaceID}, storedOld.Keyspaces)
	storedNew, err := kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal([]uint32{222}, storedNew.Keyspaces)
}

// TestUpdateKeyspaceGroupRollbackRestoresSortedOrder verifies that when
// saveKeyspaceGroups fails the rollback at line 610 (slices.Sort) correctly
// restores oldKG.Keyspaces to sorted order.
// The existing TestUpdateKeyspaceGroupRollbackOnSaveError only uses a single-element
// oldKG.Keyspaces, so sorting is a no-op there and does not cover this path.
func (suite *keyspaceGroupTestSuite) TestUpdateKeyspaceGroupRollbackRestoresSortedOrder() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	errorStore := &errorKeyspaceGroupStorage{StorageEndpoint: store}
	kgm := NewKeyspaceGroupManager(ctx, errorStore, nil)
	re.NoError(kgm.Bootstrap(ctx))

	// oldKG has multiple keyspaces so that after Remove + append the slice is
	// temporarily out of order ([100, 300, 222]) and needs Sort to be restored.
	keyspaceID := uint32(222)
	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{100, keyspaceID, 300},
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{10, 500},
		},
	}
	re.NoError(kgm.CreateKeyspaceGroups(keyspaceGroups))

	errorStore.failOnSaveID = 2
	err := kgm.UpdateKeyspaceGroup("1", "2", endpoint.Standard, endpoint.Standard, keyspaceID)
	re.ErrorIs(err, errSaveKeyspaceGroup)

	// After rollback oldKG.Keyspaces must be sorted back to [100, 222, 300].
	// Without the slices.Sort on line 610 it would be [100, 300, 222].
	oldKG := kgm.groups[endpoint.Standard].Get(1)
	re.NotNil(oldKG)
	re.Equal([]uint32{100, keyspaceID, 300}, oldKG.Keyspaces)

	newKG := kgm.groups[endpoint.Standard].Get(2)
	re.NotNil(newKG)
	re.Equal([]uint32{10, 500}, newKG.Keyspaces)
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupSplit() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Basic.String(),
			Keyspaces: []uint32{444},
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
			Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// split the bootstrap keyspace
	bootstrapKeyspaceID := GetBootstrapKeyspaceID()
	err = suite.kgm.SplitKeyspaceGroupByID(0, 4, []uint32{bootstrapKeyspaceID})
	re.ErrorIs(err, newModifyProtectedKeyspaceError())
	// split the keyspace group 1 to 4
	err = suite.kgm.SplitKeyspaceGroupByID(1, 4, []uint32{444})
	re.ErrorIs(err, errs.ErrKeyspaceGroupNotEnoughReplicas)
	// split the keyspace group 2 to 4 without giving any keyspace
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{})
	re.ErrorIs(err, errs.ErrKeyspaceNotInKeyspaceGroup)
	// split the keyspace group 2 to 4
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{333})
	re.NoError(err)
	kg2, err := suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 222}, kg2.Keyspaces)
	re.True(kg2.IsSplitSource())
	re.Equal(kg2.ID, kg2.SplitSource())
	kg4, err := suite.kgm.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.Equal([]uint32{333}, kg4.Keyspaces)
	re.True(kg4.IsSplitTarget())
	re.Equal(kg2.ID, kg4.SplitSource())
	re.Equal(kg2.UserKind, kg4.UserKind)
	re.Equal(kg2.Members, kg4.Members)

	// finish the split of the keyspace group 2
	err = suite.kgm.FinishSplitKeyspaceByID(2)
	re.ErrorContains(err, errs.ErrKeyspaceGroupNotInSplit.FastGenByArgs(2).Error())
	// finish the split of a non-existing keyspace group
	err = suite.kgm.FinishSplitKeyspaceByID(5)
	re.ErrorContains(err, errs.ErrKeyspaceGroupNotExists.FastGenByArgs(5).Error())
	// split the in-split keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{333})
	re.ErrorContains(err, errs.ErrKeyspaceGroupInSplit.FastGenByArgs(2).Error())
	// remove the in-split keyspace group
	kg2, err = suite.kgm.DeleteKeyspaceGroupByID(2)
	re.Nil(kg2)
	re.ErrorContains(err, errs.ErrKeyspaceGroupInSplit.FastGenByArgs(2).Error())
	kg4, err = suite.kgm.DeleteKeyspaceGroupByID(4)
	re.Nil(kg4)
	re.ErrorContains(err, errs.ErrKeyspaceGroupInSplit.FastGenByArgs(4).Error())
	// update the in-split keyspace group
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "2", 444, opAdd)
	re.ErrorContains(err, errs.ErrKeyspaceGroupInSplit.FastGenByArgs(2).Error())
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "4", 444, opAdd)
	re.ErrorContains(err, errs.ErrKeyspaceGroupInSplit.FastGenByArgs(4).Error())

	// finish the split of keyspace group 4
	err = suite.kgm.FinishSplitKeyspaceByID(4)
	re.NoError(err)
	kg2, err = suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 222}, kg2.Keyspaces)
	re.False(kg2.IsSplitting())
	kg4, err = suite.kgm.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.Equal([]uint32{333}, kg4.Keyspaces)
	re.False(kg4.IsSplitting())
	re.Equal(kg2.UserKind, kg4.UserKind)
	re.Equal(kg2.Members, kg4.Members)

	// split a non-existing keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(3, 5, nil)
	re.ErrorContains(err, errs.ErrKeyspaceGroupNotExists.FastGenByArgs(3).Error())
	// split into an existing keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{111})
	re.ErrorIs(err, errs.ErrKeyspaceGroupExists)
	// split with the wrong keyspaces.
	err = suite.kgm.SplitKeyspaceGroupByID(2, 5, []uint32{111, 222, 444})
	re.ErrorIs(err, errs.ErrKeyspaceNotInKeyspaceGroup)
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupSplitRange() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Basic.String(),
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 333, 444, 555, 666},
			Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// split the keyspace group 2 to 4 with keyspace range [222, 555]
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, nil, 222, 555)
	re.NoError(err)
	kg2, err := suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 666}, kg2.Keyspaces)
	re.True(kg2.IsSplitSource())
	re.Equal(kg2.ID, kg2.SplitSource())
	kg4, err := suite.kgm.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.Equal([]uint32{333, 444, 555}, kg4.Keyspaces)
	re.True(kg4.IsSplitTarget())
	re.Equal(kg2.ID, kg4.SplitSource())
	re.Equal(kg2.UserKind, kg4.UserKind)
	re.Equal(kg2.Members, kg4.Members)
	// finish the split of keyspace group 4
	err = suite.kgm.FinishSplitKeyspaceByID(4)
	re.NoError(err)
	kg2, err = suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 666}, kg2.Keyspaces)
	re.False(kg2.IsSplitting())
	kg4, err = suite.kgm.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.Equal([]uint32{333, 444, 555}, kg4.Keyspaces)
	re.False(kg4.IsSplitting())
	re.Equal(kg2.UserKind, kg4.UserKind)
	re.Equal(kg2.Members, kg4.Members)
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupMerge() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Basic.String(),
			Keyspaces: []uint32{111, 222, 333},
			Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount),
		},
		{
			ID:        uint32(3),
			UserKind:  endpoint.Basic.String(),
			Keyspaces: []uint32{444, 555},
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// split the keyspace group 1 to 2
	err = suite.kgm.SplitKeyspaceGroupByID(1, 2, []uint32{333})
	re.NoError(err)
	// finish the split of the keyspace group 2
	err = suite.kgm.FinishSplitKeyspaceByID(2)
	re.NoError(err)
	// check the keyspace group 1 and 2
	kg1, err := suite.kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	re.False(kg1.IsMerging())
	kg2, err := suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{333}, kg2.Keyspaces)
	re.False(kg2.IsSplitting())
	re.False(kg2.IsMerging())
	re.Equal(kg1.UserKind, kg2.UserKind)
	re.Equal(kg1.Members, kg2.Members)
	// merge the keyspace group 2 and 3 back into 1
	err = suite.kgm.MergeKeyspaceGroups(1, []uint32{2, 3})
	re.NoError(err)
	// check the keyspace group 2 and 3
	kg2, err = suite.kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Nil(kg2)
	kg3, err := suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Nil(kg3)
	// check the keyspace group 1
	kg1, err = suite.kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333, 444, 555}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	re.True(kg1.IsMerging())
	// finish the merging
	err = suite.kgm.FinishMergeKeyspaceByID(1)
	re.NoError(err)
	kg1, err = suite.kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333, 444, 555}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	re.False(kg1.IsMerging())

	// merge a non-existing keyspace group
	err = suite.kgm.MergeKeyspaceGroups(4, []uint32{5})
	re.ErrorContains(err, errs.ErrKeyspaceGroupNotExists.FastGenByArgs(5).Error())
	// merge with the number of keyspace groups exceeds the limit
	err = suite.kgm.MergeKeyspaceGroups(1, make([]uint32, etcdutil.MaxEtcdTxnOps/2))
	re.ErrorIs(err, errs.ErrExceedMaxEtcdTxnOps)
	// merge the default keyspace group
	err = suite.kgm.MergeKeyspaceGroups(1, []uint32{constant.DefaultKeyspaceGroupID})
	re.ErrorIs(err, errs.ErrModifyDefaultKeyspaceGroup)
}

func TestBuildSplitKeyspaces(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		old             []uint32
		new             []uint32
		startKeyspaceID uint32
		endKeyspaceID   uint32
		expectedOld     []uint32
		expectedNew     []uint32
		err             error
	}{
		{
			old:         []uint32{1, 2, 3, 4, 5},
			new:         []uint32{1, 2, 3, 4, 5},
			expectedOld: []uint32{},
			expectedNew: []uint32{1, 2, 3, 4, 5},
		},
		{
			old:         []uint32{1, 2, 3, 4, 5},
			new:         []uint32{1},
			expectedOld: []uint32{2, 3, 4, 5},
			expectedNew: []uint32{1},
		},
		{
			old: []uint32{1, 2, 3, 4, 5},
			new: []uint32{6},
			err: errs.ErrKeyspaceNotInKeyspaceGroup,
		},
		{
			old:         []uint32{1, 2},
			new:         []uint32{2, 2},
			expectedOld: []uint32{1},
			expectedNew: []uint32{2},
		},
		{
			old:             []uint32{0, 1, 2, 3, 4, 5},
			startKeyspaceID: 2,
			endKeyspaceID:   4,
			expectedOld:     []uint32{0, 1, 5},
			expectedNew:     []uint32{2, 3, 4},
		},
		{
			old:             []uint32{0, 1, 2, 3, 4, 5},
			startKeyspaceID: 0,
			endKeyspaceID:   4,
			expectedOld:     []uint32{0, 5},
			expectedNew:     []uint32{1, 2, 3, 4},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 2,
			endKeyspaceID:   4,
			expectedOld:     []uint32{1, 5},
			expectedNew:     []uint32{2, 3, 4},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 5,
			endKeyspaceID:   6,
			expectedOld:     []uint32{1, 2, 3, 4},
			expectedNew:     []uint32{5},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 2,
			endKeyspaceID:   6,
			expectedOld:     []uint32{1},
			expectedNew:     []uint32{2, 3, 4, 5},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 1,
			endKeyspaceID:   1,
			expectedOld:     []uint32{2, 3, 4, 5},
			expectedNew:     []uint32{1},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 0,
			endKeyspaceID:   6,
			expectedOld:     []uint32{},
			expectedNew:     []uint32{1, 2, 3, 4, 5},
		},
		{
			old:             []uint32{1, 2, 3, 4, 5},
			startKeyspaceID: 7,
			endKeyspaceID:   10,
			err:             errs.ErrKeyspaceGroupWithEmptyKeyspace,
		},
		{
			old: []uint32{1, 2, 3, 4, 5},
			err: errs.ErrKeyspaceNotInKeyspaceGroup,
		},
	}
	for idx, testCase := range testCases {
		old, new, err := buildSplitKeyspaces(testCase.old, testCase.new, testCase.startKeyspaceID, testCase.endKeyspaceID)
		if testCase.err != nil {
			re.ErrorIs(testCase.err, err, "test case %d", idx)
		} else {
			re.NoError(err, "test case %d", idx)

			// Special handling for test case 5 which involves keyspace 0 protection
			expectedOld := testCase.expectedOld
			expectedNew := testCase.expectedNew
			if idx == 5 {
				// Test case 5: old=[0,1,2,3,4,5], start=0, end=4
				// In Classic mode: keyspace 0 is protected, so it stays in old group
				// In NextGen mode: keyspace 0 can move, so it goes to new group
				if kerneltype.IsNextGen() {
					// NextGen: keyspace 0 can move to new group
					expectedOld = []uint32{5}
					expectedNew = []uint32{0, 1, 2, 3, 4}
				} else {
					// Classic: keyspace 0 is protected, stays in old group
					expectedOld = []uint32{0, 5}
					expectedNew = []uint32{1, 2, 3, 4}
				}
			}

			re.Equal(expectedOld, old, "test case %d", idx)
			re.Equal(expectedNew, new, "test case %d", idx)
		}
	}
}

func savePatrolTestKeyspaceGroups(
	ctx context.Context,
	t require.TestingT,
	store *endpoint.StorageEndpoint,
	groups ...*endpoint.KeyspaceGroup,
) {
	require.NoError(t, store.RunInTxn(ctx, func(txn kv.Txn) error {
		for _, group := range groups {
			if err := store.SaveKeyspaceGroup(txn, group); err != nil {
				return err
			}
		}
		return nil
	}))
}

func testKeyspaceGroupMembers() []endpoint.KeyspaceGroupMember {
	return make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount)
}

func buildSequentialKeyspaces(start uint32, count int) []uint32 {
	keyspaces := make([]uint32, 0, count)
	for i := range count {
		keyspaces = append(keyspaces, start+uint32(i))
	}
	return keyspaces
}

// TestDoPatrolKeyspaceGroupSizeForAutoSplit tests the auto-split patrol logic:
// when a group's keyspace count exceeds the default threshold (40k),
// it splits about half of the keyspaces into a new group.
func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplit() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	// Bootstrap the manager so the in-memory group index is initialized before the patrol
	// calls SplitKeyspaceGroupByID.
	re.NoError(kgm.Bootstrap(suite.ctx))
	// Build a keyspace list that is exactly one element above the default split threshold,
	// so this test validates the current production default instead of a failpoint override.
	keyspaces := make([]uint32, 0, defaultKeyspaceCountSplitThreshold+1)
	for i := 0; i <= defaultKeyspaceCountSplitThreshold; i++ {
		keyspaces = append(keyspaces, uint32(i))
	}

	// Overwrite the default keyspace group with enough keyspaces to trigger auto-split.
	err := store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		kg := &endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: keyspaces,
			Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount),
		}
		return store.SaveKeyspaceGroup(txn, kg)
	})
	re.NoError(err)

	// Run one round of patrol; should split group 0 into 0 and 1.
	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)
	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	// The patrol uses splitIdx := count / 2, so with 40001 keyspaces the source keeps 20000
	// and the target receives the remaining 20001.
	re.Len(kg0.Keyspaces, defaultKeyspaceCountSplitThreshold/2)
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.NotNil(kg1)
	// Assert exact ordering as well, to verify the patrol moves the tail half of the slice.
	re.Len(kg1.Keyspaces, defaultKeyspaceCountSplitThreshold/2+1)
	re.Equal(keyspaces[:defaultKeyspaceCountSplitThreshold/2], kg0.Keyspaces)
	re.Equal(keyspaces[defaultKeyspaceCountSplitThreshold/2:], kg1.Keyspaces)
	re.True(kg1.IsSplitTarget())
	re.Equal(constant.DefaultKeyspaceGroupID, kg1.SplitSource())
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitBelowThreshold() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	keyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold)
	savePatrolTestKeyspaceGroups(suite.ctx, suite.T(), store, &endpoint.KeyspaceGroup{
		ID:        constant.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Basic.String(),
		Keyspaces: keyspaces,
		Members:   testKeyspaceGroupMembers(),
	})

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(keyspaces, kg0.Keyspaces)
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Nil(kg1)
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitSkipsSplittingAndMergingGroups() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	splittingKeyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold+1)
	mergingKeyspaces := buildSequentialKeyspaces(100000, defaultKeyspaceCountSplitThreshold+1)
	eligibleKeyspaces := buildSequentialKeyspaces(200000, defaultKeyspaceCountSplitThreshold+1)
	savePatrolTestKeyspaceGroups(
		suite.ctx,
		suite.T(),
		store,
		&endpoint.KeyspaceGroup{
			ID:         constant.DefaultKeyspaceGroupID,
			UserKind:   endpoint.Basic.String(),
			Keyspaces:  splittingKeyspaces,
			Members:    testKeyspaceGroupMembers(),
			SplitState: &endpoint.SplitState{SplitSource: constant.DefaultKeyspaceGroupID},
		},
		&endpoint.KeyspaceGroup{
			ID:         1,
			UserKind:   endpoint.Standard.String(),
			Keyspaces:  mergingKeyspaces,
			Members:    testKeyspaceGroupMembers(),
			MergeState: &endpoint.MergeState{MergeList: []uint32{1}},
		},
		&endpoint.KeyspaceGroup{
			ID:        2,
			UserKind:  endpoint.Standard.String(),
			Keyspaces: eligibleKeyspaces,
			Members:   testKeyspaceGroupMembers(),
		},
	)

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(splittingKeyspaces, kg0.Keyspaces)
	re.True(kg0.IsSplitting())

	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.NotNil(kg1)
	re.Equal(mergingKeyspaces, kg1.Keyspaces)
	re.True(kg1.IsMerging())

	kg2, err := kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.NotNil(kg2)
	re.Equal(eligibleKeyspaces[:defaultKeyspaceCountSplitThreshold/2], kg2.Keyspaces)
	re.True(kg2.IsSplitSource())

	kg3, err := kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.NotNil(kg3)
	re.Equal(eligibleKeyspaces[defaultKeyspaceCountSplitThreshold/2:], kg3.Keyspaces)
	re.True(kg3.IsSplitTarget())
	re.Equal(uint32(2), kg3.SplitSource())
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitPrefersLargerGroups() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	// Arrange two eligible groups in ascending ID order, but make the larger one
	// sit behind the smaller one in storage so the patrol has to sort by size first.
	smallerKeyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold+1)
	largerKeyspaces := buildSequentialKeyspaces(100000, defaultKeyspaceCountSplitThreshold+2)
	savePatrolTestKeyspaceGroups(
		suite.ctx,
		suite.T(),
		store,
		&endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: smallerKeyspaces,
			Members:   testKeyspaceGroupMembers(),
		},
		&endpoint.KeyspaceGroup{
			ID:        1,
			UserKind:  endpoint.Standard.String(),
			Keyspaces: largerKeyspaces,
			Members:   testKeyspaceGroupMembers(),
		},
	)

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	// The smaller group should remain untouched because the larger group is split first.
	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(smallerKeyspaces, kg0.Keyspaces)
	re.False(kg0.IsSplitting())

	// The larger group is the one that gets split in this patrol round.
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.NotNil(kg1)
	expectedSplitIdx := (defaultKeyspaceCountSplitThreshold + 2) / 2
	re.Equal(largerKeyspaces[:expectedSplitIdx], kg1.Keyspaces)
	re.True(kg1.IsSplitSource())

	kg2, err := kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.NotNil(kg2)
	re.Equal(largerKeyspaces[expectedSplitIdx:], kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	re.Equal(uint32(1), kg2.SplitSource())
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitSkipsWhenNoAvailableTargetID() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	keyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold+1)
	savePatrolTestKeyspaceGroups(
		suite.ctx,
		suite.T(),
		store,
		&endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: keyspaces,
			Members:   testKeyspaceGroupMembers(),
		},
		&endpoint.KeyspaceGroup{
			ID:        mcs.MaxKeyspaceGroupCountInUse,
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{100},
		},
	)

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(keyspaces, kg0.Keyspaces)
	re.False(kg0.IsSplitting())
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitSkipsWhenNoKeyspacesToMove() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	// Use a synthetic malformed state to cover the defensive branch where the tail half
	// contains only default-keyspace entries, so there is nothing valid to move.
	keyspaces := make([]uint32, 0, defaultKeyspaceCountSplitThreshold+1)
	keyspaces = append(keyspaces, buildSequentialKeyspaces(1, defaultKeyspaceCountSplitThreshold/2)...)
	keyspaces = append(keyspaces, make([]uint32, defaultKeyspaceCountSplitThreshold/2+1)...)
	savePatrolTestKeyspaceGroups(suite.ctx, suite.T(), store, &endpoint.KeyspaceGroup{
		ID:        constant.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Basic.String(),
		Keyspaces: keyspaces,
		Members:   testKeyspaceGroupMembers(),
	})

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(keyspaces, kg0.Keyspaces)
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Nil(kg1)
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitSkipsUnderReplicatedGroups() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	// The first group exceeds the split threshold but is intentionally short on replicas.
	// The second group is eligible, so the patrol should skip the first and continue.
	underReplicatedKeyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold+1)
	eligibleKeyspaces := buildSequentialKeyspaces(100000, defaultKeyspaceCountSplitThreshold+1)
	savePatrolTestKeyspaceGroups(
		suite.ctx,
		suite.T(),
		store,
		&endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: underReplicatedKeyspaces,
			Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount-1),
		},
		&endpoint.KeyspaceGroup{
			ID:        1,
			UserKind:  endpoint.Standard.String(),
			Keyspaces: eligibleKeyspaces,
			Members:   testKeyspaceGroupMembers(),
		},
	)

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(suite.ctx)

	// The under-replicated source group must be left untouched.
	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(underReplicatedKeyspaces, kg0.Keyspaces)
	re.False(kg0.IsSplitting())

	// A later eligible group should still be auto-split in the same patrol round.
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.NotNil(kg1)
	expectedSplitIdx := (defaultKeyspaceCountSplitThreshold + 1) / 2
	re.Equal(eligibleKeyspaces[:expectedSplitIdx], kg1.Keyspaces)
	re.True(kg1.IsSplitSource())

	kg2, err := kgm.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.NotNil(kg2)
	re.Equal(eligibleKeyspaces[expectedSplitIdx:], kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	re.Equal(uint32(1), kg2.SplitSource())
}

func (suite *keyspaceGroupTestSuite) TestDoPatrolKeyspaceGroupSizeForAutoSplitRespectsCanceledContext() {
	re := suite.Require()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	keyspaces := buildSequentialKeyspaces(0, defaultKeyspaceCountSplitThreshold+1)
	savePatrolTestKeyspaceGroups(suite.ctx, suite.T(), store, &endpoint.KeyspaceGroup{
		ID:        constant.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Basic.String(),
		Keyspaces: keyspaces,
		Members:   testKeyspaceGroupMembers(),
	})

	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	re.NoError(kgm.Bootstrap(suite.ctx))

	ctx, cancel := context.WithCancel(suite.ctx)
	cancel()

	kgm.doPatrolKeyspaceGroupSizeForAutoSplit(ctx)

	kg0, err := kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(kg0)
	re.Equal(keyspaces, kg0.Keyspaces)
	kg1, err := kgm.GetKeyspaceGroupByID(1)
	re.NoError(err)
	re.Nil(kg1)
}

func TestParsePrimaryName(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name     string
		expected string
	}{
		{"127.0.0.1:2379-00000", "127.0.0.1:2379"},
		{"http://127.0.0.1:2379-10000", "http://127.0.0.1:2379"},
		{"https://127.0.0.1:2379-00001", "https://127.0.0.1:2379"},
		{"http://[::1]:2379-00002", "http://[::1]:2379"},
		{"https://[::1]:2379-00003", "https://[::1]:2379"},
		{"https://a-b-c-d-e-f-g:2379-00004", "https://a-b-c-d-e-f-g:2379"},
		{"https://pd-tso-server-0.tso-service.tidb-serverless.svc:2379-00002", "https://pd-tso-server-0.tso-service.tidb-serverless.svc:2379"},
		{"http://pd-tso-server-0.tso-service.tidb-serverless.svc:2379-00002", "http://pd-tso-server-0.tso-service.tidb-serverless.svc:2379"},
		{"pd-tso-server-0.tso-service.tidb-serverless.svc:2379-00000", "pd-tso-server-0.tso-service.tidb-serverless.svc:2379"},
	}
	for _, tc := range testCases {
		re.Equal(tc.expected, parsePrimaryName(tc.name))
	}
}
