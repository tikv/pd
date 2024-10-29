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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

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
	suite.kg = NewKeyspaceManager(suite.ctx, store, cluster, idAllocator, &mockConfig{}, suite.kgm)
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
			Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// split the default keyspace
	err = suite.kgm.SplitKeyspaceGroupByID(0, 4, []uint32{constant.DefaultKeyspaceID})
	re.ErrorIs(err, ErrModifyDefaultKeyspace)
	// split the keyspace group 1 to 4
	err = suite.kgm.SplitKeyspaceGroupByID(1, 4, []uint32{444})
	re.ErrorIs(err, ErrKeyspaceGroupNotEnoughReplicas)
	// split the keyspace group 2 to 4 without giving any keyspace
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{})
	re.ErrorIs(err, ErrKeyspaceNotInKeyspaceGroup)
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
	re.ErrorContains(err, ErrKeyspaceGroupNotInSplit(2).Error())
	// finish the split of a non-existing keyspace group
	err = suite.kgm.FinishSplitKeyspaceByID(5)
	re.ErrorContains(err, ErrKeyspaceGroupNotExists(5).Error())
	// split the in-split keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{333})
	re.ErrorContains(err, ErrKeyspaceGroupInSplit(2).Error())
	// remove the in-split keyspace group
	kg2, err = suite.kgm.DeleteKeyspaceGroupByID(2)
	re.Nil(kg2)
	re.ErrorContains(err, ErrKeyspaceGroupInSplit(2).Error())
	kg4, err = suite.kgm.DeleteKeyspaceGroupByID(4)
	re.Nil(kg4)
	re.ErrorContains(err, ErrKeyspaceGroupInSplit(4).Error())
	// update the in-split keyspace group
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "2", 444, opAdd)
	re.ErrorContains(err, ErrKeyspaceGroupInSplit(2).Error())
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "4", 444, opAdd)
	re.ErrorContains(err, ErrKeyspaceGroupInSplit(4).Error())

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
	re.ErrorContains(err, ErrKeyspaceGroupNotExists(3).Error())
	// split into an existing keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{111})
	re.ErrorIs(err, ErrKeyspaceGroupExists)
	// split with the wrong keyspaces.
	err = suite.kgm.SplitKeyspaceGroupByID(2, 5, []uint32{111, 222, 444})
	re.ErrorIs(err, ErrKeyspaceNotInKeyspaceGroup)
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
			Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
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
			Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
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
	re.ErrorContains(err, ErrKeyspaceGroupNotExists(5).Error())
	// merge with the number of keyspace groups exceeds the limit
	err = suite.kgm.MergeKeyspaceGroups(1, make([]uint32, etcdutil.MaxEtcdTxnOps/2))
	re.ErrorIs(err, ErrExceedMaxEtcdTxnOps)
	// merge the default keyspace group
	err = suite.kgm.MergeKeyspaceGroups(1, []uint32{constant.DefaultKeyspaceGroupID})
	re.ErrorIs(err, ErrModifyDefaultKeyspaceGroup)
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
			err: ErrKeyspaceNotInKeyspaceGroup,
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
			err:             ErrKeyspaceGroupWithEmptyKeyspace,
		},
		{
			old: []uint32{1, 2, 3, 4, 5},
			err: ErrKeyspaceNotInKeyspaceGroup,
		},
	}
	for idx, testCase := range testCases {
		old, new, err := buildSplitKeyspaces(testCase.old, testCase.new, testCase.startKeyspaceID, testCase.endKeyspaceID)
		if testCase.err != nil {
			re.ErrorIs(testCase.err, err, "test case %d", idx)
		} else {
			re.NoError(err, "test case %d", idx)
			re.Equal(testCase.expectedOld, old, "test case %d", idx)
			re.Equal(testCase.expectedNew, new, "test case %d", idx)
		}
	}
}
