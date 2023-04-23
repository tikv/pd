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

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
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
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	suite.kgm = NewKeyspaceGroupManager(suite.ctx, store, nil, 0)
	idAllocator := mockid.NewIDAllocator()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	suite.kg = NewKeyspaceManager(store, cluster, idAllocator, &mockConfig{}, suite.kgm)
	suite.NoError(suite.kgm.Bootstrap(suite.ctx))
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
	kg, err := suite.kgm.GetKeyspaceGroupByID(0)
	re.NoError(err)
	re.Equal(uint32(0), kg.ID)
	re.Equal(endpoint.Basic.String(), kg.UserKind)
	re.False(kg.IsSplitting())
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

	for i := 0; i < 99; i++ {
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
	re.Len(kg3.Keyspaces, 0)

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
	re.Len(kg3.Keyspaces, 0)
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
	re.Len(kg2.Keyspaces, 0)
	kg3, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Len(kg3.Keyspaces, 1)
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupSplit() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Basic.String(),
		},
		{
			ID:        uint32(2),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
		},
	}
	err := suite.kgm.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
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
	re.ErrorIs(err, ErrKeyspaceGroupNotInSplit)
	// finish the split of a non-existing keyspace group
	err = suite.kgm.FinishSplitKeyspaceByID(5)
	re.ErrorIs(err, ErrKeyspaceGroupNotExists)
	// split the in-split keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, []uint32{333})
	re.ErrorIs(err, ErrKeyspaceGroupInSplit)
	// remove the in-split keyspace group
	kg2, err = suite.kgm.DeleteKeyspaceGroupByID(2)
	re.Nil(kg2)
	re.ErrorIs(err, ErrKeyspaceGroupInSplit)
	kg4, err = suite.kgm.DeleteKeyspaceGroupByID(4)
	re.Nil(kg4)
	re.ErrorIs(err, ErrKeyspaceGroupInSplit)
	// update the in-split keyspace group
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "2", 444, opAdd)
	re.ErrorIs(err, ErrKeyspaceGroupInSplit)
	err = suite.kg.kgm.UpdateKeyspaceForGroup(endpoint.Standard, "4", 444, opAdd)
	re.ErrorIs(err, ErrKeyspaceGroupInSplit)

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
	re.ErrorIs(err, ErrKeyspaceGroupNotExists)
	// split into an existing keyspace group
	err = suite.kgm.SplitKeyspaceGroupByID(2, 4, nil)
	re.ErrorIs(err, ErrKeyspaceGroupExists)
	// split with the wrong keyspaces.
	err = suite.kgm.SplitKeyspaceGroupByID(2, 5, []uint32{111, 222, 444})
	re.ErrorIs(err, ErrKeyspaceNotInKeyspaceGroup)
}
