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
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	manager *GroupManager
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	suite.manager = NewKeyspaceGroupManager(suite.ctx, store)
	suite.NoError(suite.manager.Bootstrap())
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
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(3),
			UserKind: endpoint.Standard.String(),
		},
	}
	err := suite.manager.CreateKeyspaceGroups(keyspaceGroups)
	re.NoError(err)
	// list all keyspace groups
	kgs, err := suite.manager.GetKeyspaceGroups(uint32(0), 0)
	re.NoError(err)
	re.Len(kgs, 4)
	// list part of keyspace groups
	kgs, err = suite.manager.GetKeyspaceGroups(uint32(1), 2)
	re.NoError(err)
	re.Len(kgs, 2)
	// get the default keyspace group
	kg, err := suite.manager.GetKeyspaceGroupByID(0)
	re.NoError(err)
	re.Equal(uint32(0), kg.ID)
	re.Equal(endpoint.Basic.String(), kg.UserKind)
	re.False(kg.InSplit)
	kg, err = suite.manager.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Equal(uint32(3), kg.ID)
	re.Equal(endpoint.Standard.String(), kg.UserKind)
	re.False(kg.InSplit)
	// remove the keyspace group 3
	err = suite.manager.DeleteKeyspaceGroupByID(3)
	re.NoError(err)
	// get non-existing keyspace group
	kg, err = suite.manager.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Empty(kg)
	// create an existing keyspace group
	keyspaceGroups = []*endpoint.KeyspaceGroup{{ID: uint32(1), UserKind: endpoint.Standard.String()}}
	err = suite.manager.CreateKeyspaceGroups(keyspaceGroups)
	re.Error(err)
	// split the keyspace group 2 to 4
	err = suite.manager.SplitKeyspaceGroupByID(2, 4)
	re.NoError(err)
	kg2, err := suite.manager.GetKeyspaceGroupByID(2)
	re.NoError(err)
	re.Equal(uint32(2), kg2.ID)
	kg4, err := suite.manager.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.Equal(kg2.UserKind, kg4.UserKind)
	re.False(kg2.InSplit)
	re.True(kg4.InSplit)
	// finish the split of keyspace group 4
	err = suite.manager.FinishSplitKeyspaceByID(4)
	re.NoError(err)
	kg4, err = suite.manager.GetKeyspaceGroupByID(4)
	re.NoError(err)
	re.Equal(uint32(4), kg4.ID)
	re.False(kg4.InSplit)
	// split a non-existing keyspace group
	err = suite.manager.SplitKeyspaceGroupByID(3, 5)
	re.ErrorIs(err, ErrKeyspaceGroupNotFound)
	// finish the split of a non-existing keyspace group
	err = suite.manager.FinishSplitKeyspaceByID(5)
	re.ErrorIs(err, ErrKeyspaceGroupNotFound)
	// split into an existing keyspace group
	err = suite.manager.SplitKeyspaceGroupByID(2, 4)
	re.ErrorIs(err, ErrKeyspaceGroupExists)
}
