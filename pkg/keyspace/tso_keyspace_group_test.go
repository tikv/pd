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
	suite.kgm = NewKeyspaceGroupManager(suite.ctx, store)
	idAllocator := mockid.NewIDAllocator()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	suite.kg = NewKeyspaceManager(store, cluster, idAllocator, &mockConfig{}, suite.kgm)
	suite.NoError(suite.kgm.Bootstrap())
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
	kg, err = suite.kgm.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Equal(uint32(3), kg.ID)
	re.Equal(endpoint.Standard.String(), kg.UserKind)
	// remove the keyspace group 3
	err = suite.kgm.DeleteKeyspaceGroupByID(3)
	re.NoError(err)
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
			Now: time.Now().Unix(),
		})
		re.NoError(err)
	}

	for i := 1; i <= 3; i++ {
		kg, err := suite.kgm.GetKeyspaceGroupByID(uint32(i))
		re.NoError(err)
		re.Len(kg.Keyspaces, 33)
	}
}
