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
	manager *GroupManager
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	suite.manager = NewKeyspaceGroupManager(context.Background(), store)
	suite.NoError(suite.manager.Bootstrap())
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupOperations() {
	re := suite.Require()

	keyspaceGroups := []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: "business",
		},
		{
			ID:       uint32(2),
			UserKind: "business",
		},
		{
			ID:       uint32(3),
			UserKind: "business",
		},
	}
	err := suite.manager.CreateKeyspaces(keyspaceGroups)
	re.NoError(err)
	// list all keyspace groups
	kgs, err := suite.manager.GetKeyspaceGroups()
	re.NoError(err)
	re.Len(kgs, 4)
	// get the default keyspace group
	kg, err := suite.manager.GetKeyspaceGroupByID(0)
	re.NoError(err)
	re.Equal(uint32(0), kg.ID)
	re.Equal("default", kg.UserKind)
	kg, err = suite.manager.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Equal(uint32(3), kg.ID)
	re.Equal("business", kg.UserKind)
	// remove the keyspace group 3
	err = suite.manager.DeleteKeyspaceGroupByID(3)
	re.NoError(err)
	// get non-existing keyspace group
	kg, err = suite.manager.GetKeyspaceGroupByID(3)
	re.NoError(err)
	re.Empty(kg)

	// create an existing keyspace group
	keyspaceGroups = []*endpoint.KeyspaceGroup{{ID: uint32(1), UserKind: "business"}}
	err = suite.manager.CreateKeyspaces(keyspaceGroups)
	// only print the warning log
	re.NoError(err)
}
