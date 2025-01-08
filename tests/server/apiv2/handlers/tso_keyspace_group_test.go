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

package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestPDServiceCluster(suite.ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceGroupTestSuite) TestCreateKeyspaceGroups() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}
	MustCreateKeyspaceGroup(re, suite.server, kgs)

	// miss user kind, use default value.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID: uint32(3),
		},
	}}
	MustCreateKeyspaceGroup(re, suite.server, kgs)

	// invalid user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(4),
			UserKind: "invalid",
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusBadRequest)

	// miss ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusInternalServerError)

	// invalid ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       constant.MaxKeyspaceGroupCount + 1,
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusBadRequest)

	// repeated ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusInternalServerError)
}

func (suite *keyspaceGroupTestSuite) TestLoadKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}

	MustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 3)
}

func (suite *keyspaceGroupTestSuite) TestSplitKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
			Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
		},
	}}

	MustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 2)
	MustSplitKeyspaceGroup(re, suite.server, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     uint32(2),
		Keyspaces: []uint32{111, 222},
	})
	resp = MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 3)
	// Check keyspace group 1.
	kg1 := MustLoadKeyspaceGroupByID(re, suite.server, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{333}, kg1.Keyspaces)
	re.True(kg1.IsSplitSource())
	re.Equal(kg1.ID, kg1.SplitSource())
	// Check keyspace group 2.
	kg2 := MustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 222}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	re.Equal(kg1.ID, kg2.SplitSource())
	// They should have the same user kind and members.
	re.Equal(kg1.UserKind, kg2.UserKind)
	re.Equal(kg1.Members, kg2.Members)
	// Finish the split and check the split state.
	MustFinishSplitKeyspaceGroup(re, suite.server, 2)
	kg1 = MustLoadKeyspaceGroupByID(re, suite.server, 1)
	re.False(kg1.IsSplitting())
	kg2 = MustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.False(kg2.IsSplitting())
}
