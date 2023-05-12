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

package keyspace_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestAPICluster(ctx, 1)
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	// Show keyspace group information.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", utils.DefaultKeyspaceGroupID)
	args := []string{"-u", pdAddr, "keyspace-group"}
	output, err := pdctl.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(utils.DefaultKeyspaceGroupID, keyspaceGroup.ID)
	re.Contains(keyspaceGroup.Keyspaces, utils.DefaultKeyspaceID)
	// Split keyspace group.
	handlersutil.MustCreateKeyspaceGroup(re, leaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   make([]endpoint.KeyspaceGroupMember, utils.KeyspaceGroupDefaultReplicaCount),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	_, err = pdctl.ExecuteCommand(cmd, append(args, "split", "1", "2", "222", "333")...)
	re.NoError(err)
	output, err = pdctl.ExecuteCommand(cmd, append(args, "1")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(1), keyspaceGroup.ID)
	re.Equal(keyspaceGroup.Keyspaces, []uint32{111})
	output, err = pdctl.ExecuteCommand(cmd, append(args, "2")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(2), keyspaceGroup.ID)
	re.Equal(keyspaceGroup.Keyspaces, []uint32{222, 333})
}
