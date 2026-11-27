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

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pd "github.com/tikv/pd/client/http"
)

type affinityStatusTestClient struct {
	pd.Client
	ids      []string
	response *pd.AffinityGroupsResponse
}

func (c *affinityStatusTestClient) GetAffinityGroupsWithDiagnostics(_ context.Context, ids []string) (*pd.AffinityGroupsResponse, error) {
	c.ids = append(c.ids, ids...)
	if c.response != nil {
		return c.response, nil
	}
	zero := uint64(0)
	return &pd.AffinityGroupsResponse{
		StatusVersion: 1,
		AffinityGroups: map[string]*pd.AffinityGroupState{
			"_tidb_t_1001": {
				AffinityGroup: pd.AffinityGroup{ID: "_tidb_t_1001"},
				Diagnostics: &pd.AffinityGroupDiagnostics{
					ActualLeaderStoreIDs: make([]uint64, 0),
					ActualVoterStoreIDs:  make([][]uint64, 0),
					ApproximateSizeMiB:   &zero,
					RunningOperators:     make([]pd.AffinityGroupOperatorCount, 0),
					BlockedReasons:       make([]pd.AffinityGroupBlockedReason, 0),
				},
			},
		},
	}, nil
}

func TestAffinityShowStatus(t *testing.T) {
	re := require.New(t)
	previous := PDCli
	client := &affinityStatusTestClient{}
	PDCli = client
	t.Cleanup(func() { PDCli = previous })

	cmd := NewAffinityCommand()
	cmd.PersistentPreRunE = nil
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"show", "--status"})
	re.ErrorContains(cmd.Execute(), "--status requires a target affinity group")
	re.Empty(client.ids)

	output.Reset()
	cmd.SetArgs([]string{"show", "--status", "--table-id", "1001"})
	re.NoError(cmd.Execute())
	re.Equal([]string{"_tidb_t_1001"}, client.ids)
	var state pd.AffinityGroupState
	re.NoError(json.Unmarshal(output.Bytes(), &state))
	re.Equal("_tidb_t_1001", state.ID)
	re.NotNil(state.Diagnostics)
	re.NotNil(state.Diagnostics.ActualLeaderStoreIDs)
	re.Empty(state.Diagnostics.ActualLeaderStoreIDs)
	re.NotContains(output.String(), `"affinity_groups"`)
	re.NotContains(output.String(), `"status_version"`)
}

func TestAffinityShowStatusErrors(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		response *pd.AffinityGroupsResponse
		want     string
	}{
		{name: "legacy server", response: &pd.AffinityGroupsResponse{}, want: "does not support affinity group diagnostics"},
		{name: "missing group", response: &pd.AffinityGroupsResponse{StatusVersion: 1, AffinityGroups: map[string]*pd.AffinityGroupState{}}, want: "not found"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			previous := PDCli
			PDCli = &affinityStatusTestClient{response: testCase.response}
			t.Cleanup(func() { PDCli = previous })
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs([]string{"show", "--status", "--table-id", "1001"})
			re.ErrorContains(cmd.Execute(), testCase.want)
		})
	}
}
