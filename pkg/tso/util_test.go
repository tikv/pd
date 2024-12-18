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

package tso

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestExtractKeyspaceGroupIDFromKeyspaceGroupMembershipPath(t *testing.T) {
	re := require.New(t)

	compiledRegexp := keypath.GetCompiledKeyspaceGroupIDRegexp()

	rightCases := []struct {
		path string
		id   uint32
	}{
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/00000", id: 0},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/00001", id: 1},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/12345", id: 12345},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/99999", id: 99999},
		{path: "tso/keyspace_groups/membership/00000", id: 0},
		{path: "tso/keyspace_groups/membership/00001", id: 1},
		{path: "tso/keyspace_groups/membership/12345", id: 12345},
		{path: "tso/keyspace_groups/membership/99999", id: 99999},
	}

	for _, tt := range rightCases {
		id, err := ExtractKeyspaceGroupIDFromPath(compiledRegexp, tt.path)
		re.Equal(tt.id, id)
		re.NoError(err)
	}

	wrongCases := []struct {
		path string
	}{
		{path: ""},
		{path: "00001"},
		{path: "xxx/keyspace_groups/membership/00001"},
		{path: "tso/xxxxxxxxxxxxxxx/membership/00001"},
		{path: "tso/keyspace_groups/xxxxxxxxxx/00001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/xxxxxxxxxx/00001"},
		{path: "/pd/{cluster_id}/xxx/keyspace_groups/membership/00001"},
		{path: "/pd/{cluster_id}/tso/xxxxxxxxxxxxxxx/membership/00001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/0"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/0001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/123456"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/1234a"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/12345a"},
	}

	for _, tt := range wrongCases {
		_, err := ExtractKeyspaceGroupIDFromPath(compiledRegexp, tt.path)
		re.Error(err)
	}
}

func TestExtractKeyspaceGroupIDFromKeyspaceGroupPrimaryPath(t *testing.T) {
	re := require.New(t)

	compiledRegexp := keypath.GetCompiledNonDefaultIDRegexp()

	rightCases := []struct {
		path string
		id   uint32
	}{
		{path: "/ms/0/tso/keyspace_groups/election/00001/primary", id: 1},
		{path: "/ms/0/tso/keyspace_groups/election/12345/primary", id: 12345},
		{path: "/ms/0/tso/keyspace_groups/election/99999/primary", id: 99999},
	}

	for _, tt := range rightCases {
		id, err := ExtractKeyspaceGroupIDFromPath(compiledRegexp, tt.path)
		re.Equal(tt.id, id)
		re.NoError(err)
	}
}
