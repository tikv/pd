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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertToKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	content := `{"id":1,"user-kind":"basic","members":[{"address":"http://127.0.0.1:3379","priority":0}],"keyspaces":[1,2,3]}`

	output := convertToKeyspaceGroup(content, false)
	re.NotContains(output, "\"keyspaces\"")
	reJSONHasNoKeyspaces(re, output)

	output = convertToKeyspaceGroup(content, true)
	re.Contains(output, "\"keyspaces\"")
	reJSONHasKeyspaces(re, output, []any{float64(1), float64(2), float64(3)})
}

func TestConvertToKeyspaceGroups(t *testing.T) {
	re := require.New(t)
	content := `[{"id":1,"user-kind":"basic","members":[],"keyspaces":[1,2,3]},{"id":2,"user-kind":"standard","members":[],"keyspaces":[]}]`

	output := convertToKeyspaceGroups(content, false)
	re.NotContains(output, "\"keyspaces\"")
	var groups []map[string]any
	re.NoError(json.Unmarshal([]byte(output), &groups))
	re.Len(groups, 2)
	for _, group := range groups {
		_, ok := group["keyspaces"]
		re.False(ok)
	}

	output = convertToKeyspaceGroups(content, true)
	re.Contains(output, "\"keyspaces\"")
	re.NoError(json.Unmarshal([]byte(output), &groups))
	re.Equal([]any{float64(1), float64(2), float64(3)}, groups[0]["keyspaces"])
	re.Equal([]any{}, groups[1]["keyspaces"])
}

func TestNewKeyspaceGroupCommandDoesNotHideKeyspacesByDefault(t *testing.T) {
	re := require.New(t)
	flag := NewKeyspaceGroupCommand().Flags().Lookup(flagHideKGKeyspaces)
	re.NotNil(flag)
	re.Equal("false", flag.DefValue)
}

func reJSONHasNoKeyspaces(re *require.Assertions, output string) {
	var group map[string]any
	re.NoError(json.Unmarshal([]byte(output), &group))
	_, ok := group["keyspaces"]
	re.False(ok)
}

func reJSONHasKeyspaces(re *require.Assertions, output string, expected []any) {
	var group map[string]any
	re.NoError(json.Unmarshal([]byte(output), &group))
	re.Equal(expected, group["keyspaces"])
}
