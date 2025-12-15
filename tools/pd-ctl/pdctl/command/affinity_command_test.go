// Copyright 2025 TiKV Project Authors.
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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestParseRangeSpec(t *testing.T) {
	re := require.New(t)
	cmd := &cobra.Command{}
	cmd.Flags().String("format", "hex", "")

	kr, err := parseRangeSpec(cmd.Flags(), "6162:6364") // hex
	re.NoError(err)
	re.Equal([]byte("ab"), kr.StartKey)
	re.Equal([]byte("cd"), kr.EndKey)

	re.NoError(cmd.Flags().Set("format", "raw"))
	kr, err = parseRangeSpec(cmd.Flags(), ":")
	re.NoError(err)
	re.Empty(kr.StartKey)
	re.Empty(kr.EndKey)

	_, err = parseRangeSpec(cmd.Flags(), "invalid")
	re.Error(err)

	_, err = parseRangeSpec(cmd.Flags(), "b:a")
	re.Error(err)
}

func TestValidateKeyRange(t *testing.T) {
	re := require.New(t)

	re.NoError(validateKeyRange([]byte("a"), []byte("b")))
	re.NoError(validateKeyRange(nil, nil))
	re.Error(validateKeyRange([]byte("b"), []byte("a")))
	re.Error(validateKeyRange([]byte("a"), nil))
	re.Error(validateKeyRange(nil, []byte("b")))
}

func TestLoadKeyRanges(t *testing.T) {
	re := require.New(t)

	cmd := &cobra.Command{}
	cmd.Flags().StringArray("range", nil, "")
	cmd.Flags().String("format", "hex", "")

	re.NoError(cmd.Flags().Set("range", "a:b"))
	re.NoError(cmd.Flags().Set("format", "raw"))
	ranges, err := loadKeyRanges(cmd)
	re.NoError(err)
	re.Len(ranges, 1)
	re.Equal([]byte("a"), ranges[0].StartKey)
	re.Equal([]byte("b"), ranges[0].EndKey)

	cmd = &cobra.Command{}
	cmd.Flags().StringArray("range", nil, "")
	cmd.Flags().String("format", "hex", "")
	_, err = loadKeyRanges(cmd)
	re.Error(err)
}

func TestGetGroupID(t *testing.T) {
	re := require.New(t)

	cmd := &cobra.Command{}
	cmd.Flags().String("group-id", "", "")

	_, err := getGroupID(cmd)
	re.Error(err)

	re.NoError(cmd.Flags().Set("group-id", "valid_id"))
	groupID, err := getGroupID(cmd)
	re.NoError(err)
	re.Equal("valid_id", groupID)

	re.NoError(cmd.Flags().Set("group-id", "invalid id"))
	_, err = getGroupID(cmd)
	re.Error(err)
}
