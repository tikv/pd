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

package command

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

const keyspaceGroupsPrefix = "pd/api/v2/tso/keyspace-groups"

// NewKeyspaceGroupCommand return a keyspace group subcommand of rootCmd
func NewKeyspaceGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keyspace-group <keyspace_group_id>",
		Short: "show keyspace group information with the given ID",
		Run:   showKeyspaceGroupCommandFunc,
	}
	cmd.AddCommand(newSplitKeyspaceGroupCommand())
	return cmd
}

func newSplitKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "split <keyspace_group_id> <new_keyspace_group_id> [<keyspace_id>]",
		Short: "split the keyspace group with the given ID and transfer the keyspaces into the newly split one",
		Run:   splitKeyspaceGroupCommandFunc,
	}
	return r
}

func showKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Usage()
		return
	}
	r, err := doRequest(cmd, fmt.Sprintf("%s/%s", keyspaceGroupsPrefix, args[0]), http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the keyspace groups information: %s\n", err)
		return
	}
	cmd.Println(r)
}

func splitKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		cmd.Usage()
		return
	}
	newID, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the new keyspace group ID: %s\n", err)
		return
	}
	keyspaces := make([]uint32, 0, len(args)-2)
	for _, arg := range args[2:] {
		id, err := strconv.ParseUint(arg, 10, 32)
		if err != nil {
			cmd.Printf("Failed to parse the keyspace ID: %s\n", err)
			return
		}
		keyspaces = append(keyspaces, uint32(id))
	}
	postJSON(cmd, fmt.Sprintf("%s/%s/split", keyspaceGroupsPrefix, args[0]), map[string]interface{}{
		"new-id":    uint32(newID),
		"keyspaces": keyspaces,
	})
}
