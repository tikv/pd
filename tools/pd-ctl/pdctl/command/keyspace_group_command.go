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
	"net/url"
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
	cmd.AddCommand(newSplitRangeKeyspaceGroupCommand())
	cmd.AddCommand(newFinishSplitKeyspaceGroupCommand())
	cmd.AddCommand(newMergeKeyspaceGroupCommand())
	cmd.AddCommand(newFinishMergeKeyspaceGroupCommand())
	cmd.AddCommand(newSetNodesKeyspaceGroupCommand())
	cmd.AddCommand(newSetPriorityKeyspaceGroupCommand())
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

func newSplitRangeKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "split-range <keyspace_group_id> <new_keyspace_group_id> <start_keyspace_id> <end_keyspace_id>",
		Short: "split the keyspace group with the given ID and transfer the keyspaces in the given range (both ends inclusive) into the newly split one",
		Run:   splitRangeKeyspaceGroupCommandFunc,
	}
	return r
}

func newFinishSplitKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:    "finish-split <keyspace_group_id>",
		Short:  "finish split the keyspace group with the given ID",
		Run:    finishSplitKeyspaceGroupCommandFunc,
		Hidden: true,
	}
	return r
}

func newMergeKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "merge <target_keyspace_group_id> [<keyspace_group_id>]",
		Short: "merge the keyspace group with the given IDs into the target one",
		Run:   mergeKeyspaceGroupCommandFunc,
	}
	return r
}

func newFinishMergeKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:    "finish-merge <keyspace_group_id>",
		Short:  "finish merge the keyspace group with the given ID",
		Run:    finishMergeKeyspaceGroupCommandFunc,
		Hidden: true,
	}
	return r
}

func newSetNodesKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "set-node <keyspace_group_id> <tso_node_addr> [<tso_node_addr>...]",
		Short: "set the address of tso nodes for keyspace group with the given ID",
		Run:   setNodesKeyspaceGroupCommandFunc,
	}
	return r
}

func newSetPriorityKeyspaceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "set-priority <keyspace_group_id> <tso_node_addr> <priority>",
		Short: "set the priority of tso nodes for keyspace group with the given ID. If the priority is negative, it need to add a prefix with -- to avoid identified as flag.",
		Run:   setPriorityKeyspaceGroupCommandFunc,
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
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the old keyspace group ID: %s\n", err)
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

func splitRangeKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 4 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the old keyspace group ID: %s\n", err)
		return
	}
	newID, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the new keyspace group ID: %s\n", err)
		return
	}
	startKeyspaceID, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the start keyspace ID: %s\n", err)
		return
	}
	endKeyspaceID, err := strconv.ParseUint(args[3], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the end keyspace ID: %s\n", err)
		return
	}
	postJSON(cmd, fmt.Sprintf("%s/%s/split", keyspaceGroupsPrefix, args[0]), map[string]interface{}{
		"new-id":            uint32(newID),
		"start-keyspace-id": uint32(startKeyspaceID),
		"end-keyspace-id":   uint32(endKeyspaceID),
	})
}

func finishSplitKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the keyspace group ID: %s\n", err)
		return
	}
	_, err = doRequest(cmd, fmt.Sprintf("%s/%s/split", keyspaceGroupsPrefix, args[0]), http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

func mergeKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the target keyspace group ID: %s\n", err)
		return
	}
	groups := make([]uint32, 0, len(args)-1)
	for _, arg := range args[1:] {
		id, err := strconv.ParseUint(arg, 10, 32)
		if err != nil {
			cmd.Printf("Failed to parse the keyspace ID: %s\n", err)
			return
		}
		groups = append(groups, uint32(id))
	}
	postJSON(cmd, fmt.Sprintf("%s/%s/merge", keyspaceGroupsPrefix, args[0]), map[string]interface{}{
		"merge-list": groups,
	})
}

func finishMergeKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the keyspace group ID: %s\n", err)
		return
	}
	_, err = doRequest(cmd, fmt.Sprintf("%s/%s/merge", keyspaceGroupsPrefix, args[0]), http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

func setNodesKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the keyspace group ID: %s\n", err)
		return
	}
	addresses := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		u, err := url.ParseRequestURI(arg)
		if u == nil || err != nil {
			cmd.Printf("Failed to parse the tso node address: %s\n", err)
			return
		}
		addresses = append(addresses, arg)
	}
	postJSON(cmd, fmt.Sprintf("%s/%s/nodes", keyspaceGroupsPrefix, args[0]), map[string]interface{}{
		"Nodes": addresses,
	})
}

func setPriorityKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the keyspace group ID: %s\n", err)
		return
	}

	address := args[1]
	u, err := url.ParseRequestURI(address)
	if u == nil || err != nil {
		cmd.Printf("Failed to parse the tso node address: %s\n", err)
		return
	}

	priority, err := strconv.ParseInt(args[2], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the priority: %s\n", err)
		return
	}

	postJSON(cmd, fmt.Sprintf("%s/%s/priority", keyspaceGroupsPrefix, args[0]), map[string]interface{}{
		"Node":     address,
		"Priority": priority,
	})
}
