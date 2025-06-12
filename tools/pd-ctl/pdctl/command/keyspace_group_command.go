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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

const keyspaceGroupsPrefix = "pd/api/v2/tso/keyspace-groups"

// NewKeyspaceGroupCommand return a keyspace group subcommand of rootCmd
func NewKeyspaceGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keyspace-group [command] [flags]",
		Short: "show keyspace group information",
		Run:   showKeyspaceGroupsCommandFunc,
	}
	cmd.AddCommand(newSplitKeyspaceGroupCommand())
	cmd.AddCommand(newSplitRangeKeyspaceGroupCommand())
	cmd.AddCommand(newFinishSplitKeyspaceGroupCommand())
	cmd.AddCommand(newMergeKeyspaceGroupCommand())
	cmd.AddCommand(newFinishMergeKeyspaceGroupCommand())
	cmd.AddCommand(newSetNodesKeyspaceGroupCommand())
	cmd.AddCommand(newSetPriorityKeyspaceGroupCommand())
	cmd.AddCommand(newShowKeyspaceGroupPrimaryCommand())
	cmd.Flags().String("state", "", "state filter")
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
	r.Flags().Bool("all", false, "merge all keyspace groups into the default one")
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

func newShowKeyspaceGroupPrimaryCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "primary <keyspace_group_id>",
		Short: "show th primary of tso nodes for keyspace group with the given ID.",
		Run:   showKeyspaceGroupPrimaryCommandFunc,
	}
	return r
}

func showKeyspaceGroupsCommandFunc(cmd *cobra.Command, args []string) {
	prefix := keyspaceGroupsPrefix
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	cFunc := convertToKeyspaceGroups
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			cmd.Println("keyspace_group_id should be a number")
			return
		}
		prefix = fmt.Sprintf("%s/%s", keyspaceGroupsPrefix, args[0])
		cFunc = convertToKeyspaceGroup
	} else {
		flags := cmd.Flags()
		state, err := flags.GetString("state")
		if err != nil {
			cmd.Printf("Failed to get state: %s\n", err)
		}
		stateValue := ""
		if len(state) != 0 {
			state = strings.ToLower(state)
			switch state {
			case "merge", "split":
				stateValue = fmt.Sprintf("state=%v", state)
			default:
				cmd.Println("Unknown state: " + state)
				return
			}
		}

		if len(stateValue) != 0 {
			prefix = fmt.Sprintf("%v?%v", keyspaceGroupsPrefix, stateValue)
		}
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the keyspace groups information: %s\n", err)
		return
	}
	r = cFunc(r)
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
	postJSON(cmd, fmt.Sprintf("%s/%s/split", keyspaceGroupsPrefix, args[0]), map[string]any{
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
	postJSON(cmd, fmt.Sprintf("%s/%s/split", keyspaceGroupsPrefix, args[0]), map[string]any{
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
		cmd.Printf("Failed to finish split-keyspace-group: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func mergeKeyspaceGroupCommandFunc(cmd *cobra.Command, args []string) {
	var (
		targetGroupID uint32
		params        = map[string]any{}
		argNum        = len(args)
	)
	mergeAll, err := cmd.Flags().GetBool("all")
	if err != nil {
		cmd.Printf("Failed to get the merge all flag: %s\n", err)
		return
	}
	if argNum == 1 && mergeAll {
		target, err := strconv.ParseUint(args[0], 10, 32)
		if err != nil {
			cmd.Printf("Failed to parse the target keyspace group ID: %s\n", err)
			return
		}
		targetGroupID = uint32(target)
		if targetGroupID != constant.DefaultKeyspaceGroupID {
			cmd.Println("Unable to merge all keyspace groups into a non-default keyspace group")
			return
		}
		params["merge-all-into-default"] = true
	} else if argNum >= 2 && !mergeAll {
		target, err := strconv.ParseUint(args[0], 10, 32)
		if err != nil {
			cmd.Printf("Failed to parse the target keyspace group ID: %s\n", err)
			return
		}
		targetGroupID = uint32(target)
		groups := make([]uint32, 0, len(args)-1)
		for _, arg := range args[1:] {
			id, err := strconv.ParseUint(arg, 10, 32)
			if err != nil {
				cmd.Printf("Failed to parse the keyspace ID: %s\n", err)
				return
			}
			groups = append(groups, uint32(id))
		}
		params["merge-list"] = groups
	} else {
		cmd.Println("Must specify the source keyspace group ID(s) or the merge all flag")
		cmd.Usage()
		return
	}
	// TODO: implement the retry mechanism under merge all flag.
	postJSON(cmd, fmt.Sprintf("%s/%d/merge", keyspaceGroupsPrefix, targetGroupID), params)
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
		cmd.Printf("Failed to finish merge-keyspace-group: %s\n", err)
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
	nodes := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		u, err := url.ParseRequestURI(arg)
		if u == nil || err != nil {
			cmd.Printf("Failed to parse the tso node address: %s\n", err)
			return
		}
		nodes = append(nodes, arg)
	}
	patchJSON(cmd, fmt.Sprintf("%s/%s", keyspaceGroupsPrefix, args[0]), map[string]any{
		"Nodes": nodes,
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

	node := args[1]
	u, err := url.ParseRequestURI(node)
	if u == nil || err != nil {
		cmd.Printf("Failed to parse the tso node address: %s\n", err)
		return
	}

	// Escape the node address to avoid the error of parsing the url
	// But the url.PathEscape will escape the '/' to '%2F', which % will cause the error of parsing the url
	// So we need to replace the % to \%
	node = url.PathEscape(node)
	node = strings.ReplaceAll(node, "%", "\\%")

	priority, err := strconv.ParseInt(args[2], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the priority: %s\n", err)
		return
	}

	patchJSON(cmd, fmt.Sprintf("%s/%s/%s", keyspaceGroupsPrefix, args[0], node), map[string]any{
		"Priority": priority,
	})
}

func showKeyspaceGroupPrimaryCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Usage()
		return
	}
	_, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		cmd.Printf("Failed to parse the keyspace group ID: %s\n", err)
		return
	}
	r, err := doRequest(cmd, fmt.Sprintf("%s/%s?fields=primary", keyspaceGroupsPrefix, args[0]), http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the keyspace group primary information: %s\n", err)
		return
	}
	cmd.Println(r)
}

func convertToKeyspaceGroup(content string) string {
	kg := endpoint.KeyspaceGroup{}
	err := json.Unmarshal([]byte(content), &kg)
	if err != nil {
		return content
	}
	byteArr, err := json.MarshalIndent(kg, "", "  ")
	if err != nil {
		return content
	}
	return string(byteArr)
}

func convertToKeyspaceGroups(content string) string {
	kgs := []*endpoint.KeyspaceGroup{}
	err := json.Unmarshal([]byte(content), &kgs)
	if err != nil {
		return content
	}
	byteArr, err := json.MarshalIndent(kgs, "", "  ")
	if err != nil {
		return content
	}
	return string(byteArr)
}
