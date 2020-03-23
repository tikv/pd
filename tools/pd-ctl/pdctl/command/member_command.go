// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	membersPrefix      = "pd/api/v1/members"
	leaderMemberPrefix = "pd/api/v1/leader"
)

func showMemberCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, membersPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get pd members: %s\n", err)
		return
	}
	cmd.Println(r)
}

func deleteMemberByNameCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println("Usage: member delete <member_name>")
		return
	}
	prefix := membersPrefix + "/name/" + args[0]
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to delete member %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func deleteMemberByIDCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println("Usage: member delete id <member_id>")
		return
	}
	prefix := membersPrefix + "/id/" + args[0]
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to delete member %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func getLeaderMemberCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, leaderMemberPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get the leader of pd members: %s\n", err)
		return
	}
	cmd.Println(r)
}

func resignLeaderCommandFunc(cmd *cobra.Command, args []string) {
	prefix := leaderMemberPrefix + "/resign"
	_, err := doRequest(cmd, prefix, http.MethodPost)
	if err != nil {
		cmd.Printf("Failed to resign: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func transferPDLeaderCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println("Usage: leader transfer <member_name>")
		return
	}
	prefix := leaderMemberPrefix + "/transfer/" + args[0]
	_, err := doRequest(cmd, prefix, http.MethodPost)
	if err != nil {
		cmd.Printf("Failed to trasfer leadership: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func setLeaderPriorityFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println("Usage: leader_priority <member_name> <priority>")
		return
	}
	prefix := membersPrefix + "/name/" + args[0]
	priority, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		cmd.Printf("failed to parse priority: %v\n", err)
		return
	}
	data := map[string]interface{}{"leader-priority": priority}
	reqData, _ := json.Marshal(data)
	_, err = doRequest(cmd, prefix, http.MethodPost, WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		cmd.Printf("failed to set leader priority: %v\n", err)
		return
	}
	cmd.Println("Success!")
}
