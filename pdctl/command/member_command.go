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
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var (
	membersPrefix      = "pd/api/v1/members"
	leaderMemberPrefix = "pd/api/v1/leader"
)

// NewMemberCommand return a member subcommand of rootCmd
func NewMemberCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "member [leader|delete]",
		Short: "show the pd member status",
		Run:   showMemberCommandFunc,
	}
	m.AddCommand(NewLeaderMemberCommand())
	m.AddCommand(NewDeleteMemberCommand())
	return m
}

// NewDeleteMemberCommand return a delete subcommand of memberCmd
func NewDeleteMemberCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <subcommand>",
		Short: "delete a member",
	}
	d.AddCommand(&cobra.Command{
		Use:   "name <member_name>",
		Short: "delete a member by name",
		Run:   deleteMemberByNameCommandFunc,
	})
	d.AddCommand(&cobra.Command{
		Use:   "id <member_id>",
		Short: "delete a member by id",
		Run:   deleteMemberByIDCommandFunc,
	})
	return d
}

// NewLeaderMemberCommand return a leader subcommand of memberCmd
func NewLeaderMemberCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "leader",
		Short: "show the leader member status",
		Run:   getLeaderMemberCommandFunc,
	}
	return l
}

func showMemberCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, membersPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get pd members: %s\n", err)
		return
	}
	fmt.Println(r)
}

func deleteMemberByNameCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: member delete <member_name>")
		return
	}
	prefix := membersPrefix + "/name/" + args[0]
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		fmt.Printf("Failed to delete member %s: %s\n", args[0], err)
		return
	}
	fmt.Println("Success!")
}

func deleteMemberByIDCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: member delete id <member_id>")
		return
	}
	prefix := membersPrefix + "/id/" + args[0]
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		fmt.Printf("Failed to delete member %s: %s\n", args[0], err)
		return
	}
	fmt.Println("Success!")
}

func getLeaderMemberCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, leaderMemberPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the leader of pd members: %s\n", err)
		return
	}
	fmt.Println(r)
}
