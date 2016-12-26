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
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var (
	membersPrefix      = "pd/api/v1/members"
	memberPrefix       = "pd/api/v1/members/%s"
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
		Use:   "delete <member_id>",
		Short: "delete the member",
		Run:   deleteMemberCommandFunc,
	}
	return d
}

// NewGetLeaderMemberCommand return a leader subcommand of memberCmd
func NewLeaderMemberCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "leader",
		Short: "show the leader member status",
		Run:   getLeaderMemberCommandFunc,
	}
	return l
}

func showMemberCommandFunc(cmd *cobra.Command, args []string) {
	url := getAddressFromCmd(cmd, membersPrefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get pd members:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		printResponseError(r)
		return
	}

	io.Copy(os.Stdout, r.Body)
}

func deleteMemberCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: member delete <member_id>")
		return
	}
	cli := &http.Client{}
	prefix := fmt.Sprintf(memberPrefix, args[0])
	url := getAddressFromCmd(cmd, prefix)
	r, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("Failed to delete member %s: [%s]\n", args[0], err)
		return
	}
	reps, err := cli.Do(r)
	if err != nil {
		fmt.Printf("Failed to delete member %s: [%s]\n", args[0], err)
		return
	}

	defer reps.Body.Close()
	if reps.StatusCode == http.StatusOK {
		fmt.Println("Success!")
	} else {
		printResponseError(reps)
	}
}

func getLeaderMemberCommandFunc(cmd *cobra.Command, args []string) {
	url := getAddressFromCmd(cmd, leaderMemberPrefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get the leader of pd members:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		printResponseError(r)
		return
	}

	io.Copy(os.Stdout, r.Body)
}
