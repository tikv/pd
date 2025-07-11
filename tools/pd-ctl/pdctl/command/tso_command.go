// Copyright 2017 TiKV Project Authors.
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
	"net/http"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/pkg/utils/tsoutil"
)

var (
	tsoMemberPrefix = "/api/v1/primary/transfer"
)

// NewTSOCommand return a TSO subcommand of rootCmd
func NewTSOCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tso <timestamp|leader>",
		Short: "parse TSO to the system and logic time",
		Run:   showTSOCommandFunc,
	}
	cmd.AddCommand(NewTSOMemberCommand())
	return cmd
}

// NewTSOMemberCommand return a leader subcommand of tsoCmd
func NewTSOMemberCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "leader <subcommand>",
		Short: "leader commands",
	}
	d.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "show the leader member status",
		Run:   getLeaderMemberCommandFunc,
	})
	d.AddCommand(&cobra.Command{
		Use:   "resign",
		Short: "resign current leader pd's leadership",
		Run:   resignLeaderCommandFunc,
	})
	d.AddCommand(&cobra.Command{
		Use:   "transfer <member_name>",
		Short: "transfer leadership to another pd",
		Run:   transferPDLeaderCommandFunc,
	})
	return d
}

func getTsoMemberCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, leaderMemberPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the leader of pd members: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showTSOCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println("Usage: tso <timestamp>")
		return
	}
	ts, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Printf("Failed to parse TSO: %s\n", err)
		return
	}

	physicalTime, logical := tsoutil.ParseTS(ts)
	cmd.Println("system: ", physicalTime)
	cmd.Println("logic:  ", logical)
}
