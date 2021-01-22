// Copyright 2021 TiKV Project Authors.
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

// NewLocationCommand return a member subcommand of rootCmd
func NewLocationCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "location <member_name> <dc_location>",
		Short: "update dc location of pd member",
		Run:   updateDcLocationByNameCommandFunc,
	}

	return m
}

func updateDcLocationByNameCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println("Usage: member dc_location name <member_name> <dc_location>")
		return
	}
	if len(args[1]) == 0 {
		cmd.Println("dc_location should not be empty")
		return
	}
	prefix := fmt.Sprintf("pd/api/v1/tso/dc-location/%v?dcLocation=%v", args[0], args[1])

	_, err := doRequest(cmd, prefix, http.MethodPost)
	if err != nil {
		cmd.Printf("Fail to set dc location to %s for member name %s: %s\n", args[1], args[0], err)
		return
	}
	cmd.Println("Success!")
}
