// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	debugPrefix = "pd/api/v1/debug"
)

// NewDebugCommand New a debug subcommand of the rootCmd
func NewDebugCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "debug <is_enable>",
		Short: "enable debug info",
		Run:   debugCommandFunc,
	}
	return conf
}

func debugCommandFunc(cmd *cobra.Command, args []string) {
	var debug bool
	var err error
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	} else if debug, err = strconv.ParseBool(args[0]); err != nil {
		fmt.Println("argument should be a bool var")
		return
	}

	data, err := json.Marshal(debug)
	if err != nil {
		fmt.Printf("Failed to set debug mode: %s\n", err)
		return
	}
	req, err := getRequest(cmd, debugPrefix, http.MethodPost, "application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Printf("Failed to set debug mode: %s\n", err)
		return
	}
	_, err = dail(req)
	if err != nil {
		fmt.Printf("Failed to set debug mode: %s\n", err)
		return
	}
	fmt.Println("Success!")
}
