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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

const (
	resourceManagerPrefix = "resource-manager/api/v1"
	// flags
	rmConfigController = "config/controller"
)

// NewResourceManagerCommand return a resource manager subcommand of rootCmd
func NewResourceManagerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-manager <command> [flags]",
		Short: "resource-manager commands",
	}
	cmd.AddCommand(newResourceManagerConfigCommand())
	return cmd
}

func newResourceManagerConfigCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "config",
		Short: "config resource manager",
	}
	r.AddCommand(newConfigControllerCommand())
	return r
}

func newConfigControllerCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "controller",
		Short: "config controller",
	}
	r.AddCommand(newConfigControllerSetCommand())
	r.AddCommand(newConfigControllerShowCommand())
	return r
}

func newConfigControllerSetCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set controller config",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				cmd.Println(cmd.UsageString())
				return
			}

			var val any
			val, err := strconv.ParseFloat(args[1], 64)
			if err != nil {
				val = args[1]
			}
			data := map[string]any{args[0]: val}
			jsonData, err := json.Marshal(data)
			if err != nil {
				cmd.Println(err)
				return
			}
			resp, err := doRequest(cmd, fmt.Sprintf("%s/%s", resourceManagerPrefix, rmConfigController), http.MethodPost, http.Header{}, WithBody(bytes.NewBuffer(jsonData)))
			if err != nil {
				cmd.PrintErrln("Failed to set the config: ", err)
				return
			}
			cmd.Println(resp)
		},
	}
	return r
}

func newConfigControllerShowCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "show",
		Short: "show controller config",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 0 {
				cmd.Println(cmd.UsageString())
				return
			}
			resp, err := doRequest(cmd, fmt.Sprintf("%s/%s", resourceManagerPrefix, rmConfigController), http.MethodGet, http.Header{})
			if err != nil {
				cmd.Println(err)
				return
			}
			cmd.Println(resp)
		},
	}
	return r
}
