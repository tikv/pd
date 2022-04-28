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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const unsafePrefix = "pd/api/v1/admin/unsafe"

// NewUnsafeCommand returns the unsafe subcommand of rootCmd.
func NewUnsafeCommand() *cobra.Command {
	unsafeCmd := &cobra.Command{
		Use:   `unsafe [command]`,
		Short: "Unsafe operations",
	}
	unsafeCmd.AddCommand(NewRemoveFailedStoresCommand())
	return unsafeCmd
}

// NewRemoveFailedStoresCommand returns the unsafe remove failed stores command.
func NewRemoveFailedStoresCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-failed-stores <store_id1>[,<store_id2>,...]",
		Short: "Remove failed stores unsafely",
		Run:   removeFailedStoresCommandFunc,
	}
	cmd.PersistentFlags().Float64("timeout", 600, "timeout in seconds")
	cmd.AddCommand(NewRemoveFailedStoresShowCommand())
	cmd.AddCommand(NewRemoveFailedStoresHistoryCommand())
	return cmd
}

// NewRemoveFailedStoresShowCommand returns the unsafe remove failed stores show command.
func NewRemoveFailedStoresShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show the status of ongoing failed stores removal",
		Run:   removeFailedStoresShowCommandFunc,
	}
}

// NewRemoveFailedStoresHistoryCommand returns the unsafe remove failed stores history command.
func NewRemoveFailedStoresHistoryCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "history",
		Short: "Show the history of failed stores removal",
		Run:   removeFailedStoresHistoryCommandFunc,
	}
}

func removeFailedStoresCommandFunc(cmd *cobra.Command, args []string) {
	prefix := fmt.Sprintf("%s/remove-failed-stores", unsafePrefix)
	if len(args) < 1 {
		cmd.Usage()
		return
	}
	strStores := strings.Split(args[0], ",")
	var stores []uint64
	for _, strStore := range strStores {
		store, err := strconv.ParseUint(strStore, 10, 64)
		if err != nil {
			cmd.Usage()
			return
		}
		stores = append(stores, store)
	}
	postInput := map[string]interface{}{
		"stores": stores,
	}
	timeout, err := cmd.Flags().GetFloat64("timeout")
	if err != nil {
		cmd.Usage()
		return
	} else if timeout != 600 {
		postInput["timeout"] = timeout
	}
	postJSON(cmd, prefix, postInput)
}

func removeFailedStoresShowCommandFunc(cmd *cobra.Command, args []string) {
	var resp string
	var err error
	prefix := fmt.Sprintf("%s/remove-failed-stores/show", unsafePrefix)
	resp, err = doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(resp)
}

func removeFailedStoresHistoryCommandFunc(cmd *cobra.Command, args []string) {
	var resp string
	var err error
	prefix := fmt.Sprintf("%s/remove-failed-stores/history", unsafePrefix)
	resp, err = doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(resp)
}
