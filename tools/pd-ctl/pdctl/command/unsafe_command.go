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
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const unsafePrefix = "pd/api/v1/admin/unsafe"

var maxPlanExecutionTimeoutSeconds = float64(time.Duration(math.MaxInt64/2) / time.Second)

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
		Use:   "remove-failed-stores [<store_id1>,<store_id2>,...]",
		Short: "Remove failed stores unsafely",
		Run:   removeFailedStoresCommandFunc,
	}
	cmd.PersistentFlags().Float64("timeout", 300, "timeout in seconds")
	cmd.PersistentFlags().Float64("plan-execution-timeout", 60, "plan execution timeout in seconds before dispatching the plan again")
	cmd.PersistentFlags().Bool("disable-paranoid-check", false, "disable the empty region overlap paranoid check")
	cmd.PersistentFlags().Bool("auto-detect", false, `detect failed stores automatically without needing to pass failed store ids, and all stores not in PD stores list are regarded as failed; 
	Note: DO NOT RECOMMEND to use this flag for general use, it's used only for case that PD doesn't have the store information of failed stores after pd-recover;
	Note: Do it with caution to make sure all live stores's heartbeats has been reported PD already, otherwise it may regarded some stores as failed mistakenly.`)
	cmd.AddCommand(NewRemoveFailedStoresShowCommand())
	cmd.AddCommand(NewRemoveFailedStoresAbortCommand())
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

// NewRemoveFailedStoresAbortCommand returns the unsafe remove failed stores abort command.
func NewRemoveFailedStoresAbortCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "abort",
		Short: "Abort the current failed stores removal",
		Run:   removeFailedStoresAbortCommandFunc,
	}
}

func removeFailedStoresCommandFunc(cmd *cobra.Command, args []string) {
	prefix := fmt.Sprintf("%s/remove-failed-stores", unsafePrefix)
	postInput := make(map[string]any, 3)

	autoDetect, err := cmd.Flags().GetBool("auto-detect")
	if err != nil {
		cmd.Println(err)
		return
	}

	if autoDetect {
		if len(args) > 0 {
			cmd.Println("The flag `auto-detect` is set, no need to specify failed store ids")
			return
		}
		postInput["auto-detect"] = autoDetect
	} else {
		if len(args) < 1 {
			cmd.Println("Failed store ids are not specified")
			cmd.Usage()
			return
		}

		strStores := strings.Split(args[0], ",")
		var stores []uint64
		for _, strStore := range strStores {
			store, err := strconv.ParseUint(strStore, 10, 64)
			if err != nil {
				cmd.Println(err)
				return
			}
			stores = append(stores, store)
		}
		postInput["stores"] = stores
	}

	timeout, err := cmd.Flags().GetFloat64("timeout")
	if err != nil {
		cmd.Println(err)
		return
	} else if timeout != 300 {
		postInput["timeout"] = timeout
	}

	planExecutionTimeout, err := cmd.Flags().GetFloat64("plan-execution-timeout")
	if err != nil {
		cmd.Println(err)
		return
	} else if planExecutionTimeout != 60 {
		if err := validatePlanExecutionTimeoutSeconds(planExecutionTimeout); err != nil {
			cmd.Println(err)
			return
		}
		postInput["plan-execution-timeout"] = planExecutionTimeout
	}

	disableParanoidCheck, err := cmd.Flags().GetBool("disable-paranoid-check")
	if err != nil {
		cmd.Println(err)
		return
	} else if disableParanoidCheck {
		postInput["disable-paranoid-check"] = disableParanoidCheck
	}

	postJSON(cmd, prefix, postInput)
}

func validatePlanExecutionTimeoutSeconds(timeout float64) error {
	if timeout <= 0 || timeout != math.Trunc(timeout) || timeout > maxPlanExecutionTimeoutSeconds {
		return fmt.Errorf("plan-execution-timeout must be a positive integer number of seconds no greater than %.0f", maxPlanExecutionTimeoutSeconds)
	}
	return nil
}

func removeFailedStoresShowCommandFunc(cmd *cobra.Command, _ []string) {
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

func removeFailedStoresAbortCommandFunc(cmd *cobra.Command, _ []string) {
	prefix := fmt.Sprintf("%s/remove-failed-stores/abort", unsafePrefix)
	postJSON(cmd, prefix, nil)
}
