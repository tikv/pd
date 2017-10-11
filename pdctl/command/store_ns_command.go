// Copyright 2017 PingCAP, Inc.
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
	"strconv"

	"github.com/spf13/cobra"
)

const storeNsPrefix = "pd/api/v1/store_ns/%s"

// NewStoreNsCommand return a store_ns subcommand of rootCmd
func NewStoreNsCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "store_ns [set|rm] <store_id> <namespace>",
		Short: "show the store status",
	}
	s.AddCommand(NewSetNamespaceStoreCommand())
	s.AddCommand(NewRemoveNamespaceStoreCommand())
	return s
}

// NewSetNamespaceStoreCommand returns a set subcommand of storeNsCmd.
func NewSetNamespaceStoreCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "set <store_id> <namespace>",
		Short: "set namespace to store",
		Run:   setNamespaceStoreCommandFunc,
	}

	return n
}

// NewRemoveNamespaceStoreCommand returns a rm subcommand of storeNsCmd.
func NewRemoveNamespaceStoreCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "rm <store_id> <namespace>",
		Short: "remove namespace from store",
		Run:   removeNamespaceStoreCommandFunc,
	}

	return n
}

func setNamespaceStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: store_ns set <store_id> <namespace>")
		return
	}
	_, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storeNsPrefix, args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"namespace": args[1],
		"action":    "add",
	})
}

func removeNamespaceStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: store_ns rm <store_id> <namespace>")
		return
	}
	_, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storeNsPrefix, args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"namespace": args[1],
		"action":    "remove",
	})
}
