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
	"strconv"

	"github.com/spf13/cobra"
)

var (
	storesPrefix = "pd/api/v1/stores"
	storePrefix  = "pd/api/v1/store/%s"
)

// NewStoreCommand return a store subcommand of rootCmd
func NewStoreCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "store [delete] <store_id>",
		Short: "show the store status",
		Run:   showStoreCommandF,
	}
	s.AddCommand(NewDeleteStoreCommand())
	return s
}

// NewDeleteStoreCommand return a  delete subcommand of storeCmd
func NewDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <store_id>",
		Short: "delete the store",
		Run:   deleteStoreCommandF,
	}
	return d
}

func showStoreCommandF(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = storesPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("store_id should be a number")
			return
		}
		prefix = fmt.Sprintf(storePrefix, args[0])
	}
	url := getAddressFromCmd(cmd, prefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get store:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		printResponseError(r)
		return
	}

	io.Copy(os.Stdout, r.Body)
}

func deleteStoreCommandF(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: store delete <store_id>")
		return
	}
	cli := &http.Client{}
	if _, err := strconv.Atoi(args[0]); err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storePrefix, args[0])
	url := getAddressFromCmd(cmd, prefix)
	r, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("Failed to delete store %s: [%s]\n", args[0], err)
		return
	}
	reps, err := cli.Do(r)
	if err != nil {
		fmt.Printf("Failed to delete store %s: [%s]\n", args[0], err)
		return
	}

	defer reps.Body.Close()
	if reps.StatusCode == http.StatusOK {
		fmt.Println("Success!")
	} else {
		printResponseError(reps)
	}
}
