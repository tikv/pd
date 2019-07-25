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
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	storesPrefix = "pd/api/v1/stores"
	storePrefix  = "pd/api/v1/store/%v"
)

// NewStoreCommand return a stores subcommand of rootCmd
func NewStoreCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   `store [delete|label|weight|limit] <store_id> [--jq="<query string>"]`,
		Short: "show the store status",
		Run:   showStoreCommandFunc,
	}
	s.AddCommand(NewDeleteStoreCommand())
	s.AddCommand(NewLabelStoreCommand())
	s.AddCommand(NewSetStoreWeightCommand())
	s.AddCommand(NewSetStoreLimitCommand())
	s.Flags().String("jq", "", "jq query")
	return s
}

// NewDeleteStoreByAddrCommand returns a subcommand of delete
func NewDeleteStoreByAddrCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "addr <address>",
		Short: "delete store by its address",
		Run:   deleteStoreCommandByAddrFunc,
	}
	return d
}

// NewDeleteStoreCommand return a  delete subcommand of storeCmd
func NewDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <store_id>",
		Short: "delete the store",
		Run:   deleteStoreCommandFunc,
	}
	d.AddCommand(NewDeleteStoreByAddrCommand())
	return d
}

// NewLabelStoreCommand returns a label subcommand of storeCmd.
func NewLabelStoreCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "label <store_id> <key> <value>",
		Short: "set a store's label value",
		Run:   labelStoreCommandFunc,
	}
	return l
}

// NewSetStoreWeightCommand returns a weight subcommand of storeCmd.
func NewSetStoreWeightCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "weight <store_id> <leader_weight> <region_weight>",
		Short: "set a store's leader and region balance weight",
		Run:   setStoreWeightCommandFunc,
	}
}

// NewSetStoreLimitCommand returns a limit subcommand of storeCmd.
func NewSetStoreLimitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "limit <store_id> <rate>",
		Short: "set a store's rate limit",
		Run:   setStoreLimitCommandFunc,
	}
}

// NewStoresCommand returns a store subcommand of rootCmd
func NewStoresCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   `stores [subcommand] [--jq="<query string>"]`,
		Short: "store status",
	}
	s.AddCommand(NewRemoveTombStoneCommand())
	s.AddCommand(NewSetStoresCommand())
	s.AddCommand(NewShowStoresCommand())
	s.Flags().String("jq", "", "jq query")
	return s
}

// NewRemoveTombStoneCommand returns a tombstone subcommand of storesCmd.
func NewRemoveTombStoneCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "remove-tombstone",
		Short: "remove tombstone record if only safe",
		Run:   removeTombStoneCommandFunc,
	}
}

// NewShowStoresCommand returns a show subcommand of storesCmd.
func NewShowStoresCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show [limit]",
		Short: "show the stores",
		Run:   showStoresCommandFunc,
	}
	sc.AddCommand(NewShowAllLimitCommand())
	return sc
}

// NewShowAllLimitCommand return a show limit subcommand of show command
func NewShowAllLimitCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "limit",
		Short: "show all stores' limit",
		Run:   showAllLimitCommandFunc,
	}
	return sc
}

// NewSetStoresCommand returns a set subcommand of storesCmd.
func NewSetStoresCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set [limit]",
		Short: "set stores",
	}
	sc.AddCommand(NewSetAllLimitCommand())
	return sc
}

// NewSetAllLimitCommand returns a set limit subcommand of set command.
func NewSetAllLimitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "limit <rate>",
		Short: "set all store's rate limit",
		Run:   setAllLimitCommandFunc,
	}
}

func showStoreCommandFunc(cmd *cobra.Command, args []string) {
	prefix := storesPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			cmd.Println("store_id should be a number")
			return
		}
		prefix = fmt.Sprintf(storePrefix, args[0])
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}
	if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
		printWithJQFilter(r, flag.Value.String())
		return
	}
	cmd.Println(r)
}

func deleteStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		cmd.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storePrefix, args[0])
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to delete store %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func deleteStoreCommandByAddrFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	addr := args[0]

	// fetch all the stores
	r, err := doRequest(cmd, storesPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}

	storeInfo := struct {
		Stores []struct {
			Store struct {
				ID      int    `json:"id"`
				Address string `json:"address"`
			} `json:"store"`
		} `json:"stores"`
	}{}
	if err = json.Unmarshal([]byte(r), &storeInfo); err != nil {
		cmd.Printf("Failed to parse store info: %s\n", err)
		return
	}

	// filter by the addr
	id := -1
	for _, store := range storeInfo.Stores {
		if store.Store.Address == addr {
			id = store.Store.ID
			break
		}
	}

	if id == -1 {
		cmd.Printf("address not found: %s\n", addr)
		return
	}

	// delete store by its ID
	prefix := fmt.Sprintf(storePrefix, id)
	_, err = doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to delete store %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func labelStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println("Usage: store label <store_id> <key> <value>")
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		cmd.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "label"), args[0])
	postJSON(cmd, prefix, map[string]interface{}{args[1]: args[2]})
}

func setStoreWeightCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println("Usage: store weight <store_id> <leader_weight> <region_weight>")
		return
	}
	leader, err := strconv.ParseFloat(args[1], 64)
	if err != nil || leader < 0 {
		cmd.Println("leader_weight should be a number that >= 0.")
		return
	}
	region, err := strconv.ParseFloat(args[2], 64)
	if err != nil || region < 0 {
		cmd.Println("region_weight should be a number that >= 0")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "weight"), args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"leader": leader,
		"region": region,
	})
}

func setStoreLimitCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println("Usage: store limit <store_id> <rate>")
		return
	}
	rate, err := strconv.ParseFloat(args[1], 64)
	if err != nil || rate < 0 {
		cmd.Println("rate should be a number that >= 0.")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "limit"), args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"rate": rate,
	})
}

func showStoresCommandFunc(cmd *cobra.Command, args []string) {
	prefix := storesPrefix
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}
	if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
		printWithJQFilter(r, flag.Value.String())
		return
	}
	cmd.Println(r)
}

func showAllLimitCommandFunc(cmd *cobra.Command, args []string) {
	prefix := path.Join(storesPrefix, "limit")
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get all limit: %s\n", err)
		return
	}
	cmd.Println(r)
}

func removeTombStoneCommandFunc(cmd *cobra.Command, args []string) {
	prefix := path.Join(storesPrefix, "remove-tombstone")
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to remove tombstone store %s \n", err)
		return
	}
	cmd.Println("Success!")
}

func setAllLimitCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println("Usage: stores set limit <rate>")
		return
	}
	rate, err := strconv.ParseFloat(args[0], 64)
	if err != nil || rate < 0 {
		cmd.Println("rate should be a number that >= 0.")
		return
	}
	prefix := path.Join(storesPrefix, "limit")
	postJSON(cmd, prefix, map[string]interface{}{
		"rate": rate,
	})
}
