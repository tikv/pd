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
		Use:   `store [command] [flags]`,
		Short: "manipulate or query stores",
		Run:   showStoreCommandFunc,
	}
	s.AddCommand(NewDeleteStoreCommand())
	s.AddCommand(NewLabelStoreCommand())
	s.AddCommand(NewSetStoreWeightCommand())
	s.AddCommand(NewStoreLimitCommand())
	s.AddCommand(NewRemoveTombStoneCommand())
	s.AddCommand(NewStoreLimitSceneCommand())
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
		Use:   "label <store_id> <key> <value> [<key> <value>]...",
		Short: "set a store's label value",
		Run:   labelStoreCommandFunc,
	}
	l.Flags().BoolP("force", "f", false, "overwrite the label forcibly")
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

// NewStoreLimitCommand returns a limit subcommand of storeCmd.
func NewStoreLimitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "limit [<store_id>|<all> <rate>]",
		Short: "set a store's rate limit",
		Run:   storeLimitCommandFunc,
	}
}

// NewStoresCommand returns a store subcommand of rootCmd
func NewStoresCommand() *cobra.Command {
	s := &cobra.Command{
		Use:        `stores [command] [flags]`,
		Short:      "store status",
		Deprecated: "use store command instead",
	}
	s.AddCommand(NewRemoveTombStoneCommandDeprecated())
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

// NewRemoveTombStoneCommandDeprecated returns a tombstone subcommand of storesCmd.
func NewRemoveTombStoneCommandDeprecated() *cobra.Command {
	return &cobra.Command{
		Use:        "remove-tombstone",
		Short:      "remove tombstone record if only safe",
		Deprecated: "use store remove-tombstone instead",
		Run:        removeTombStoneCommandFunc,
	}
}

// NewShowStoresCommand returns a show subcommand of storesCmd.
func NewShowStoresCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:        "show [limit]",
		Short:      "show the stores",
		Run:        showStoresCommandFunc,
		Deprecated: "use store [limit] instead",
	}
	sc.AddCommand(NewShowAllLimitCommand())
	return sc
}

// NewShowAllLimitCommand return a show limit subcommand of show command
func NewShowAllLimitCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:        "limit",
		Short:      "show all stores' limit",
		Deprecated: "use store limit instead",
		Run:        showAllLimitCommandFunc,
	}
	return sc
}

// NewSetStoresCommand returns a set subcommand of storesCmd.
func NewSetStoresCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:        "set [limit]",
		Short:      "set stores",
		Deprecated: "use store command instead",
	}
	sc.AddCommand(NewSetAllLimitCommand())
	return sc
}

// NewSetAllLimitCommand returns a set limit subcommand of set command.
func NewSetAllLimitCommand() *cobra.Command {
	return &cobra.Command{
		Use:        "limit <rate>",
		Short:      "set all store's rate limit",
		Deprecated: "use store limit all <rate> instead",
		Run:        setAllLimitCommandFunc,
	}
}

// NewStoreLimitSceneCommand returns a limit-scene command for store command
func NewStoreLimitSceneCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "limit-scene [<scene> <rate>]",
		Short: "show or set the limit value for a scene",
		Run:   storeLimitSceneCommandFunc,
	}
}

func storeLimitSceneCommandFunc(cmd *cobra.Command, args []string) {
	var resp string
	var err error
	prefix := fmt.Sprintf("%s/limit/scene", storesPrefix)

	switch len(args) {
	case 0:
		// show all limit values
		resp, err = doRequest(cmd, prefix, http.MethodGet)
		if err != nil {
			cmd.Println(err)
			return
		}
		cmd.Println(resp)
	case 1:
		cmd.Usage()
		return
	case 2:
		// set limit value for a scene
		scene := args[0]
		if scene != "idle" &&
			scene != "low" &&
			scene != "normal" &&
			scene != "high" {
			cmd.Println("invalid scene")
			return
		}

		rate, err := strconv.Atoi(args[1])
		if err != nil {
			cmd.Println(err)
			return
		}
		postJSON(cmd, prefix, map[string]interface{}{scene: rate})
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
	// The least args' numbers is 1, which means users can set empty key and value
	// In this way, if force flag is set then it means clear all labels,
	// if force flag isn't set then it means do nothing
	if len(args) < 1 || len(args)%2 != 1 {
		cmd.Usage()
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		cmd.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "label"), args[0])
	labels := make(map[string]interface{})
	for i := 1; i < len(args); i += 2 {
		labels[args[i]] = args[i+1]
	}
	if force, _ := cmd.Flags().GetBool("force"); force {
		prefix += "?force=true"
	}
	postJSON(cmd, prefix, labels)
}

func setStoreWeightCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Usage()
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

func storeLimitCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		showAllLimitCommandFunc(cmd, args)
		return
	}
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	rate, err := strconv.ParseFloat(args[1], 64)
	if err != nil || rate < 0 {
		cmd.Println("rate should be a number that >= 0.")
		return
	}
	// if the storeid is "all", set limits for all stores
	if args[0] == "all" {
		prefix := path.Join(storesPrefix, "limit")
		postJSON(cmd, prefix, map[string]interface{}{
			"rate": rate,
		})
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
		cmd.Usage()
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
