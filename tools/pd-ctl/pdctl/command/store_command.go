// Copyright 2016 TiKV Project Authors.
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
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/response"
)

var (
	storesPrefix       = "pd/api/v1/stores"
	storesLimitPrefix  = "pd/api/v1/stores/limit"
	storePrefix        = "pd/api/v1/store/%v"
	storeUpStatePrefix = "pd/api/v1/store/%v/state?state=Up"
	maxStoreLimit      = float64(200)
)

// NewStoreCommand return a stores subcommand of rootCmd
func NewStoreCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   `store [command] [flags]`,
		Short: "manipulate or query stores",
		Run:   showStoreCommandFunc,
	}
	s.AddCommand(NewDeleteStoreCommand())
	s.AddCommand(NewCancelDeleteStoreCommand())
	s.AddCommand(NewLabelStoreCommand())
	s.AddCommand(NewSetStoreWeightCommand())
	s.AddCommand(NewStoreLimitCommand())
	s.AddCommand(NewRemoveTombStoneCommand())
	s.AddCommand(NewStoreCheckCommand())
	s.Flags().String("jq", "", "jq query")
	s.Flags().StringSlice("state", nil, "state filter")
	return s
}

// NewDeleteStoreByAddrCommand returns a subcommand of delete
func NewDeleteStoreByAddrCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "addr <address>",
		Short: "delete store by its address",
		RunE:  deleteStoreCommandByAddrFunc,
	}
	return d
}

// NewDeleteStoreCommand return a delete subcommand of storeCmd
func NewDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <store_id>",
		Short: "delete the store",
		Run:   deleteStoreCommandFunc,
	}
	d.AddCommand(NewDeleteStoreByAddrCommand())
	return d
}

// NewCancelDeleteStoreByAddrCommand returns a subcommand of cancel delete
func NewCancelDeleteStoreByAddrCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "addr <address>",
		Short: "cancel delete store by its address",
		Run:   cancelDeleteStoreCommandByAddrFunc,
	}
	return d
}

// NewCancelDeleteStoreCommand return a cancel delete subcommand of storeCmd
func NewCancelDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "cancel-delete <store_id>",
		Short: "cancel delete the store",
		Run:   cancelDeleteStoreCommandFunc,
	}
	d.AddCommand(NewCancelDeleteStoreByAddrCommand())
	return d
}

// NewLabelStoreCommand returns a label subcommand of storeCmd.
func NewLabelStoreCommand() *cobra.Command {
	l := &cobra.Command{
		Use:  "label <store_id> <key>=<value> [<key>=<value>]",
		Args: cobra.MinimumNArgs(2),
		Example: `  # Add nonexistent label or update existent
	label <store_id> <key>=<value> [<key>=<value>]...
  # Delete one existent label via key
	label <store_id> <key> --delete
  # Rewrite all labels for the store
	label <store_id> <key>=<value> [<key>=<value>]... --rewrite`,
		Short: "Set a store's labels",
		Run:   labelStoreCommandFunc,
	}
	l.Flags().BoolP("force", "f", false, "[Deprecated] rewrite all labels for the store, same as rewrite")
	l.Flags().BoolP("rewrite", "r", false, "rewrite all labels for the store")
	l.Flags().BoolP("delete", "d", false, "delete the specified label")
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
	c := &cobra.Command{
		Use:   "limit [<store_id>|<all> [<key> <value>]... <limit> <type>]",
		Short: "show or set a store's rate limit",
		Long:  "show or set a store's rate limit, <type> can be 'add-peer'(default) or 'remove-peer'",
		Run:   storeLimitCommandFunc,
	}
	return c
}

// NewStoreCheckCommand return a check subcommand of storeCmd
func NewStoreCheckCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "check [up|offline|tombstone|disconnected|down]",
		Short: "Check all the stores with specified status",
		Run:   storeCheckCommandFunc,
	}
	return d
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
	s.Flags().StringSlice("state", nil, "state filter")
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
	sc.AddCommand(NewShowAllStoresLimitCommand())
	return sc
}

// NewShowAllStoresLimitCommand return a show limit subcommand of show command
func NewShowAllStoresLimitCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:        "limit <type>",
		Short:      "show all stores' limit, <type> can be 'add-peer'(default) or 'remove-peer'",
		Deprecated: "use store limit instead",
		Run:        showAllStoresLimitCommandFunc,
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
		Use:        "limit <rate> <type>",
		Short:      "set all store's rate limit",
		Long:       "set all store's rate limit, <type> can be 'add-peer'(default) or 'remove-peer'",
		Deprecated: "use store limit all <rate> instead",
		Run:        setAllLimitCommandFunc,
	}
}

func convertToStoreInfo(content string) string {
	store := &response.StoreInfo{}
	err := json.Unmarshal([]byte(content), store)
	if err != nil {
		return content
	}
	// In StoreInfo, users only need to pay attention to StateName.
	// and State will be omitted when its value equals 0 from kvproto metapb
	store.Store.State = 0
	byteArr, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return content
	}
	return string(byteArr)
}

func convertToStoresInfo(content string) string {
	stores := &response.StoresInfo{}
	err := json.Unmarshal([]byte(content), stores)
	if err != nil {
		return content
	}
	// In StoreInfo, users only need to pay attention to StateName.
	// and both State and NodeState will be omitted when its value equals 0 from kvproto metapb
	for _, store := range stores.Stores {
		store.Store.State = 0
		store.Store.NodeState = 0
	}
	byteArr, err := json.MarshalIndent(stores, "", "  ")
	if err != nil {
		return content
	}
	return string(byteArr)
}

func showStoreCommandFunc(cmd *cobra.Command, args []string) {
	prefix := storesPrefix
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	cFunc := convertToStoresInfo
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			cmd.Println("store_id should be a number")
			return
		}
		prefix = fmt.Sprintf(storePrefix, args[0])
		cFunc = convertToStoreInfo
	} else {
		flags := cmd.Flags()
		states, err := flags.GetStringSlice("state")
		if err != nil {
			cmd.Printf("Failed to get state: %s\n", err)
		}
		stateValues := make([]string, 0, len(states))
		for _, state := range states {
			stateValue, ok := metapb.StoreState_value[state]
			if !ok {
				cmd.Println("Unknown state: " + state)
				return
			}
			stateValues = append(stateValues, fmt.Sprintf("state=%v", stateValue))
		}
		if len(stateValues) != 0 {
			prefix = fmt.Sprintf("%v?%v", storesPrefix, strings.Join(stateValues, "&"))
		}
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}
	r = cFunc(r)
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
	_, err := doRequest(cmd, prefix, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Printf("Failed to delete store %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func deleteStoreCommandByAddrFunc(cmd *cobra.Command, args []string) error {
	id := getStoreID(cmd, args, false)
	if id == -1 {
		return fmt.Errorf("address not found: %s", args[0])
	}
	// delete store by its ID
	prefix := fmt.Sprintf(storePrefix, id)
	_, err := doRequest(cmd, prefix, http.MethodDelete, http.Header{})
	if err != nil {
		return fmt.Errorf("failed to delete store %s: %s", args[0], err)
	}
	cmd.Println("Success!")
	return nil
}

func cancelDeleteStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		cmd.Println("store_id should be a number")
		return
	}

	prefix := fmt.Sprintf(storePrefix, args[0])
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}

	storeInfo := struct {
		Store struct {
			State metapb.StoreState `json:"state"`
		} `json:"store"`
	}{}
	if err = json.Unmarshal([]byte(r), &storeInfo); err != nil {
		cmd.Printf("Failed to parse store info: %s\n", err)
		return
	}

	if storeInfo.Store.State != metapb.StoreState_Offline {
		cmd.Printf("store %v is not offline\n", args[0])
		return
	}

	prefix = fmt.Sprintf(storeUpStatePrefix, args[0])
	_, err = doRequest(cmd, prefix, http.MethodPost, http.Header{})
	if err != nil {
		cmd.Printf("Failed to cancel delete store %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func cancelDeleteStoreCommandByAddrFunc(cmd *cobra.Command, args []string) {
	id := getStoreID(cmd, args, true)
	if id == -1 {
		cmd.Printf("address not found: %s\n", args[0])
		return
	}
	if id == 0 {
		return
	}
	// cancel delete store by its ID
	prefix := fmt.Sprintf(storeUpStatePrefix, id)
	_, err := doRequest(cmd, prefix, http.MethodPost, http.Header{})
	if err != nil {
		cmd.Printf("Failed to cancel delete store %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func getStoreID(cmd *cobra.Command, args []string, isCancel bool) (id int) {
	id = -1
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	addr := args[0]

	// fetch all the stores
	r, err := doRequest(cmd, storesPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}

	storeInfo := struct {
		Stores []struct {
			Store struct {
				ID      int               `json:"id"`
				Address string            `json:"address"`
				State   metapb.StoreState `json:"state"`
			} `json:"store"`
		} `json:"stores"`
	}{}
	if err = json.Unmarshal([]byte(r), &storeInfo); err != nil {
		cmd.Printf("Failed to parse store info: %s\n", err)
		return
	}

	// filter by the addr
	for _, store := range storeInfo.Stores {
		if store.Store.Address == addr {
			if isCancel && store.Store.State != metapb.StoreState_Offline {
				cmd.Printf("store is not offline: %s\n", addr)
				return
			}
			id = store.Store.ID
			break
		}
	}

	return
}

func labelStoreCommandFunc(cmd *cobra.Command, args []string) {
	// The least args' numbers is 1, which means users can set empty key and value
	// In this way, if force flag is set then it means clear all labels,
	// if force flag isn't set then it means do nothing
	isDelete, err := cmd.Flags().GetBool("delete")
	if err != nil {
		cmd.Println("can't parse parameters", err)
		return
	}
	if len(args) <= 1 {
		cmd.Usage()
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		cmd.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "label"), args[0])
	var toDeletedLabel string
	if isDelete {
		if len(args) != 2 {
			cmd.PrintErrln("Failed: not allow to delete multiple labels at a time")
			return
		}
		toDeletedLabel = args[1]
		b, _ := json.Marshal(toDeletedLabel)
		res, err := doRequest(cmd, prefix, http.MethodDelete, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(b)))
		if err != nil {
			cmd.Printf("Failed! %s\n", err)
			return
		}
		cmd.Printf("Success! %s\n", res)
		return
	}

	labels := make(map[string]any)
	// useEqual is used to compatible the old way
	// TODO: remove old way
	useEqual := true
	for i := 1; i < len(args); {
		if i == 1 {
			sliceArg := strings.Split(args[i], "=")
			if len(sliceArg) != 2 {
				useEqual = false
			}
		}
		if useEqual { // new way
			sliceArg := strings.Split(args[i], "=")
			if len(sliceArg) != 2 {
				cmd.PrintErrln("Failed: invalid arguments, expect <key>=<value>")
				return
			}
			key, value := strings.TrimSpace(sliceArg[0]), strings.TrimSpace(sliceArg[1])
			labels[key] = value
			i++
		} else { // old way
			if i+1 >= len(args) {
				cmd.PrintErrln("Failed: invalid arguments, expect <key> <value> [key value]...")
				return
			}
			labels[args[i]] = args[i+1]
			i += 2
		}
	}
	if force, _ := cmd.Flags().GetBool("force"); force {
		prefix += "?force=true"
	} else if rewrite, _ := cmd.Flags().GetBool("rewrite"); rewrite {
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
	postJSON(cmd, prefix, map[string]any{
		"leader": leader,
		"region": region,
	})
}

func storeLimitCommandFunc(cmd *cobra.Command, args []string) {
	argsCount := len(args)
	if argsCount <= 1 {
		prefix := storesLimitPrefix
		if argsCount == 1 {
			prefix += fmt.Sprintf("?type=%s", args[0])
		}
		r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
		if err != nil {
			cmd.Printf("Failed to get store limit: %s\n", err)
			return
		}
		cmd.Println(r)
	} else if argsCount <= 3 {
		rate, err := strconv.ParseFloat(args[1], 64)
		if err != nil || rate <= 0 {
			cmd.Println("rate should be a number that > 0.")
			return
		}
		// if the store id is "all", set limits for all stores
		var prefix string
		if args[0] == "all" {
			prefix = storesLimitPrefix
			if rate > maxStoreLimit {
				cmd.Printf("rate should less than %f for all\n", maxStoreLimit)
				return
			}
		} else {
			prefix = fmt.Sprintf(path.Join(storePrefix, "limit"), args[0])
		}
		postInput := map[string]any{
			"rate": rate,
		}
		if argsCount == 3 {
			postInput["type"] = args[2]
		}
		postJSON(cmd, prefix, postInput)
	} else {
		if args[0] != "all" {
			cmd.Println("Labels are an option of set all stores limit.")
		} else {
			postInput := map[string]any{}
			prefix := storesLimitPrefix
			ratePos := argsCount - 1
			if argsCount%2 == 1 {
				postInput["type"] = args[argsCount-1]
				ratePos = argsCount - 2
			}
			rate, err := strconv.ParseFloat(args[ratePos], 64)
			if err != nil || rate <= 0 {
				cmd.Println("rate should be a number that > 0.")
				return
			}
			if rate > maxStoreLimit {
				cmd.Printf("rate should less than %f for all\n", maxStoreLimit)
				return
			}
			postInput["rate"] = rate
			labels := make(map[string]any)
			for i := 1; i < ratePos; i += 2 {
				labels[args[i]] = args[i+1]
			}
			postInput["labels"] = labels
			postJSON(cmd, prefix, postInput)
		}
	}
}

func storeCheckCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	caser := cases.Title(language.Und)
	state := caser.String(strings.ToLower(args[0]))
	prefix := fmt.Sprintf("%s/check?state=%s", storesPrefix, state)
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get store: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showStoresCommandFunc(cmd *cobra.Command, _ []string) {
	prefix := storesPrefix
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
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

func showAllStoresLimitCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	prefix := storesLimitPrefix
	if len(args) == 1 {
		prefix += fmt.Sprintf("?type=%s", args[0])
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get all stores' limit: %s\n", err)
		return
	}
	cmd.Println(r)
}

func removeTombStoneCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}
	prefix := path.Join(storesPrefix, "remove-tombstone")
	_, err := doRequest(cmd, prefix, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Printf("Failed to remove tombstone store %s \n", err)
		return
	}
	cmd.Println("Success!")
}

func setAllLimitCommandFunc(cmd *cobra.Command, args []string) {
	argsCount := len(args)
	if argsCount != 1 && argsCount != 2 {
		cmd.Usage()
		return
	}
	rate, err := strconv.ParseFloat(args[0], 64)
	if err != nil || rate <= 0 {
		cmd.Println("rate should be a number that > 0.")
		return
	}
	prefix := storesLimitPrefix
	input := map[string]any{
		"rate": rate,
	}
	if len(args) == 2 {
		input["type"] = args[1]
	}
	postJSON(cmd, prefix, input)
}
