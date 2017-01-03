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
	"strconv"
	"time"

	"github.com/pingcap/pd/pkg/timeutil"
	"github.com/spf13/cobra"
)

var (
	storesPrefix      = "pd/api/v1/stores"
	storePrefix       = "pd/api/v1/store/%s"
	queryStoresPrefix = "pd/api/v1/stores?state=%s"
)

type storeStatus struct {
	StoreID uint64 `json:"store_id"`
	// Capacity for the store.
	Capacity ByteSize `json:"capacity"`
	// Available size for the store.
	Available ByteSize `json:"available"`
	// Total region count in this store.
	RegionCount uint32 `json:"region_count"`
	// Current sending snapshot count.
	SendingSnapCount uint32 `json:"sending_snap_count"`
	// Current receiving snapshot count.
	ReceivingSnapCount uint32 `json:"receiving_snap_count"`
	// When the store is started (unix timestamp in seconds).
	StartTime uint32 `json:"start_time"`
	// How many region is applying snapshot.
	ApplyingSnapCount uint32 `json:"applying_snap_count"`
	// If the store is busy
	IsBusy            bool              `json:"is_busy"`
	StartTS           time.Time         `json:"start_ts"`
	LastHeartbeatTS   time.Time         `json:"last_heartbeat_ts"`
	TotalRegionCount  int               `json:"total_region_count"`
	LeaderRegionCount int               `json:"leader_region_count"`
	Uptime            timeutil.Duration `json:"uptime"`
}

type store struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
	State   int32  `json:"state"`
}

type storeInfo struct {
	Store  *store       `json:"store"`
	Status *storeStatus `json:"status"`
	Scores []int        `json:"scores"`
}

type storesInfo struct {
	Count  int          `json:"count"`
	Stores []*storeInfo `json:"stores"`
}

// NewStoreCommand return a store subcommand of rootCmd
func NewStoreCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "store [delete|status] <store_id>",
		Short: "show the store status",
		Run:   showStoreCommandFunc,
	}
	s.AddCommand(NewDeleteStoreCommand())
	s.AddCommand(NewFilterStoreCommand())
	return s
}

// NewDeleteStoreCommand return a delete subcommand of storeCmd
func NewDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <store_id>",
		Short: "delete the store",
		Run:   deleteStoreCommandFunc,
	}
	return d
}

// NewFilterStoreCommand return a fileter subcommand of storeCmd
func NewFilterStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "status <up|offline|tombstone>",
		Short: "show the stores with a specifying status",
		Run:   filterStoreCommandFunc,
	}
	return d
}

func filterStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}
	var prefix string
	switch args[0] {
	case "up", "0":
		prefix = fmt.Sprintf(queryStoresPrefix, "0")
	case "offline", "1":
		prefix = fmt.Sprintf(queryStoresPrefix, "1")
	case "tombstone", "2":
		prefix = fmt.Sprintf(queryStoresPrefix, "2")
	default:
		fmt.Println(cmd.UsageString())
		return
	}
	stores := &storesInfo{}
	err := doRequestWithData(cmd, prefix, "GET", stores)
	if err != nil {
		fmt.Printf("Failed to get store: %s", err)
		return
	}
	res, err := json.MarshalIndent(stores, "", "  ")
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Println(string(res))
}

func showStoreCommandFunc(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = storesPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("store_id should be a number")
			return
		}
		prefix = fmt.Sprintf(storePrefix, args[0])
		r, err := doRequest(cmd, prefix, "GET")
		if err != nil {
			fmt.Printf("Failed to get store: %s", err)
		} else {
			fmt.Println(r)
		}
		return
	}
	stores := &storesInfo{}
	err := doRequestWithData(cmd, prefix, "GET", stores)
	if err != nil {
		fmt.Printf("Failed to get store: %s", err)
		return
	}
	res, err := json.MarshalIndent(stores, "", "  ")
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Println(string(res))
}

func deleteStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: store delete <store_id>")
		return
	}

	if _, err := strconv.Atoi(args[0]); err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storePrefix, args[0])
	_, err := doRequest(cmd, prefix, "DELETE")
	if err != nil {
		fmt.Printf("Failed to delete store %s: %s", args[0], err)
		return
	}
	fmt.Println("Success!")
}
