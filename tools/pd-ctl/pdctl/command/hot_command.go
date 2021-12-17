// Copyright 2017 TiKV Project Authors.
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
	"net/http"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

const (
	hotReadRegionsPrefix  = "pd/api/v1/hotspot/regions/read"
	hotWriteRegionsPrefix = "pd/api/v1/hotspot/regions/write"
	hotStoresPrefix       = "pd/api/v1/hotspot/stores"
)

// NewHotSpotCommand return a hot subcommand of rootCmd
func NewHotSpotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hot",
		Short: "show the hotspot status of the cluster",
	}
	cmd.AddCommand(NewHotWriteRegionCommand())
	cmd.AddCommand(NewHotReadRegionCommand())
	cmd.AddCommand(NewHotStoreCommand())
	return cmd
}

// NewHotWriteRegionCommand return a hot regions subcommand of hotSpotCmd
func NewHotWriteRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write [<store id> ...]",
		Short: "show the hot write regions",
		Run:   showHotWriteRegionsCommandFunc,
	}
	return cmd
}

func showHotWriteRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(hotWriteRegionsPrefix, args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get write hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotReadRegionCommand return a hot read regions subcommand of hotSpotCmd
func NewHotReadRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read [<store id> ...]",
		Short: "show the hot read regions",
		Run:   showHotReadRegionsCommandFunc,
	}
	return cmd
}

func showHotReadRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(hotReadRegionsPrefix, args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get read hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotStoreCommand return a hot stores subcommand of hotSpotCmd
func NewHotStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "show the hot stores",
		Run:   showHotStoresCommandFunc,
	}
	return cmd
}

func showHotStoresCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, hotStoresPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get store hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

func parseOptionalArgs(prefix string, args []string) (string, error) {
	argsLen := len(args)
	if argsLen > 0 {
		prefix += "?"
	}
	for i, arg := range args {
		if _, err := strconv.Atoi(arg); err != nil {
			return "", errors.Errorf("store id should be a number, but got %s", arg)
		}
		if i != argsLen {
			prefix = prefix + "store_id=" + arg + "&"
		} else {
			prefix = prefix + "store_id=" + arg
		}
	}
	return prefix, nil
}
