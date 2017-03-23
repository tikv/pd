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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	configPrefix    = "pd/api/v1/config"
	schedulePrefix  = "pd/api/v1/config/schedule"
	replicatePrefix = "pd/api/v1/config/replicate"
)

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	conf.AddCommand(NewScheduleConfigCommand())
	conf.AddCommand(NewReplicationConfigCommand())
	return conf
}

// NewShowConfigCommand return a show subcommand of configCmd
func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show",
		Short: "show config of PD",
		Run:   showConfigCommandFunc,
	}
	sc.AddCommand(NewShowAllConfigCommand())
	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of PD",
		Run:   showAllConfigCommandFunc,
	}
	return sc
}

// NewScheduleConfigCommand return a set subcommand of configCmd
func NewScheduleConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "schedule <option> <value>",
		Short: "set the schedule option with value",
		Run:   setScheduleConfigCommandFunc,
	}
	return sc
}

// NewReplicationConfigCommand return a set subcommand of configCmd
func NewReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication <option> <value>",
		Short: "set the replication option with value",
		Run:   setReplicationConfigCommandFunc,
	}
	return sc
}

func showConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s", err)
		return
	}
	fmt.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s", err)
		return
	}
	fmt.Println(r)
}

func postDataWithPath(cmd *cobra.Command, args []string, path string) error {
	opt, val := args[0], args[1]
	var value interface{}
	data := make(map[string]interface{})
	value, err := strconv.ParseFloat(val, 64)
	if err != nil {
		value = val
	}
	data[opt] = value
	reqData, err := json.Marshal(data)
	req, err := getRequest(cmd, path, http.MethodPost, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		return err
	}
	_, err = dail(req)
	if err != nil {
		return err
	}
	return nil
}

func setScheduleConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}
	err := postDataWithPath(cmd, args, schedulePrefix)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	fmt.Println("Success!")
}

func setReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}
	err := postDataWithPath(cmd, args, replicatePrefix)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	fmt.Println("Success!")
}
