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
	"net/http"
	"path"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	configPrefix         = "pd/api/v1/config"
	schedulePrefix       = "pd/api/v1/config/schedule"
	replicationPrefix    = "pd/api/v1/config/replicate"
	labelPropertyPrefix  = "pd/api/v1/config/label-property"
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
)

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	conf.AddCommand(NewSetConfigCommand())
	conf.AddCommand(NewDeleteConfigCommand())
	return conf
}

// NewShowConfigCommand return a show subcommand of configCmd
func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show [replication|label-property|all]",
		Short: "show replication and schedule config of PD",
		Run:   showConfigCommandFunc,
	}
	sc.AddCommand(NewShowAllConfigCommand())
	sc.AddCommand(NewShowScheduleConfigCommand())
	sc.AddCommand(NewShowReplicationConfigCommand())
	sc.AddCommand(NewShowLabelPropertyCommand())
	sc.AddCommand(NewShowClusterVersionCommand())
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

// NewShowScheduleConfigCommand return a show all subcommand of show subcommand
func NewShowScheduleConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "schedule",
		Short: "show schedule config of PD",
		Run:   showScheduleConfigCommandFunc,
	}
	return sc
}

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication",
		Short: "show replication config of PD",
		Run:   showReplicationConfigCommandFunc,
	}
	return sc
}

// NewShowLabelPropertyCommand returns a show label property subcommand of show subcommand.
func NewShowLabelPropertyCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "label-property",
		Short: "show label property config",
		Run:   showLabelPropertyConfigCommandFunc,
	}
	return sc
}

// NewShowClusterVersionCommand returns a cluster version subcommand of show subcommand.
func NewShowClusterVersionCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "cluster-version",
		Short: "show the cluster version",
		Run:   showClusterVersionCommandFunc,
	}
	return sc
}

// NewSetConfigCommand return a set subcommand of configCmd
func NewSetConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set <option> <value>, set label-property <type> <key> <value>, set cluster-version <version>",
		Short: "set the option with value",
		Run:   setConfigCommandFunc,
	}
	sc.AddCommand(NewSetLabelPropertyCommand())
	sc.AddCommand(NewSetClusterVersionCommand())
	return sc
}

// NewSetLabelPropertyCommand creates a set subcommand of set subcommand
func NewSetLabelPropertyCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "label-property <type> <key> <value>",
		Short: "set a label property config item",
		Run:   setLabelPropertyConfigCommandFunc,
	}
	return sc
}

// NewSetClusterVersionCommand creates a set subcommand of set subcommand
func NewSetClusterVersionCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "cluster-version <version>",
		Short: "set cluster version",
		Run:   setClusterVersionCommandFunc,
	}
	return sc
}

// NewDeleteConfigCommand a set subcommand of cfgCmd
func NewDeleteConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "delete label-property",
		Short: "delete the config option",
	}
	sc.AddCommand(NewDeleteLabelPropertyConfigCommand())
	return sc
}

// NewDeleteLabelPropertyConfigCommand a set subcommand of delete subcommand.
func NewDeleteLabelPropertyConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "label-property <type> <key> <value>",
		Short: "delete a label property config item",
		Run:   deleteLabelPropertyConfigCommandFunc,
	}
	return sc
}

func showConfigCommandFunc(cmd *cobra.Command, args []string) {
	allR, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	allData := make(map[string]interface{})
	err = json.Unmarshal([]byte(allR), &allData)
	if err != nil {
		cmd.Printf("Failed to unmarshal config: %s\n", err)
		return
	}

	data := make(map[string]interface{})
	data["replication"] = allData["replication"]
	scheduleConfig := make(map[string]interface{})
	scheduleConfigData, err := json.Marshal(allData["schedule"])
	if err != nil {
		cmd.Printf("Failed to marshal schedule config: %s\n", err)
		return
	}
	err = json.Unmarshal(scheduleConfigData, &scheduleConfig)
	if err != nil {
		cmd.Printf("Failed to unmarshal schedule config: %s\n", err)
		return
	}

	delete(scheduleConfig, "schedulers-v2")
	delete(scheduleConfig, "schedulers-payload")
	data["schedule"] = scheduleConfig
	r, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal config: %s\n", err)
		return
	}
	cmd.Println(string(r))
}

func showScheduleConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicationPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, labelPropertyPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, clusterVersionPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get cluster version: %s\n", err)
		return
	}
	cmd.Println(r)
}

func postConfigDataWithPath(cmd *cobra.Command, key, value, path string) error {
	var val interface{}
	data := make(map[string]interface{})
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	data[key] = val
	reqData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = doRequest(cmd, path, http.MethodPost,
		WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		return err
	}
	return nil
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	opt, val := args[0], args[1]
	err := postConfigDataWithPath(cmd, opt, val, configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func setLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	postLabelProperty(cmd, "set", args)
}

func deleteLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	postLabelProperty(cmd, "delete", args)
}

func postLabelProperty(cmd *cobra.Command, action string, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"type":        args[0],
		"action":      action,
		"label-key":   args[1],
		"label-value": args[2],
	}
	prefix := path.Join(labelPropertyPrefix)
	postJSON(cmd, prefix, input)
}

func setClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"cluster-version": args[0],
	}
	postJSON(cmd, clusterVersionPrefix, input)
}
