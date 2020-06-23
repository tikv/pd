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
	"io/ioutil"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/spf13/cobra"
)

var (
	configPrefix          = "pd/api/v1/config"
	schedulePrefix        = "pd/api/v1/config/schedule"
	replicatePrefix       = "pd/api/v1/config/replicate"
	labelPropertyPrefix   = "pd/api/v1/config/label-property"
	clusterVersionPrefix  = "pd/api/v1/config/cluster-version"
	rulesPrefix           = "pd/api/v1/config/rules"
	rulePrefix            = "pd/api/v1/config/rule"
	replicationModePrefix = "pd/api/v1/config/replication-mode"
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
	conf.AddCommand(NewPlacementRulesCommand())
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
	sc.AddCommand(newShowReplicationModeCommand())
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

func newShowReplicationModeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "replication-mode",
		Short: "show replication mode config",
		Run:   showReplicationModeCommandFunc,
	}
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
	sc.AddCommand(newSetReplicationModeCommand())
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

func newSetReplicationModeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "replication-mode <mode> [<key>, <value>]",
		Short: "set replication mode config",
		Run:   setReplicationModeCommandFunc,
	}
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
	delete(scheduleConfig, "store-limit")
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
	r, err := doRequest(cmd, replicatePrefix, http.MethodGet)
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

func showReplicationModeCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicationModePrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get replication mode config: %s\n", err)
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

func setReplicationModeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) == 1 {
		postJSON(cmd, replicationModePrefix, map[string]interface{}{"replication-mode": args[0]})
	} else if len(args) == 3 {
		t := findFieldByJSONTag(reflect.TypeOf(config.ReplicationModeConfig{}), []string{args[0], args[1]})
		if t != nil && t.Kind() != reflect.String {
			// convert to number for numberic fields.
			arg2, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				cmd.Printf("value %v cannot covert to number: %v", args[2], err)
				return
			}
			postJSON(cmd, replicationModePrefix, map[string]interface{}{args[0]: map[string]interface{}{args[1]: arg2}})
			return
		}
		postJSON(cmd, replicationModePrefix, map[string]interface{}{args[0]: map[string]string{args[1]: args[2]}})
	} else {
		cmd.Println(cmd.UsageString())
	}
}

func findFieldByJSONTag(t reflect.Type, tags []string) reflect.Type {
	if len(tags) == 0 {
		return t
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < t.NumField(); i++ {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if jsonTag == tags[0] {
			return findFieldByJSONTag(t.Field(i).Type, tags[1:])
		}
	}
	return nil
}

// NewPlacementRulesCommand placement rules subcommand
func NewPlacementRulesCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "placement-rules",
		Short: "placement rules configuration",
	}
	enable := &cobra.Command{
		Use:   "enable",
		Short: "enable placement rules",
		Run:   enablePlacementRulesFunc,
	}
	disable := &cobra.Command{
		Use:   "disable",
		Short: "disable placement rules",
		Run:   disablePlacementRulesFunc,
	}
	show := &cobra.Command{
		Use:   "show",
		Short: "show placement rules",
		Run:   getPlacementRulesFunc,
	}
	show.Flags().String("group", "", "group id")
	show.Flags().String("id", "", "rule id")
	show.Flags().String("region", "", "region id")
	load := &cobra.Command{
		Use:   "load",
		Short: "load placement rules to a file",
		Run:   getPlacementRulesFunc,
	}
	load.Flags().String("group", "", "group id")
	load.Flags().String("id", "", "rule id")
	load.Flags().String("region", "", "region id")
	load.Flags().String("out", "rules.json", "the filename contains rules")
	save := &cobra.Command{
		Use:   "save",
		Short: "save rules from file",
		Run:   putPlacementRulesFunc,
	}
	save.Flags().String("in", "rules.json", "the filename contains rules")
	c.AddCommand(enable, disable, show, load, save)
	return c
}

func enablePlacementRulesFunc(cmd *cobra.Command, args []string) {
	err := postConfigDataWithPath(cmd, "enable-placement-rules", "true", configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func disablePlacementRulesFunc(cmd *cobra.Command, args []string) {
	err := postConfigDataWithPath(cmd, "enable-placement-rules", "false", configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func getPlacementRulesFunc(cmd *cobra.Command, args []string) {
	getFlag := func(key string) string {
		if f := cmd.Flag(key); f != nil {
			return f.Value.String()
		}
		return ""
	}

	group, id, region, file := getFlag("group"), getFlag("id"), getFlag("region"), getFlag("out")
	var reqPath string
	respIsList := true
	switch {
	case region == "" && group == "" && id == "": // all rules
		reqPath = rulesPrefix
	case region == "" && group == "" && id != "":
		cmd.Println(`"id" should be specified along with "group"`)
		return
	case region == "" && group != "" && id == "": // all rules in a group
		reqPath = path.Join(rulesPrefix, "group", group)
	case region == "" && group != "" && id != "": // single rule
		reqPath, respIsList = path.Join(rulePrefix, group, id), false
	case region != "" && group == "" && id == "": // rules matches a region
		reqPath = path.Join(rulesPrefix, "region", region)
	default:
		cmd.Println(`"region" should not be specified with "group" or "id" at the same time`)
		return
	}
	res, err := doRequest(cmd, reqPath, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	if file == "" {
		cmd.Println(res)
		return
	}
	if !respIsList {
		res = "[\n" + res + "]\n"
	}
	err = ioutil.WriteFile(file, []byte(res), 0644)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("rules saved to file " + file)
}

func putPlacementRulesFunc(cmd *cobra.Command, args []string) {
	var file string
	if f := cmd.Flag("in"); f != nil {
		file = f.Value.String()
	}
	content, err := ioutil.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}
	var rules []*placement.Rule
	if err = json.Unmarshal(content, &rules); err != nil {
		cmd.Println(err)
		return
	}
	for _, r := range rules {
		if r.Count > 0 {
			b, _ := json.Marshal(r)
			_, err = doRequest(cmd, rulePrefix, http.MethodPost, WithBody("application/json", bytes.NewBuffer(b)))
			if err != nil {
				fmt.Printf("failed to save rule %s/%s: %v\n", r.GroupID, r.ID, err)
				return
			}
			fmt.Printf("saved rule %s/%s\n", r.GroupID, r.ID)
		}
	}
	for _, r := range rules {
		if r.Count == 0 {
			_, err = doRequest(cmd, path.Join(rulePrefix, r.GroupID, r.ID), http.MethodDelete)
			if err != nil {
				fmt.Printf("failed to delete rule %s/%s: %v\n", r.GroupID, r.ID, err)
				return
			}
			fmt.Printf("deleted rule %s/%s\n", r.GroupID, r.ID)
		}
	}
	cmd.Println("Success!")
}
