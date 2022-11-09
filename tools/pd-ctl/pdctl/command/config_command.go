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
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/reflectutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule/placement"
)

var (
	configPrefix          = "pd/api/v1/config"
	schedulePrefix        = "pd/api/v1/config/schedule"
	replicatePrefix       = "pd/api/v1/config/replicate"
	labelPropertyPrefix   = "pd/api/v1/config/label-property"
	clusterVersionPrefix  = "pd/api/v1/config/cluster-version"
	rulesPrefix           = "pd/api/v1/config/rules"
	rulesBatchPrefix      = "pd/api/v1/config/rules/batch"
	rulePrefix            = "pd/api/v1/config/rule"
	ruleGroupPrefix       = "pd/api/v1/config/rule_group"
	ruleGroupsPrefix      = "pd/api/v1/config/rule_groups"
	replicationModePrefix = "pd/api/v1/config/replication-mode"
	ruleBundlePrefix      = "pd/api/v1/config/placement-rule"
	pdServerPrefix        = "pd/api/v1/config/pd-server"
	switchWitnessPrefix   = "pd/api/v1/config/switch-witness"
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
	conf.AddCommand(NewSwitchWitnessCommand())
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
	sc.AddCommand(NewShowServerConfigCommand())
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

// NewShowServerConfigCommand returns a server configuration of show subcommand.
func NewShowServerConfigCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "server",
		Short: "show PD server config",
		Run:   showServerCommandFunc,
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
	allR, err := doRequest(cmd, configPrefix, http.MethodGet, http.Header{})
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

	for _, config := range hideConfig {
		delete(scheduleConfig, config)
	}

	data["schedule"] = scheduleConfig
	r, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal config: %s\n", err)
		return
	}
	cmd.Println(string(r))
}

var hideConfig = []string{
	"schedulers-v2",
	"schedulers-payload",
	"store-limit",
	"enable-remove-down-replica",
	"enable-replace-offline-replica",
	"enable-make-up-replica",
	"enable-remove-extra-replica",
	"enable-location-replacement",
	"enable-one-way-merge",
	"enable-debug-metrics",
	"store-limit-mode",
	"scheduler-max-waiting-operator",
}

func showScheduleConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicatePrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, labelPropertyPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, clusterVersionPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get cluster version: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationModeCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicationModePrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get replication mode config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showServerCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, pdServerPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get server config: %s\n", err)
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
		http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(reqData)))
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
		t := reflectutil.FindFieldByJSONTag(reflect.TypeOf(config.ReplicationModeConfig{}), []string{args[0], args[1]})
		if t != nil && t.Kind() == reflect.Int {
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
	show.Flags().Bool("detail", false, "detailed match info for region")
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
	ruleGroup := &cobra.Command{
		Use:   "rule-group",
		Short: "rule group configurations",
	}
	ruleGroupShow := &cobra.Command{
		Use:   "show [id]",
		Short: "show rule group configuration(s)",
		Run:   showRuleGroupFunc,
	}
	ruleGroupSet := &cobra.Command{
		Use:   "set <id> <index> <override>",
		Short: "update rule group configuration",
		Run:   updateRuleGroupFunc,
	}
	ruleGroupDelete := &cobra.Command{
		Use:   "delete <id>",
		Short: "delete rule group configuration",
		Run:   deleteRuleGroupFunc,
	}
	ruleGroup.AddCommand(ruleGroupShow, ruleGroupSet, ruleGroupDelete)
	ruleBundle := &cobra.Command{
		Use:   "rule-bundle",
		Short: "process rules in group(s), set/save perform in a replace fashion",
	}
	ruleBundleGet := &cobra.Command{
		Use:   "get <id>",
		Short: "get rule group config and its rules by group id",
		Run:   getRuleBundle,
	}
	ruleBundleGet.Flags().String("out", "", "the output file")
	ruleBundleSet := &cobra.Command{
		Use:   "set",
		Short: "set rule group config and its rules from file",
		Run:   setRuleBundle,
	}
	ruleBundleSet.Flags().String("in", "group.json", "the file contains one group config and its rules")
	ruleBundleDelete := &cobra.Command{
		Use:   "delete <id>",
		Short: "delete rule group config and its rules by group id",
		Run:   delRuleBundle,
	}
	ruleBundleDelete.Flags().Bool("regexp", false, "match group id by regular expression")
	ruleBundleLoad := &cobra.Command{
		Use:   "load",
		Short: "load all group configs and rules to file",
		Run:   loadRuleBundle,
	}
	ruleBundleLoad.Flags().String("out", "rules.json", "the output file")
	ruleBundleSave := &cobra.Command{
		Use:   "save",
		Short: "save all group configs and rules from file",
		Run:   saveRuleBundle,
	}
	ruleBundleSave.Flags().String("in", "rules.json", "the file contains all group configs and all rules")
	ruleBundleSave.Flags().Bool("partial", false, "do not drop all old configurations, partial update")
	ruleBundle.AddCommand(ruleBundleGet, ruleBundleSet, ruleBundleDelete, ruleBundleLoad, ruleBundleSave)
	c.AddCommand(enable, disable, show, load, save, ruleGroup, ruleBundle)
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
		if ok, _ := cmd.Flags().GetBool("detail"); ok {
			reqPath = path.Join(reqPath, "detail")
		}
	default:
		cmd.Println(`"region" should not be specified with "group" or "id" at the same time`)
		return
	}
	res, err := doRequest(cmd, reqPath, http.MethodGet, http.Header{})
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
	err = os.WriteFile(file, []byte(res), 0644) // #nosec
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
	content, err := os.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}

	var opts []*placement.RuleOp
	if err = json.Unmarshal(content, &opts); err != nil {
		cmd.Println(err)
		return
	}

	validOpts := opts[:0]
	for _, op := range opts {
		if op.Count > 0 {
			op.Action = placement.RuleOpAdd
			validOpts = append(validOpts, op)
		} else if op.Count == 0 {
			op.Action = placement.RuleOpDel
			validOpts = append(validOpts, op)
		}
	}

	b, _ := json.Marshal(validOpts)
	_, err = doRequest(cmd, rulesBatchPrefix, http.MethodPost, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(b)))
	if err != nil {
		cmd.Printf("failed to save rules %s: %s\n", b, err)
		return
	}

	cmd.Println("Success!")
}

func showRuleGroupFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	reqPath := ruleGroupsPrefix
	if len(args) > 0 {
		reqPath = path.Join(ruleGroupPrefix, args[0])
	}

	res, err := doRequest(cmd, reqPath, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(res)
}

func updateRuleGroupFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		cmd.Printf("index %s should be a number\n", args[1])
		return
	}
	var override bool
	switch strings.ToLower(args[2]) {
	case "false":
	case "true":
		override = true
	default:
		cmd.Printf("override %s should be a boolean\n", args[2])
		return
	}
	postJSON(cmd, ruleGroupPrefix, map[string]interface{}{
		"id":       args[0],
		"index":    index,
		"override": override,
	})
}

func deleteRuleGroupFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	_, err := doRequest(cmd, path.Join(ruleGroupPrefix, args[0]), http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Printf("Failed to remove rule group config: %s \n", err)
		return
	}
	cmd.Println("Success!")
}

func getRuleBundle(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	reqPath := path.Join(ruleBundlePrefix, args[0])

	res, err := doRequest(cmd, reqPath, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}

	file := ""
	if f := cmd.Flag("out"); f != nil {
		file = f.Value.String()
	}
	if file == "" {
		cmd.Println(res)
		return
	}

	err = os.WriteFile(file, []byte(res), 0644) // #nosec
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Printf("rule group saved to file %s\n", file)
}

func setRuleBundle(cmd *cobra.Command, args []string) {
	var file string
	if f := cmd.Flag("in"); f != nil {
		file = f.Value.String()
	}
	content, err := os.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}

	id := struct {
		GroupID string `json:"group_id"`
	}{}
	if err = json.Unmarshal(content, &id); err != nil {
		cmd.Println(err)
		return
	}

	reqPath := path.Join(ruleBundlePrefix, id.GroupID)

	res, err := doRequest(cmd, reqPath, http.MethodPost, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewReader(content)))
	if err != nil {
		cmd.Printf("failed to save rule bundle %s: %s\n", content, err)
		return
	}

	cmd.Println(res)
}

func delRuleBundle(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	reqPath := path.Join(ruleBundlePrefix, url.PathEscape(args[0]))

	if ok, _ := cmd.Flags().GetBool("regexp"); ok {
		reqPath += "?regexp"
	}

	res, err := doRequest(cmd, reqPath, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}

	cmd.Println(res)
}

func loadRuleBundle(cmd *cobra.Command, args []string) {
	res, err := doRequest(cmd, ruleBundlePrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}

	file := ""
	if f := cmd.Flag("out"); f != nil {
		file = f.Value.String()
	}
	if file == "" {
		cmd.Println(res)
		return
	}

	err = os.WriteFile(file, []byte(res), 0644) // #nosec
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Printf("rule group saved to file %s\n", file)
}

func saveRuleBundle(cmd *cobra.Command, args []string) {
	var file string
	if f := cmd.Flag("in"); f != nil {
		file = f.Value.String()
	}
	content, err := os.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}

	path := ruleBundlePrefix
	if ok, _ := cmd.Flags().GetBool("partial"); ok {
		path += "?partial=true"
	}

	res, err := doRequest(cmd, path, http.MethodPost, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewReader(content)))
	if err != nil {
		cmd.Printf("failed to save rule bundles %s: %s\n", content, err)
		return
	}

	cmd.Println(res)
}

// NewSwitchWitnessCommand placement rules subcommand
func NewSwitchWitnessCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "switch-witness",
		Short: "switch witness configuration",
	}
	enable := &cobra.Command{
		Use:   "enable",
		Short: "enable switch witness",
		Run:   enableSwitchWitness,
	}
	disable := &cobra.Command{
		Use:   "disable",
		Short: "disable switch witness",
		Run:   disableSwitchWitness,
	}
	c.AddCommand(enable, disable)
	return c
}

func enableSwitchWitness(cmd *cobra.Command, args []string) {
	err := postConfigDataWithPath(cmd, "enable-switch-witness", "true", configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func disableSwitchWitness(cmd *cobra.Command, args []string) {
	err := postConfigDataWithPath(cmd, "enable-switch-witness", "false", configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}
