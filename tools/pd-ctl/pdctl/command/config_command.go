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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/server/config"
)

// flagFromAPIServer has no influence for pd mode, but it is useful for us to debug in api mode.
const flagFromAPIServer = "from_api_server"

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:               "config <subcommand>",
		Short:             "tune pd configs",
		PersistentPreRunE: requirePDClient,
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
	sc.AddCommand(NewShowServerConfigCommand())
	sc.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of PD",
		Run:   showAllConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
	return sc
}

// NewShowScheduleConfigCommand return a show all subcommand of show subcommand
func NewShowScheduleConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "schedule",
		Short: "show schedule config of PD",
		Run:   showScheduleConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
	return sc
}

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication",
		Short: "show replication config of PD",
		Run:   showReplicationConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
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

func showConfigCommandFunc(cmd *cobra.Command, _ []string) {
	allData, err := pdRespHandler(cmd).GetConfig(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	data := make(map[string]any)
	data["replication"] = allData["replication"]
	scheduleConfig := make(map[string]any)
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
	jsonPrint(cmd, data)
}

var hideConfig = []string{
	"schedulers-v2",
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

func showScheduleConfigCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := pdRespHandler(cmd).GetScheduleConfig(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := pdRespHandler(cmd).GetReplicateConfig(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := PDCli.GetLabelPropertyConfig(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := pdRespHandler(cmd).GetConfig(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func showClusterVersionCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := PDCli.GetClusterVersion(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get cluster version: %s\n", err)
		return
	}
	// add `""` is to maintain consistency with the original output without the PD HTTP client.
	cmd.Println(fmt.Sprintf(`"%s"`, r))
}

func showReplicationModeCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := PDCli.GetReplicationModeConfig(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get replication mode config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func showServerCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := PDCli.GetPDServerConfig(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get server config: %s\n", err)
		return
	}
	jsonPrint(cmd, r)
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	var val any
	val, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		val = args[1]
	}
	if err = PDCli.SetConfig(cmd.Context(), map[string]any{
		args[0]: val,
	}); err != nil {
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
	input := map[string]any{
		"type":        args[0],
		"action":      action,
		"label-key":   args[1],
		"label-value": args[2],
	}
	if err := PDCli.SetLabelPropertyConfig(cmd.Context(), input); err != nil {
		cmd.Printf("Failed to set label property config: %s\n", err)
		return
	}
	cmd.Println("Success! The config is updated.")
}

func setClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]any{
		"cluster-version": args[0],
	}
	if err := PDCli.SetClusterVersion(cmd.Context(), input); err != nil {
		cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
		return
	}
	cmd.Println("Success! The cluster version is updated.")
}

func setReplicationModeCommandFunc(cmd *cobra.Command, args []string) {
	successOutput := "Success! The replication mode config is updated."
	switch len(args) {
	case 1:
		if err := PDCli.SetReplicationModeConfig(cmd.Context(), map[string]any{"replication-mode": args[0]}); err != nil {
			cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
			return
		}
		cmd.Println(successOutput)
	case 3:
		t := reflectutil.FindFieldByJSONTag(reflect.TypeOf(config.ReplicationModeConfig{}), []string{args[0], args[1]})
		if t != nil && t.Kind() == reflect.Int {
			// convert to number for numeric fields.
			arg2, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				cmd.Printf("value %v cannot covert to number: %v", args[2], err)
				return
			}
			if err := PDCli.SetReplicationModeConfig(cmd.Context(), map[string]any{args[0]: map[string]any{args[1]: arg2}}); err != nil {
				cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
				return
			}
			cmd.Println(successOutput)
			return
		}
		if err := PDCli.SetReplicationModeConfig(cmd.Context(), map[string]any{args[0]: map[string]any{args[1]: args[2]}}); err != nil {
			cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
			return
		}
		cmd.Println(successOutput)
	default:
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
	show.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
	load := &cobra.Command{
		Use:   "load",
		Short: "load placement rules to a file",
		Run:   getPlacementRulesFunc,
	}
	load.Flags().String("group", "", "group id")
	load.Flags().String("id", "", "rule id")
	load.Flags().String("region", "", "region id")
	load.Flags().String("out", "rules.json", "the filename contains rules")
	load.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
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
	ruleGroupShow.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
	ruleGroupSet := &cobra.Command{
		Use:   "set <id> <index> <override>",
		Short: "update rule group configuration",
		Run:   updateRuleGroupFunc,
	}
	ruleGroupDelete := &cobra.Command{
		Use:   "delete <id>",
		Short: "delete rule group configuration. Note: this command will be deprecated soon, use <rule-bundle delete> instead",
		Run:   delRuleBundle,
	}
	ruleGroupDelete.Flags().Bool("regexp", false, "match group id by regular expression")
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
	ruleBundleGet.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
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
	ruleBundleLoad.Flags().Bool(flagFromAPIServer, false, "read data from api server rather than micro service")
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

func enablePlacementRulesFunc(cmd *cobra.Command, _ []string) {
	if err := PDCli.SetConfig(cmd.Context(), map[string]any{
		"enable-placement-rules": "true",
	}); err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func disablePlacementRulesFunc(cmd *cobra.Command, _ []string) {
	if err := PDCli.SetConfig(cmd.Context(), map[string]any{
		"enable-placement-rules": "false",
	}); err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func getPlacementRulesFunc(cmd *cobra.Command, _ []string) {
	getFlag := func(key string) string {
		if f := cmd.Flag(key); f != nil {
			return f.Value.String()
		}
		return ""
	}

	group, id, region, file := getFlag("group"), getFlag("id"), getFlag("region"), getFlag("out")
	var rules any
	var err error
	respIsList := true
	switch {
	case region == "" && group == "" && id == "": // all rules
		rules, err = pdRespHandler(cmd).GetAllPlacementRules(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	case region == "" && group == "" && id != "":
		cmd.Println(`"id" should be specified along with "group"`)
		return
	case region == "" && group != "" && id == "": // all rules in a group
		rules, err = pdRespHandler(cmd).GetPlacementRulesByGroup(cmd.Context(), group, withForbiddenForwardToMicroServiceHeader(cmd)...)
	case region == "" && group != "" && id != "": // single rule
		rules, err = pdRespHandler(cmd).GetPlacementRule(cmd.Context(), group, id, withForbiddenForwardToMicroServiceHeader(cmd)...)
		respIsList = false
	case region != "" && group == "" && id == "": // rules matches a region
		var detail bool
		if ok, _ := cmd.Flags().GetBool("detail"); ok {
			detail = true
		}
		rules, err = pdRespHandler(cmd).GetPlacementRulesByRegion(cmd.Context(), region, detail, withForbiddenForwardToMicroServiceHeader(cmd)...)
	default:
		cmd.Println(`"region" should not be specified with "group" or "id" at the same time`)
		return
	}
	if err != nil {
		cmd.Println(err)
		return
	}

	jsonBytes, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal the data to json: %s\n", err)
		return
	}
	res := string(jsonBytes)
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

func putPlacementRulesFunc(cmd *cobra.Command, _ []string) {
	var file string
	if f := cmd.Flag("in"); f != nil {
		file = f.Value.String()
	}
	content, err := os.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}

	var opts []*pd.RuleOp
	if err = json.Unmarshal(content, &opts); err != nil {
		cmd.Println(err)
		return
	}

	validOpts := opts[:0]
	for _, op := range opts {
		if op.Count > 0 {
			op.Action = pd.RuleOpAdd
			validOpts = append(validOpts, op)
		} else if op.Count == 0 {
			op.Action = pd.RuleOpDel
			validOpts = append(validOpts, op)
		}
	}

	if err = PDCli.SetPlacementRuleInBatch(cmd.Context(), validOpts); err != nil {
		cmd.Printf("failed to save rules %s\n", err)
		return
	}

	cmd.Println("Success!")
}

func showRuleGroupFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	var res any
	var err error
	if len(args) > 0 {
		res, err = pdRespHandler(cmd).GetPlacementRuleGroupByID(cmd.Context(), args[0], withForbiddenForwardToMicroServiceHeader(cmd)...)
	} else {
		res, err = pdRespHandler(cmd).GetAllPlacementRuleGroups(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	}
	if err != nil {
		cmd.Println(err)
		return
	}
	jsonPrint(cmd, res)
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
	if err = PDCli.SetPlacementRuleGroup(cmd.Context(), &pd.RuleGroup{
		ID:       args[0],
		Index:    int(index),
		Override: override,
	}); err != nil {
		cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
		return
	}
	cmd.Println("Success! Update rule group successfully.")
}

func getRuleBundle(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	res, err := pdRespHandler(cmd).GetPlacementRuleBundleByGroup(cmd.Context(), args[0], withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Println(err)
		return
	}
	ruleGroupWriteToFile(cmd, res)
}

func setRuleBundle(cmd *cobra.Command, _ []string) {
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

	var ruleBundle *pd.GroupBundle
	if err = json.Unmarshal(content, &ruleBundle); err != nil {
		cmd.Println(err)
		return
	}
	if err = PDCli.SetPlacementRuleBundleByGroup(cmd.Context(), id.GroupID, ruleBundle); err != nil {
		cmd.Printf("failed to save rule bundle %s: %s\n", content, err)
		return
	}

	cmd.Println("Success! Update group and rules successfully.")
}

func delRuleBundle(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	var regexp bool
	if ok, _ := cmd.Flags().GetBool("regexp"); ok {
		regexp = true
	}
	if err := PDCli.DeletePlacementRuleBundleByGroup(cmd.Context(), url.PathEscape(args[0]), regexp); err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Delete group and rules successfully.")
}

func loadRuleBundle(cmd *cobra.Command, _ []string) {
	res, err := pdRespHandler(cmd).GetAllPlacementRuleBundles(cmd.Context(), withForbiddenForwardToMicroServiceHeader(cmd)...)
	if err != nil {
		cmd.Println(err)
		return
	}
	ruleGroupWriteToFile(cmd, res)
}

func saveRuleBundle(cmd *cobra.Command, _ []string) {
	var file string
	if f := cmd.Flag("in"); f != nil {
		file = f.Value.String()
	}
	content, err := os.ReadFile(file)
	if err != nil {
		cmd.Println(err)
		return
	}

	allData := make([]*pd.GroupBundle, 0)
	if err = json.Unmarshal(content, &allData); err != nil {
		cmd.Printf("Failed to unmarshal config: %s\n", err)
		return
	}
	var partial bool
	if ok, _ := cmd.Flags().GetBool("partial"); ok {
		partial = true
	}
	if err = PDCli.SetPlacementRuleBundles(cmd.Context(), allData, partial); err != nil {
		cmd.Printf("failed to save rule bundles %s: %s\n", content, err)
		return
	}

	cmd.Println("Success! Update rules and groups successfully.")
}

func withForbiddenForwardToMicroServiceHeader(cmd *cobra.Command) []pd.HeaderOption {
	forbiddenRedirectToMicroService, err := cmd.Flags().GetBool(flagFromAPIServer)
	if err == nil && forbiddenRedirectToMicroService {
		return []pd.HeaderOption{pd.WithForbiddenForwardToMicroServiceHeader()}
	}
	return nil
}

func pdRespHandler(cmd *cobra.Command) pd.Client {
	forbiddenRedirectToMicroService, err := cmd.Flags().GetBool(flagFromAPIServer)
	if err == nil && forbiddenRedirectToMicroService {
		return PDCli.WithRespHandler(microServiceRespHandler)
	}
	return PDCli
}

// microServiceRespHandler will be injected into the PD HTTP client to handle the response,
func microServiceRespHandler(resp *http.Response, res any) error {
	// Check response, it is used to mark whether the request has been forwarded to the micro service.
	if resp.Header.Get(apiutil.XForwardedToMicroServiceHeader) == "true" {
		return errors.Errorf("the request is forwarded to micro service unexpectedly")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bs, _ := io.ReadAll(resp.Body)
		return errors.Errorf("request pd http api failed with status: '%s', body: '%s'", resp.Status, bs)
	}

	if res == nil {
		return nil
	}

	err := json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func ruleGroupWriteToFile(cmd *cobra.Command, val any) {
	file := ""
	if f := cmd.Flag("out"); f != nil {
		file = f.Value.String()
	}

	jsonBytes, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal the data to json: %s\n", err)
		return
	}

	if file == "" {
		cmd.Println(string(jsonBytes))
		return
	}

	err = os.WriteFile(file, jsonBytes, 0644) // #nosec
	if err != nil {
		cmd.Printf("Failed to write the data to file: %s\n", err)
		return
	}
	cmd.Printf("rule group saved to file %s\n", file)
}
