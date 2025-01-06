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
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/server/config"
)

const (
	configPrefix                  = "pd/api/v1/config"
	schedulePrefix                = "pd/api/v1/config/schedule"
	replicatePrefix               = "pd/api/v1/config/replicate"
	labelPropertyPrefix           = "pd/api/v1/config/label-property"
	clusterVersionPrefix          = "pd/api/v1/config/cluster-version"
	rulesPrefix                   = "pd/api/v1/config/rules"
	rulesBatchPrefix              = "pd/api/v1/config/rules/batch"
	rulePrefix                    = "pd/api/v1/config/rule"
	ruleGroupPrefix               = "pd/api/v1/config/rule_group"
	ruleGroupsPrefix              = "pd/api/v1/config/rule_groups"
	replicationModePrefix         = "pd/api/v1/config/replication-mode"
	ruleBundlePrefix              = "pd/api/v1/config/placement-rule"
	pdServerPrefix                = "pd/api/v1/config/pd-server"
	serviceMiddlewareConfigPrefix = "pd/api/v1/service-middleware/config"
	// flagFromPDService has no influence for pd mode, but it is useful for us to debug in pd service mode.
	flagFromPDService = "from_pd_service"
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
	sc.AddCommand(NewShowServerConfigCommand())
	sc.AddCommand(NewShowServiceMiddlewareConfigCommand())
	sc.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of PD",
		Run:   showAllConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
	return sc
}

// NewShowScheduleConfigCommand return a show all subcommand of show subcommand
func NewShowScheduleConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "schedule",
		Short: "show schedule config of PD",
		Run:   showScheduleConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
	return sc
}

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication",
		Short: "show replication config of PD",
		Run:   showReplicationConfigCommandFunc,
	}
	sc.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
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

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowServiceMiddlewareConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "service-middleware",
		Short: "show service middleware config of PD",
		Run:   showServiceMiddlewareConfigCommandFunc,
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
	sc.AddCommand(newSetReplicationModeCommand())
	sc.AddCommand(newSetServiceMiddlewareCommand())
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

func newSetServiceMiddlewareCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "service-middleware <type> [<key> <value> | <label> <qps|concurrency> <value>]",
		Short: "set service middleware config",
		Run:   setServiceMiddlewareCommandFunc,
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
	header := buildHeader(cmd)
	allR, err := doRequest(cmd, configPrefix, http.MethodGet, header)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	allData := make(map[string]any)
	err = json.Unmarshal([]byte(allR), &allData)
	if err != nil {
		cmd.Printf("Failed to unmarshal config: %s\n", err)
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
	r, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal config: %s\n", err)
		return
	}
	cmd.Println(string(r))
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
	header := buildHeader(cmd)
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet, header)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, _ []string) {
	header := buildHeader(cmd)
	r, err := doRequest(cmd, replicatePrefix, http.MethodGet, header)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, labelPropertyPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, _ []string) {
	header := buildHeader(cmd)
	r, err := doRequest(cmd, configPrefix, http.MethodGet, header)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showClusterVersionCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, clusterVersionPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get cluster version: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationModeCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, replicationModePrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get replication mode config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showServerCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, pdServerPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get server config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showServiceMiddlewareConfigCommandFunc(cmd *cobra.Command, _ []string) {
	header := buildHeader(cmd)
	r, err := doRequest(cmd, serviceMiddlewareConfigPrefix, http.MethodGet, header)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func postConfigDataWithPath(cmd *cobra.Command, key, value, path string) error {
	var val any
	data := make(map[string]any)
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
	input := map[string]any{
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
	input := map[string]any{
		"cluster-version": args[0],
	}
	postJSON(cmd, clusterVersionPrefix, input)
}

func setReplicationModeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) == 1 {
		postJSON(cmd, replicationModePrefix, map[string]any{"replication-mode": args[0]})
	} else if len(args) == 3 {
		t := reflectutil.FindFieldByJSONTag(reflect.TypeOf(config.ReplicationModeConfig{}), []string{args[0], args[1]})
		if t != nil && t.Kind() == reflect.Int {
			// convert to number for numeric fields.
			arg2, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				cmd.Printf("value %v cannot covert to number: %v", args[2], err)
				return
			}
			postJSON(cmd, replicationModePrefix, map[string]any{args[0]: map[string]any{args[1]: arg2}})
			return
		}
		postJSON(cmd, replicationModePrefix, map[string]any{args[0]: map[string]string{args[1]: args[2]}})
	} else {
		cmd.Println(cmd.UsageString())
	}
}

func setServiceMiddlewareCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 && len(args) != 4 {
		cmd.Println(cmd.UsageString())
		return
	}

	if len(args) == 3 {
		cfg := map[string]any{
			fmt.Sprintf("%s.%s", args[0], args[1]): args[2],
		}
		postJSON(cmd, serviceMiddlewareConfigPrefix, cfg)
		return
	}

	input := map[string]any{
		"label": args[1],
	}

	if strings.ToLower(args[2]) == "qps" {
		value, err := strconv.ParseFloat(args[3], 64)
		if err != nil {
			cmd.Println(err)
			return
		}
		input["qps"] = value
	} else if strings.ToLower(args[2]) == "concurrency" {
		value, err := strconv.ParseUint(args[3], 10, 64)
		if err != nil {
			cmd.Println(err)
			return
		}
		input["concurrency"] = value
	} else {
		cmd.Println("Input is invalid, should be qps or concurrency")
		return
	}

	if args[0] == "rate-limit" {
		input["type"] = "label"
		postJSON(cmd, serviceMiddlewareConfigPrefix+"/rate-limit", input)
		return
	} else if args[0] == "grpc-rate-limit" {
		postJSON(cmd, serviceMiddlewareConfigPrefix+"/grpc-rate-limit", input)
		return
	}
	cmd.Printf("Failed to get correct type: %s\n", args[0])
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
	show.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
	load := &cobra.Command{
		Use:   "load",
		Short: "load placement rules to a file",
		Run:   getPlacementRulesFunc,
	}
	load.Flags().String("group", "", "group id")
	load.Flags().String("id", "", "rule id")
	load.Flags().String("region", "", "region id")
	load.Flags().String("out", "rules.json", "the filename contains rules")
	load.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
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
	ruleGroupShow.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
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
	ruleBundleGet.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
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
	ruleBundleLoad.Flags().Bool(flagFromPDService, false, "read data from PD service rather than micro service")
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
	err := postConfigDataWithPath(cmd, "enable-placement-rules", "true", configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func disablePlacementRulesFunc(cmd *cobra.Command, _ []string) {
	err := postConfigDataWithPath(cmd, "enable-placement-rules", "false", configPrefix)
	if err != nil {
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
	header := buildHeader(cmd)
	res, err := doRequest(cmd, reqPath, http.MethodGet, header)
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
	header := buildHeader(cmd)
	res, err := doRequest(cmd, reqPath, http.MethodGet, header)
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
	postJSON(cmd, ruleGroupPrefix, map[string]any{
		"id":       args[0],
		"index":    index,
		"override": override,
	})
}

func getRuleBundle(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	reqPath := path.Join(ruleBundlePrefix, args[0])
	header := buildHeader(cmd)
	res, err := doRequest(cmd, reqPath, http.MethodGet, header)
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

func loadRuleBundle(cmd *cobra.Command, _ []string) {
	header := buildHeader(cmd)
	res, err := doRequest(cmd, ruleBundlePrefix, http.MethodGet, header)
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

func buildHeader(cmd *cobra.Command) http.Header {
	header := http.Header{}
	forbiddenRedirectToMicroService, err := cmd.Flags().GetBool(flagFromPDService)
	if err == nil && forbiddenRedirectToMicroService {
		header.Add(apiutil.XForbiddenForwardToMicroServiceHeader, "true")
	}
	return header
}
