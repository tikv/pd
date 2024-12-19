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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"
)

var (
	schedulersPrefix          = "pd/api/v1/schedulers"
	schedulerConfigPrefix     = "pd/api/v1/scheduler-config"
	schedulerDiagnosticPrefix = "pd/api/v1/schedulers/diagnostic"
	evictLeaderSchedulerName  = "evict-leader-scheduler"
	grantLeaderSchedulerName  = "grant-leader-scheduler"
)

// NewSchedulerCommand returns a scheduler command.
func NewSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler commands",
	}
	c.AddCommand(NewShowSchedulerCommand())
	c.AddCommand(NewAddSchedulerCommand())
	c.AddCommand(NewRemoveSchedulerCommand())
	c.AddCommand(NewPauseSchedulerCommand())
	c.AddCommand(NewResumeSchedulerCommand())
	c.AddCommand(NewConfigSchedulerCommand())
	c.AddCommand(NewDescribeSchedulerCommand())
	return c
}

// NewPauseSchedulerCommand returns a command to pause a scheduler.
func NewPauseSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "pause <scheduler> <delay_seconds>",
		Short: "pause a scheduler",
		Run:   pauseSchedulerCommandFunc,
	}
	return c
}

func pauseSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	delay, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || delay <= 0 {
		cmd.Usage()
		return
	}
	path := schedulersPrefix + "/" + getEscapedSchedulerName(args[0])
	input := map[string]any{"delay": delay}
	postJSON(cmd, path, input)
}

// Since certain scheduler's name is defined by caller such as scatter-range,
// it's possible the name contains special characters, like "#", "&" and so on.
// So we need to escape the scheduler name here before attaching it to the URL.
func getEscapedSchedulerName(schedulerName string) string {
	return url.PathEscape(schedulerName)
}

// NewResumeSchedulerCommand returns a command to resume a scheduler.
func NewResumeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "resume <scheduler>",
		Short: "resume a scheduler",
		Run:   resumeSchedulerCommandFunc,
	}
	return c
}

func resumeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	path := schedulersPrefix + "/" + getEscapedSchedulerName(args[0])
	input := map[string]any{"delay": 0}
	postJSON(cmd, path, input)
}

// NewShowSchedulerCommand returns a command to show schedulers.
func NewShowSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show",
		Short: "show schedulers",
		Run:   showSchedulerCommandFunc,
	}
	c.Flags().String("status", "", "the scheduler status value can be [paused | disabled]")
	c.Flags().BoolP("timestamp", "t", false, "fetch the paused and resume timestamp for paused scheduler(s)")
	return c
}

func showSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	url := schedulersPrefix
	if flag := cmd.Flag("status"); flag != nil && flag.Value.String() != "" {
		url = fmt.Sprintf("%s?status=%s", url, flag.Value.String())
		if tsFlag, _ := cmd.Flags().GetBool("timestamp"); tsFlag {
			url += "&timestamp=true"
		}
	}
	r, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

// NewAddSchedulerCommand returns a command to add scheduler.
func NewAddSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <scheduler>",
		Short: "add a scheduler",
	}
	c.AddCommand(NewGrantLeaderSchedulerCommand())
	c.AddCommand(NewEvictLeaderSchedulerCommand())
	c.AddCommand(NewShuffleLeaderSchedulerCommand())
	c.AddCommand(NewShuffleRegionSchedulerCommand())
	c.AddCommand(NewShuffleHotRegionSchedulerCommand())
	c.AddCommand(NewScatterRangeSchedulerCommand())
	c.AddCommand(NewScatterRangeSchedulerCommandV2())
	c.AddCommand(NewBalanceLeaderSchedulerCommand())
	c.AddCommand(NewBalanceRegionSchedulerCommand())
	c.AddCommand(NewBalanceHotRegionSchedulerCommand())
	c.AddCommand(NewRandomMergeSchedulerCommand())
	c.AddCommand(NewLabelSchedulerCommand())
	c.AddCommand(NewEvictSlowStoreSchedulerCommand())
	c.AddCommand(NewGrantHotRegionSchedulerCommand())
	c.AddCommand(NewSplitBucketSchedulerCommand())
	c.AddCommand(NewSlowTrendEvictLeaderSchedulerCommand())
	c.AddCommand(NewBalanceWitnessSchedulerCommand())
	c.AddCommand(NewTransferWitnessLeaderSchedulerCommand())
	return c
}

// NewGrantLeaderSchedulerCommand returns a command to add a grant-leader-scheduler.
func NewGrantLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-leader-scheduler <store_id>",
		Short: "add a scheduler to grant leader to a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

// NewEvictLeaderSchedulerCommand returns a command to add a evict-leader-scheduler.
func NewEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "add a scheduler to evict leader from a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

func checkSchedulerExist(cmd *cobra.Command, schedulerName string) (bool, error) {
	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return false, err
	}
	var schedulerList []string
	json.Unmarshal([]byte(r), &schedulerList)
	for idx := range schedulerList {
		if strings.Contains(schedulerList[idx], schedulerName) {
			return true, nil
		}
	}
	return false, nil
}

func addSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	// we should ensure whether it is the first time to create evict-leader-scheduler
	// or just update the evict-leader. But is add one ttl time.
	switch cmd.Name() {
	case evictLeaderSchedulerName, grantLeaderSchedulerName:
		exist, err := checkSchedulerExist(cmd, cmd.Name())
		if err != nil {
			return
		}
		if exist {
			addStoreToSchedulerConfig(cmd, cmd.Name(), args)
			return
		}
		fallthrough
	default:
		storeID, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println(err)
			return
		}

		input := make(map[string]any)
		input["name"] = cmd.Name()
		input["store_id"] = storeID
		postJSON(cmd, schedulersPrefix, input)
	}
}

// NewShuffleLeaderSchedulerCommand returns a command to add a shuffle-leader-scheduler.
func NewShuffleLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-leader-scheduler",
		Short: "add a scheduler to shuffle leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleRegionSchedulerCommand returns a command to add a shuffle-region-scheduler.
func NewShuffleRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-region-scheduler",
		Short: "add a scheduler to shuffle regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleHotRegionSchedulerCommand returns a command to add a shuffle-hot-region-scheduler.
func NewShuffleHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-hot-region-scheduler [limit]",
		Short: "add a scheduler to shuffle hot regions",
		Run:   addSchedulerForShuffleHotRegionCommandFunc,
	}
	return c
}

func addSchedulerForShuffleHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	limit := uint64(1)
	if len(args) == 1 {
		l, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println("Error: ", err)
			return
		}
		limit = l
	}
	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["limit"] = limit
	postJSON(cmd, schedulersPrefix, input)
}

// NewBalanceLeaderSchedulerCommand returns a command to add a balance-leader-scheduler.
func NewBalanceLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-leader-scheduler",
		Short: "add a scheduler to balance leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewEvictSlowStoreSchedulerCommand returns a command to add a evict-slow-store-scheduler.
func NewEvictSlowStoreSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-slow-store-scheduler",
		Short: "add a scheduler to detect and evict slow stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceRegionSchedulerCommand returns a command to add a balance-region-scheduler.
func NewBalanceRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-region-scheduler",
		Short: "add a scheduler to balance regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceHotRegionSchedulerCommand returns a command to add a balance-hot-region-scheduler.
func NewBalanceHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-hot-region-scheduler",
		Short: "add a scheduler to balance hot regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewRandomMergeSchedulerCommand returns a command to add a random-merge-scheduler.
func NewRandomMergeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "random-merge-scheduler",
		Short: "add a scheduler to merge regions randomly",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewLabelSchedulerCommand returns a command to add a label-scheduler.
func NewLabelSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "label-scheduler",
		Short: "add a scheduler to schedule regions according to the label",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewSplitBucketSchedulerCommand returns a command to add a split-bucket-scheduler.
func NewSplitBucketSchedulerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "split-bucket-scheduler",
		Short: "add a scheduler to split bucket",
		Run:   addSchedulerForSplitBucketCommandFunc,
	}
	return cmd
}

// NewGrantHotRegionSchedulerCommand returns a command to add a grant-hot-region-scheduler.
func NewGrantHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "grant-hot-region-scheduler <store_leader_id> <store_follower_id_1,store_follower_id_2>",
		Short: `add a scheduler to grant hot region to fixed stores. 
				Note: If there is balance-hot-region-scheduler running, please remove it first, otherwise grant-hot-region-scheduler will not work.`,
		Run: addSchedulerForGrantHotRegionCommandFunc,
	}
	return c
}

// NewBalanceWitnessSchedulerCommand returns a command to add a balance-witness-scheduler.
func NewBalanceWitnessSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-witness-scheduler",
		Short: "add a scheduler to balance witness",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewTransferWitnessLeaderSchedulerCommand returns a command to add a transfer-witness-leader-shceudler.
func NewTransferWitnessLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-witness-leader-scheduler",
		Short: "add a scheduler to transfer witness leader",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewSlowTrendEvictLeaderSchedulerCommand returns a command to add a evict-slow-trend-scheduler.
func NewSlowTrendEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-slow-trend-scheduler",
		Short: "add a scheduler to detect and evict slow stores by trend",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

func addSchedulerForSplitBucketCommandFunc(cmd *cobra.Command, _ []string) {
	input := make(map[string]any)
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

func addSchedulerForGrantHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["store-leader-id"] = args[0]
	input["store-id"] = args[1]
	postJSON(cmd, schedulersPrefix, input)
}

func addSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]any)
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

// NewScatterRangeSchedulerCommand returns a command to add a scatter-range-scheduler.
func NewScatterRangeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:        "scatter-range [--format=raw|encode|hex] <start_key> <end_key> <range_name>",
		Run:        addSchedulerForScatterRangeCommandFunc,
		Deprecated: "scatter-range will be deprecated in the future, please use `scatter-range-scheduler` instead",
	}
	c.Flags().String("format", "hex", "the key format")
	return c
}

// NewScatterRangeSchedulerCommandV2 returns a command to add a scatter-range-scheduler.
func NewScatterRangeSchedulerCommandV2() *cobra.Command {
	c := &cobra.Command{
		Use:   "scatter-range-scheduler [--format=raw|encode|hex] <start_key> <end_key> <range_name>",
		Short: "add a scheduler to scatter range",
		Run:   addSchedulerForScatterRangeCommandFunc,
	}
	c.Flags().String("format", "hex", "the key format")
	return c
}

func addSchedulerForScatterRangeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	startKey, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	endKey, err := parseKey(cmd.Flags(), args[1])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["start_key"] = url.QueryEscape(startKey)
	input["end_key"] = url.QueryEscape(endKey)
	input["range_name"] = args[2]
	postJSON(cmd, schedulersPrefix, input)
}

// NewRemoveSchedulerCommand returns a command to remove scheduler.
func NewRemoveSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <scheduler>",
		Short: "remove a scheduler",
		Run:   removeSchedulerCommandFunc,
	}
	return c
}

func redirectRemoveSchedulerToDeleteConfig(cmd *cobra.Command, schedulerName string, args []string) {
	args = strings.Split(args[0], "-")
	args = args[len(args)-1:]
	deleteStoreFromSchedulerConfig(cmd, schedulerName, args)
}

func removeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	// FIXME: maybe there is a more graceful method to handler it
	switch {
	case strings.HasPrefix(args[0], evictLeaderSchedulerName) && args[0] != evictLeaderSchedulerName:
		redirectRemoveSchedulerToDeleteConfig(cmd, evictLeaderSchedulerName, args)
	case strings.HasPrefix(args[0], grantLeaderSchedulerName) && args[0] != grantLeaderSchedulerName:
		redirectRemoveSchedulerToDeleteConfig(cmd, grantLeaderSchedulerName, args)
	default:
		path := schedulersPrefix + "/" + getEscapedSchedulerName(args[0])
		_, err := doRequest(cmd, path, http.MethodDelete, http.Header{})
		if err != nil {
			cmd.Println(err)
			return
		}
		cmd.Println("Success!")
	}
}

// NewConfigSchedulerCommand returns commands to config scheduler.
func NewConfigSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "config a scheduler",
	}
	c.AddCommand(
		newConfigEvictLeaderCommand(),
		newConfigGrantLeaderCommand(),
		newConfigHotRegionCommand(),
		newConfigShuffleRegionCommand(),
		newConfigGrantHotRegionCommand(),
		newConfigBalanceLeaderCommand(),
		newSplitBucketCommand(),
		newConfigEvictSlowStoreCommand(),
		newConfigShuffleHotRegionSchedulerCommand(),
		newConfigEvictSlowTrendCommand(),
	)
	return c
}

func newConfigBalanceLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-leader-scheduler",
		Short: "balance-leader-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	c.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "show the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})

	return c
}

func newSplitBucketCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "split-bucket-scheduler",
		Short: "split-bucket-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	c.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "list the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})

	return c
}

func newConfigHotRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-hot-region-scheduler",
		Short: "balance-hot-region-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	// Deprecated: list command will be deprecated in future version, use show command instead.
	c.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "list the config item (will be deprecated in feature version, use show command instead)",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "show",
		Short: "list the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})

	return c
}

func newConfigEvictLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler",
		Short: "evict-leader-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}
	c.AddCommand(&cobra.Command{
		Use:   "add-store <store-id>",
		Short: "add a store to evict leader list",
		Run:   func(cmd *cobra.Command, args []string) { addStoreToSchedulerConfig(cmd, c.Name(), args) },
	}, &cobra.Command{
		Use:   "delete-store <store-id>",
		Short: "delete a store from evict leader list",
		Run:   func(cmd *cobra.Command, args []string) { deleteStoreFromSchedulerConfig(cmd, c.Name(), args) },
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})
	return c
}

func newConfigGrantLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-leader-scheduler",
		Short: "grant-leader-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}
	c.AddCommand(&cobra.Command{
		Use:   "add-store <store-id>",
		Short: "add a store to grant leader list",
		Run:   func(cmd *cobra.Command, args []string) { addStoreToSchedulerConfig(cmd, c.Name(), args) },
	}, &cobra.Command{
		Use:   "delete-store <store-id>",
		Short: "delete a store from grant leader list",
		Run:   func(cmd *cobra.Command, args []string) { deleteStoreFromSchedulerConfig(cmd, c.Name(), args) },
	})
	return c
}

func newConfigShuffleRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-region-scheduler",
		Short: "shuffle-region-scheduler config",
		Run:   showShuffleRegionSchedulerRolesCommandFunc,
	}
	c.AddCommand(&cobra.Command{
		Use:   "show-roles",
		Short: "show affected roles (leader, follower, learner)",
		Run:   showShuffleRegionSchedulerRolesCommandFunc,
	}, &cobra.Command{
		Use:   "set-roles [leader,][follower,][learner]",
		Short: "set affected roles",
		Run:   setShuffleRegionSchedulerRolesCommandFunc,
	})
	return c
}

func addStoreToSchedulerConfig(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	exist, err := checkSchedulerExist(cmd, schedulerName)
	if err != nil {
		return
	}
	if !exist {
		cmd.Printf("Unable to update config: scheduler %s does not exist.\n", schedulerName)
		return
	}

	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}
	input := make(map[string]any)
	input["name"] = schedulerName
	input["store_id"] = storeID

	postJSON(cmd, path.Join(schedulerConfigPrefix, schedulerName, "config"), input)
}

var hiddenHotConfig = []string{
	"max-zombie-rounds",
	"max-peer-number",
	"byte-rate-rank-step-ratio",
	"key-rate-rank-step-ratio",
	"query-rate-rank-step-ratio",
	"count-rank-step-ratio",
	"great-dec-ratio",
	"minor-dec-ratio",
	"enable-for-tiflash",
}

func listSchedulerConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	p := cmd.Name()
	if p == "list" || p == "show" {
		p = cmd.Parent().Name()
	}
	path := path.Join(schedulerConfigPrefix, p, "list")
	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			err = errors.New("[404] scheduler not found")
		}
		cmd.Println(err)
		return
	}
	if p == "balance-hot-region-scheduler" {
		schedulerConfig := make(map[string]any)
		err := json.Unmarshal([]byte(r), &schedulerConfig)
		if err != nil {
			cmd.Println(err)
			return
		}
		for _, config := range hiddenHotConfig {
			delete(schedulerConfig, config)
		}
		b, err := json.MarshalIndent(schedulerConfig, "", "  ")
		if err != nil {
			cmd.Println(err)
			return
		}
		r = string(b)
	}
	cmd.Println(r)
}

func newConfigGrantHotRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-hot-region-scheduler",
		Short: "grant-hot-region-scheduler config",
		Run:   showGrantHotRegionCommandFunc,
	}
	c.AddCommand(&cobra.Command{
		Use:   "set [leader] [peer,]",
		Short: "set store leader and peers",
		Run:   func(cmd *cobra.Command, args []string) { setGrantHotRegionCommandFunc(cmd, c.Name(), args) }},
	)
	return c
}

func showGrantHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	p := cmd.Name()
	path := path.Join(schedulerConfigPrefix, p, "list")
	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func setGrantHotRegionCommandFunc(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]any)
	input["store-leader-id"] = args[0]
	input["store-id"] = args[1]
	postJSON(cmd, path.Join(schedulerConfigPrefix, schedulerName, "config"), input)
}

func postSchedulerConfigCommandFunc(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	var val any
	input := make(map[string]any)
	key, value := args[0], args[1]
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	if schedulerName == "balance-hot-region-scheduler" && (key == "read-priorities" || key == "write-leader-priorities" || key == "write-peer-priorities") {
		input[key] = strings.Split(value, ",")
	} else {
		input[key] = val
	}
	postJSON(cmd, path.Join(schedulerConfigPrefix, schedulerName, "config"), input)
}

func deleteStoreFromSchedulerConfig(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	exist, err := checkSchedulerExist(cmd, schedulerName)
	if err != nil {
		return
	}
	if !exist {
		cmd.Printf("Unable to update config: scheduler %s does not exist.\n", schedulerName)
		return
	}

	path := path.Join(schedulerConfigPrefix, "/", schedulerName, "delete", args[0])
	_, err = doRequest(cmd, path, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

func showShuffleRegionSchedulerRolesCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	p := cmd.Name()
	if p == "show-roles" {
		p = cmd.Parent().Name()
	}
	url := path.Join(schedulerConfigPrefix, p, "list")
	r, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		// try to use old api
		var err2 error
		url := path.Join(schedulerConfigPrefix, p, "roles")
		r, err2 = doRequest(cmd, url, http.MethodGet, http.Header{})
		if err2 != nil {
			cmd.Println(err, err2)
			return
		}
	}
	cmd.Println(r)
}

func setShuffleRegionSchedulerRolesCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	var roles []string
	fields := strings.Split(strings.ToLower(args[0]), ",")
	for _, f := range fields {
		if f != "" {
			roles = append(roles, f)
		}
	}
	b, _ := json.Marshal(roles)
	path := path.Join(schedulerConfigPrefix, cmd.Parent().Name(), "roles")
	_, err := doRequest(cmd, path, http.MethodPost, http.Header{"Content-Type": {"application/json"}},
		WithBody(bytes.NewBuffer(b)))
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

func newConfigEvictSlowStoreCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-slow-store-scheduler",
		Short: "evict-slow-store-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	c.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "list the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})
	return c
}

func newConfigShuffleHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-hot-region-scheduler",
		Short: "shuffle-hot-region-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	c.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "list the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})
	return c
}

func newConfigEvictSlowTrendCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-slow-trend-scheduler",
		Short: "evict-slow-trend-scheduler config",
		Run:   listSchedulerConfigCommandFunc,
	}

	c.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "list the config item",
		Run:   listSchedulerConfigCommandFunc,
	}, &cobra.Command{
		Use:   "set <key> <value>",
		Short: "set the config item",
		Run:   func(cmd *cobra.Command, args []string) { postSchedulerConfigCommandFunc(cmd, c.Name(), args) },
	})
	return c
}

// NewDescribeSchedulerCommand returns command to describe the scheduler.
func NewDescribeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "describe",
		Short: "describe a scheduler",
	}
	c.AddCommand(
		newDescribeBalanceRegionCommand(),
		newDescribeBalanceLeaderCommand(),
	)
	return c
}

func newDescribeBalanceRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-region-scheduler",
		Short: "describe the balance-region-scheduler",
		Run:   describeSchedulerCommandFunc,
	}
	return c
}

func newDescribeBalanceLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-leader-scheduler",
		Short: "describe the balance-leader-scheduler",
		Run:   describeSchedulerCommandFunc,
	}
	return c
}

func describeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	schedulerName := cmd.Name()
	url := path.Join(schedulerDiagnosticPrefix, schedulerName)

	r, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}
