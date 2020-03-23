// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type commandDef struct {
	Short      string
	Long       string
	Deprecated string
	Run        func(cmd *cobra.Command, args []string)
	Flags      func(flag *pflag.FlagSet)
}

var commandDefs = map[string]commandDef{
	"config <subcommand>":                               {Short: "tune pd configs"},
	"config show":                                       {Short: "show replication and schedule config of PD", Run: showConfigCommandFunc},
	"config show all":                                   {Short: "show all config of PD", Run: showAllConfigCommandFunc},
	"config show schedule":                              {Short: "show schedule config of PD", Run: showScheduleConfigCommandFunc},
	"config show replication":                           {Short: "show replication config of PD", Run: showReplicationConfigCommandFunc},
	"config show label-property":                        {Short: "show label property config", Run: showLabelPropertyConfigCommandFunc},
	"config show cluster-version":                       {Short: "show the cluster version", Run: showClusterVersionCommandFunc},
	"config set <option> <value>":                       {Short: "set the option with value", Run: setConfigCommandFunc},
	"config set label-property <type> <key> <value>":    {Short: "set a label property config item", Run: setLabelPropertyConfigCommandFunc},
	"config set cluster-version <version>":              {Short: "set cluster version", Run: setClusterVersionCommandFunc},
	"config delete":                                     {Short: "delete the config option"},
	"config delete label-property <type> <key> <value>": {Short: "delete a label property config item", Run: deleteLabelPropertyConfigCommandFunc},

	"member":                                          {Short: "show the pd member status", Run: showMemberCommandFunc},
	"member leader <subcommand>":                      {Short: "leader commands"},
	"member leader show":                              {Short: "show the leader member status", Run: getLeaderMemberCommandFunc},
	"member leader resign":                            {Short: "resign current leader pd's leadership", Run: resignLeaderCommandFunc},
	"member leader transfer <member name>":            {Short: "transfer leadership to another pd", Run: transferPDLeaderCommandFunc},
	"member delete <subcommand>":                      {Short: "delete a member"},
	"member delete name <member name>":                {Short: "delete a member by name", Run: deleteMemberByIDCommandFunc},
	"member delete id <member id>":                    {Short: "delete a member by id", Run: deleteMemberByNameCommandFunc},
	"member leader_priority <member_name> <priority>": {Short: "set the member's priority to be elected as etcd leader", Run: setLeaderPriorityFunc},

	"store [subcommand]":                                      {Short: "manipulate or query stores", Run: showStoreCommandFunc, Flags: withJQFlag},
	"store delete <store_id>":                                 {Short: "delete the store", Run: deleteStoreCommandFunc},
	"store delete addr <address>":                             {Short: "delete store by its address", Run: deleteStoreCommandByAddrFunc},
	"store label <store_id> <key> <value> [<key> <value>]...": {Short: "set a store's label value", Run: labelStoreCommandFunc, Flags: withForceStoreLabelFlag},
	"store weight <store_id> <leader_weight> <region_weight>": {Short: "set a store's leader and region balance weight", Run: setStoreWeightCommandFunc},
	"store limit [<store_id>|<all> <rate>]":                   {Short: "set a store's rate limit", Run: storeLimitCommandFunc},
	"store remove-tombstone":                                  {Short: "remove tombstone record if only safe", Run: removeTombStoneCommandFunc},
	"store limit-scene [<scene> <rate>]":                      {Short: "show or set the limit value for a scene", Run: storeLimitSceneCommandFunc},

	"label":                      {Short: "show store labels", Run: showLabelsCommandFunc},
	"label store <name> [value]": {Short: "show the stores with specify label", Run: showLabelListStoresCommandFunc},

	`region [region_id] [-jq="<query string>"]`:                                                                {Short: "show the region status", Run: showRegionCommandFunc, Flags: withJQFlag},
	"region key [--format=raw|encode|hex] <key>":                                                               {Short: "show the region with key", Run: showRegionWithTableCommandFunc, Flags: withKeyFormatFlag},
	"region check [miss-peer|extra-peer|down-peer|pending-peer|offline-peer|empty-region|hist-size|hist-keys]": {Short: "show the region with check specific status", Run: showRegionWithCheckCommandFunc},
	"region sibling <region_id>":                                                                               {Short: "show the sibling regions of specific region", Run: showRegionWithSiblingCommandFunc},
	"region store <store_id>":                                                                                  {Short: "show the regions of a specific store", Run: showRegionWithStoreCommandFunc},
	"region startkey [--format=raw|encode|hex] <key> <limit>":                                                  {Short: "show regions from start key", Run: showRegionsFromStartKeyCommandFunc, Flags: withKeyFormatFlag},
	`regoin topread <limit> [--jq="<query string>"]`:                                                           {Short: "show regions with top read flow", Run: showRegionTopReadCommandFunc, Flags: withJQFlag},
	`region topwrite <limit> [--jq="<query string>"]`:                                                          {Short: "show regions with top write flow", Run: showRegionTopWriteCommandFunc, Flags: withJQFlag},
	`region topconfver <limit> [--jq="<query string>"]`:                                                        {Short: "show regions with top conf version", Run: showRegionTopConfVerCommandFunc, Flags: withJQFlag},
	`region topversion <limit> [--jq="<query string>"]`:                                                        {Short: "show regions with top version", Run: showRegionTopVersionCommandFunc, Flags: withJQFlag},
	`region topsize <limit> [--jq="<query string>"]`:                                                           {Short: "show regions with top size", Run: showRegionTopSizeCommandFunc, Flags: withJQFlag},
	`region scan [--jq="<query string>"]`:                                                                      {Short: "scan all regions", Run: scanRegionCommandFunc, Flags: withJQFlag},

	"scheduler":                 {Short: "scheduler commands"},
	"scheduler show":            {Short: "show schedulers", Run: showSchedulerCommandFunc},
	"scheduler add <scheduler>": {Short: "add a scheduler"},
	"scheduler add grant-leader-scheduler <store_id>":                                          {Short: "add a scheduler to grant leader to a store", Run: addSchedulerForStoreCommandFunc},
	"scheduler add evict-leader-scheduler <store_id>":                                          {Short: "add a scheduler to evict leader from a store", Run: addSchedulerForStoreCommandFunc},
	"scheduler add shuffle-leader-scheduler":                                                   {Short: "add a scheduler to shuffle leaders between stores", Run: addSchedulerCommandFunc},
	"scheduler add shuffle-region-scheduler":                                                   {Short: "add a scheduler to shuffle regions between stores", Run: addSchedulerCommandFunc},
	"scheduler add shuffle-hot-region-scheduler [limit]":                                       {Short: "add a scheduler to shuffle hot regions", Run: addSchedulerForShuffleHotRegionCommandFunc},
	"scheduler add scatter-range [--format=raw|encode|hex] <start_key> <end_key> <range_name>": {Short: "add a scheduler to scatter range", Run: addSchedulerForScatterRangeCommandFunc, Flags: withKeyFormatFlag},
	"scheduler add balance-leader-scheduler":                                                   {Short: "add a scheduler to balance leaders between stores", Run: addSchedulerCommandFunc},
	"scheduler add balance-region-scheduler":                                                   {Short: "add a scheduler to balance regions between stores", Run: addSchedulerCommandFunc},
	"scheduler add balance-hot-region-scheduler":                                               {Short: "add a scheduler to balance hot regions between stores", Run: addSchedulerCommandFunc},
	"scheduler add random-merge-scheduler":                                                     {Short: "add a scheduler to merge regions randomly", Run: addSchedulerCommandFunc},
	"scheduler add balance-adjacent-region-scheduler [leader_limit] [peer_limit]":              {Short: "add a scheduler to disperse adjacent regions on each store", Run: addSchedulerForBalanceAdjacentRegionCommandFunc},
	"scheduler add label-scheduler":                                                            {Short: "add a scheduler to schedule regions according to the label", Run: addSchedulerCommandFunc},
	"scheduler remove <scheduler>":                                                             {Short: "remove a scheduler", Run: removeSchedulerCommandFunc},
	"scheduler pause <scheduler> <delay>":                                                      {Short: "pause a scheduler", Run: pauseOrResumeSchedulerCommandFunc},
	"scheduler resume <scheduler>":                                                             {Short: "resume a scheduler", Run: pauseOrResumeSchedulerCommandFunc},
	"scheduler config":                                                                         {Short: "config a scheduler"},
	"scheduler config evict-leader-scheduler":                                                  {Short: "show evict-leader-scheduler config", Run: listSchedulerConfigCommandFunc},
	"scheduler config evict-leader-scheduler add-store <store-id>":                             {Short: "add a store to evict leader list", Run: addStoreToSchedulerConfigSub},
	"scheduler config evict-leader-scheduler delete-store <store-id>":                          {Short: "delete a store from evict leader list", Run: deleteStoreFromSchedulerConfigSub},
	"scheduler config grant-leader-scheduler":                                                  {Short: "show grant-leader-scheduler config", Run: listSchedulerConfigCommandFunc},
	"scheduler config grant-leader-scheduler add-store <store-id>":                             {Short: "add a store to grant leader list", Run: addStoreToSchedulerConfigSub},
	"scheduler config grant-leader-scheduler delete-store <store-id>":                          {Short: "delete a store from grant leader list", Run: deleteStoreFromSchedulerConfigSub},
	"scheduler config balance-hot-region-scheduler":                                            {Short: "show balance-hot-region-scheduler config", Run: listSchedulerConfigCommandFunc},
	"scheduler config balance-hot-region-scheduler list":                                       {Short: "list the config item", Run: listSchedulerConfigCommandFunc},
	"scheduler config balance-hot-region-scheduler set <key> <value>":                          {Short: "set the config item", Run: postSchedulerConfigCommandFuncSub},
	"scheduler config shuffle-region-scheduler":                                                {Short: "shuffle-region-scheduler config"},
	"scheduler config shuffle-region-scheduler show-roles":                                     {Short: "show affected roles (leader,follower,learner)", Run: showShuffleRegionSchedulerRolesCommandFunc},
	"scheduler config shuffle-region-scheduler set-roles [leader,][follower,][learner]":        {Short: "set affected roles", Run: setSuffleRegionSchedulerRolesCommandFunc},

	"operator":                   {Short: "operator commands"},
	"operator show [kind]":       {Short: "show operators", Run: showOperatorCommandFunc},
	"operator check [region id]": {Short: "checks the status of operator", Run: checkOperatorCommandFunc},
	"operator add <operator>":    {Short: "add an operator"},
	"operator add transfer-leader <region_id> <to_store_id>":               {Short: "transfer a region's leader to the specified store", Run: transferLeaderCommandFunc},
	"operator add transfer-region <region_id> <to_store_id>...":            {Short: "transfer a region's peers to the specified stores", Run: transferRegionCommandFunc},
	"operator add transfer-peer <region_id> <from_store_id> <to_store_id>": {Short: "transfer a region's peer from the specified store to another store", Run: transferPeerCommandFunc},
	"operator add add-peer <region_id> <to_store_id>":                      {Short: "add a region peer on specified store", Run: addPeerCommandFunc},
	"operator add add-learner <region_id> <to_store_id>":                   {Short: "add a region learner on specified store", Run: addLearnerCommandFunc},
	"operator add remove-peer <region_id> <from_store_id>":                 {Short: "remove a region peer on specified store", Run: removePeerCommandFunc},
	"operator add merge-region <source_region_id> <target_region_id>":      {Short: "merge source region into target reigon", Run: mergeRegionCommandFunc},
	"operator add split-region <region_id> [--policy=scan|approximate]":    {Short: "split a region", Run: splitRegionCommandFunc, Flags: withSplitPolicyFlag},
	"operator add scatter-region <region_id>":                              {Short: "usually used for a batch of adjacent regions", Long: "usually used for a batch of adjacent regions, for example, scatter the regions for 1 to 100, need to use the following commands in order: \"scatter-region 1; scatter-region 2; ...; scatter-region 100;\"", Run: scatterRegionCommandFunc},
	"operator remove <region_id>":                                          {Short: "remove the region operator", Run: removeOperatorCommandFunc},

	"hot":       {Short: "show the hotspot status of the cluster"},
	"hot write": {Short: "show the hot write regions", Run: showHotWriteRegionsCommandFunc},
	"hot read":  {Short: "show the hot read regions", Run: showHotReadRegionsCommandFunc},
	"hot store": {Short: "show the hot stores", Run: showHotStoresCommandFunc},

	"component <subcommand>":                                      {Short: "manipulate components' configs"},
	"component show <component ID>":                               {Short: "show component config with a given component ID (e.g. 127.0.0.1:20160)", Run: showComponentConfigCommandFunc},
	"component set [<component>|<component ID>] <option> <value>": {Short: "set the component config (set option with value)", Run: setComponentConfigCommandFunc},
	"component delete <component ID>":                             {Short: "delete component config with a given component ID (e.g. 127.0.0.1:20160)", Run: deleteComponentConfigCommandFunc},
	"component ids <component>":                                   {Short: "get all component IDs with a given component (e.g. tikv)", Run: getComponentIDCommandFunc},

	"plugin <subcommand>":         {Short: "plugin commands"},
	"plugin load <plugin_path>":   {Short: "load a plugin, path must begin with ./pd/plugin/", Run: loadPluginCommandFunc},
	"plugin unload <plugin_path>": {Short: "unload a plugin, path must begin with ./pd/plugin/", Run: unloadPluginCommandFunc},

	"cluster":                           {Short: "show the cluster information", Run: showClusterCommandFunc},
	"health":                            {Short: "show all node's health information of the pd cluster", Run: showHealthCommandFunc},
	"ping":                              {Short: "show the total time spend ping the pd", Run: showPingCommandFunc},
	"log [fatal|error|warn|info|debug]": {Short: "set log level", Run: logCommandFunc},
	"tso <timestamp>":                   {Short: "parse TSO to the system and logic time", Run: showTSOCommandFunc},
	"exit":                              {Short: "exit pdctl", Run: exitCommandFunc},

	// deprecated commands
	"stores [command] [flags]": {Deprecated: "use store command instead", Short: "store status", Flags: withJQFlag},
	"stores remove-tombstone":  {Deprecated: "use store remove-tombstone instead", Short: "remove tombstone record if only safe", Run: removeTombStoneCommandFunc},
	"stores set [limit]":       {Deprecated: "use store command instead", Short: "set stores"},
	"stores limit <rate>":      {Deprecated: "use store limit all <rate> instead", Short: "set all store's rate limit", Run: setAllLimitCommandFunc},
	"stores show [limit]":      {Deprecated: "use store [limit] instead", Short: "show the stores limits", Run: showStoresCommandFunc},
}

// config <subcommand> => "config", "", "config"
// config show => "config show", "config", "show"
// config set cluster-version <option> <value> => "config set cluster-version", "config set", "cluster-version"
func parseCommandKey(use string) (key, parentKey, cmd string) {
	if i := strings.IndexAny(use, "<["); i != -1 {
		use = strings.TrimSpace(use[:i])
	}
	if i := strings.LastIndexByte(use, ' '); i != -1 {
		return use, use[:i], use[i+1:]
	}
	return use, "", use
}

// SetupCommands creates all defined comands and add them to root.
func SetupCommands(root *cobra.Command) {
	m := map[string]*cobra.Command{"": root}
	for use, def := range commandDefs {
		key, _, cmd := parseCommandKey(use)
		c := &cobra.Command{
			Use:        cmd,
			Short:      def.Short,
			Long:       def.Long,
			Deprecated: def.Deprecated,
			Run:        def.Run,
		}
		if def.Flags != nil {
			def.Flags(c.Flags())
		}
		m[key] = c
	}
	for use := range commandDefs {
		key, parent, _ := parseCommandKey(use)
		m[parent].AddCommand(m[key])
	}
}
