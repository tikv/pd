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
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"
)

var (
	operatorsPrefix = "pd/api/v1/operators"
	peerRoles       = map[string]struct{}{
		"leader":   {},
		"voter":    {},
		"follower": {},
		"learner":  {},
	}
)

const (
	// HistoryExample history command example.
	HistoryExample = `
  If the timestamp is right, the the output will be like:

  [
    "admin-remove-peer {rm peer: store [2]} (kind:admin,region, region:1(1,1), createAt:2022-02-15 15:11:14.974435 +0800 
	CST m=+0.663988396, startAt:2022-02-15 15:11:14.974485 +0800 CST m=+0.664038719, currentStep:0, size:1, steps:[remove peer on store 2]) 
	(finishAt:2022-02-15 15:11:14.975531 +0800 CST m=+0.665084434, duration:1.045715ms)"
  ]
`
)

// NewOperatorCommand returns an operator command.
func NewOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "operator",
		Short: "operator commands",
	}
	c.AddCommand(NewShowOperatorCommand())
	c.AddCommand(NewCheckOperatorCommand())
	c.AddCommand(NewAddOperatorCommand())
	c.AddCommand(NewRemoveOperatorCommand())
	c.AddCommand(NewHistoryOperatorCommand())
	return c
}

// NewCheckOperatorCommand returns a command to show status of the operator.
func NewCheckOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "check [region_id]",
		Short: "checks the status of operator",
		Run:   checkOperatorCommandFunc,
	}
	return c
}

// NewShowOperatorCommand returns a command to show operators.
func NewShowOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show [kind]",
		Short: "show operators",
		Run:   showOperatorCommandFunc,
	}
	return c
}

func showOperatorCommandFunc(cmd *cobra.Command, args []string) {
	var path string
	if len(args) == 0 {
		path = operatorsPrefix
	} else if len(args) == 1 {
		path = fmt.Sprintf("%s?kind=%s", operatorsPrefix, args[0])
	} else {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func checkOperatorCommandFunc(cmd *cobra.Command, args []string) {
	var path string
	if len(args) == 0 {
		path = operatorsPrefix
	} else if len(args) == 1 {
		path = fmt.Sprintf("%s/%s", operatorsPrefix, args[0])
	} else {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

// NewAddOperatorCommand returns a command to add operators.
func NewAddOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <operator>",
		Short: "add an operator",
	}
	c.AddCommand(NewTransferLeaderCommand())
	c.AddCommand(NewTransferRegionCommand())
	c.AddCommand(NewTransferPeerCommand())
	c.AddCommand(NewAddPeerCommand())
	c.AddCommand(NewAddLearnerCommand())
	c.AddCommand(NewRemovePeerCommand())
	c.AddCommand(NewMergeRegionCommand())
	c.AddCommand(NewSplitRegionCommand())
	c.AddCommand(NewScatterRegionCommand())
	return c
}

// NewTransferLeaderCommand returns a command to transfer leader.
func NewTransferLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-leader <region_id> <to_store_id>",
		Short: "transfer a region's leader to the specified store",
		Run:   transferLeaderCommandFunc,
	}
	return c
}

func transferLeaderCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewTransferRegionCommand returns a command to transfer region.
func NewTransferRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-region <region_id> <to_store_id> [leader|voter|follower|learner] ...",
		Short: "transfer a region's peers to the specified stores",
		Run:   transferRegionCommandFunc,
	}
	return c
}

func transferRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) <= 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, roles, err := parseUit64sAndPeerRole(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	if len(roles) > 0 && len(roles)+1 != len(ids) {
		cmd.Println("peer role is not match with store")
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_ids"] = ids[1:]
	if len(roles) > 0 {
		input["peer_roles"] = roles
	}
	postJSON(cmd, operatorsPrefix, input)
}

// NewTransferPeerCommand returns a command to transfer region.
func NewTransferPeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-peer <region_id> <from_store_id> <to_store_id>",
		Short: "transfer a region's peer from the specified store to another store",
		Run:   transferPeerCommandFunc,
	}
	return c
}

func transferPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["from_store_id"] = ids[1]
	input["to_store_id"] = ids[2]
	postJSON(cmd, operatorsPrefix, input)
}

// NewAddPeerCommand returns a command to add region peer.
func NewAddPeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add-peer <region_id> <to_store_id>",
		Short: "add a region peer on specified store",
		Run:   addPeerCommandFunc,
	}
	return c
}

func addPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewAddLearnerCommand returns a command to add region learner.
func NewAddLearnerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add-learner <region_id> <to_store_id>",
		Short: "add a region learner on specified store",
		Run:   addLearnerCommandFunc,
	}
	return c
}

func addLearnerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewMergeRegionCommand returns a command to merge two regions.
func NewMergeRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "merge-region <source_region_id> <target_region_id>",
		Short: "merge source region into target region",
		Run:   mergeRegionCommandFunc,
	}
	return c
}

func mergeRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["source_region_id"] = ids[0]
	input["target_region_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewRemovePeerCommand returns a command to add region peer.
func NewRemovePeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove-peer <region_id> <from_store_id>",
		Short: "remove a region peer on specified store",
		Run:   removePeerCommandFunc,
	}
	return c
}

func removePeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewSplitRegionCommand returns a command to split a region.
func NewSplitRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "split-region <region_id> [--policy=scan|approximate|usekey] [--keys]",
		Short: "split a region",
		Run:   splitRegionCommandFunc,
	}
	c.Flags().String("policy", "scan", "the policy to get region split key")
	c.Flags().String("keys", "", "the split key, hex encoded")
	return c
}

func splitRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	policy := cmd.Flags().Lookup("policy").Value.String()
	switch policy {
	case "scan", "approximate", "usekey":
	default:
		cmd.Println("Error: unknown policy")
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["policy"] = policy
	keys := cmd.Flags().Lookup("keys").Value.String()
	if len(keys) > 0 {
		input["keys"] = []string{keys}
	}
	postJSON(cmd, operatorsPrefix, input)
}

// NewScatterRegionCommand returns a command to scatter a region.
func NewScatterRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scatter-region <region_id>",
		Short: "usually used for a batch of adjacent regions",
		Long:  "usually used for a batch of adjacent regions, for example, scatter the regions for 1 to 100, need to use the following commands in order: \"scatter-region 1; scatter-region 2; ...; scatter-region 100;\"",
		Run:   scatterRegionCommandFunc,
	}
	return c
}

func scatterRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]any)
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	postJSON(cmd, operatorsPrefix, input)
}

// NewRemoveOperatorCommand returns a command to remove operators.
func NewRemoveOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <region_id>",
		Short: "remove the region operator",
		Run:   removeOperatorCommandFunc,
	}
	return c
}

func removeOperatorCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	path := operatorsPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

// NewHistoryOperatorCommand returns a command to history finished operators.
func NewHistoryOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "history <start>",
		Short:   "list all finished operators since start, start is a timestamp",
		Run:     historyOperatorCommandFunc,
		Example: HistoryExample,
	}
	return c
}

func historyOperatorCommandFunc(cmd *cobra.Command, args []string) {
	path := operatorsPrefix + "/" + "records"
	if len(args) == 1 {
		path += "?from=" + args[0]
	}
	records, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(records)
}

func parseUint64s(args []string) ([]uint64, error) {
	results := make([]uint64, 0, len(args))
	for _, arg := range args {
		v, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		results = append(results, v)
	}
	return results, nil
}

func parseUit64sAndPeerRole(args []string) ([]uint64, []string, error) {
	if len(args) <= 2 {
		ids, err := parseUint64s(args)
		return ids, nil, err
	}
	if _, ok := peerRoles[args[2]]; !ok {
		ids, err := parseUint64s(args)
		return ids, nil, err
	}

	ids := make([]uint64, 0, len(args))
	roles := make([]string, 0, len(args))
	for idx, arg := range args {
		if idx == 0 || idx&1 == 1 {
			v, err := strconv.ParseUint(arg, 10, 64)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			ids = append(ids, v)
			continue
		}
		if _, ok := peerRoles[arg]; ok {
			roles = append(roles, arg)
		}
	}
	return ids, roles, nil
}
