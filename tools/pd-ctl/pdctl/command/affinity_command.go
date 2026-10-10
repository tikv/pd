// Copyright 2025 TiKV Project Authors.
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
	"strconv"
	"strings"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands",
		Long:              `Manage affinity groups for a single table by table ID and optional partition ID.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().Uint64("table-id", 0, "table ID for the affinity group")
	cmd.PersistentFlags().Uint64("partition-id", 0, "partition ID for partitioned tables")

	cmd.AddCommand(
		newAffinityShowCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityRebalanceCommand(),
	)
	return cmd
}

func newAffinityRebalanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebalance",
		Short: "preview or apply affinity balance for one table",
		Run:   affinityRebalanceCommandFunc,
	}
	cmd.Flags().Bool("apply", false, "apply the generated plan")
	cmd.Flags().Bool("force", false, "required when applying a rebalance plan")
	return cmd
}

type affinityRebalanceResult struct {
	GroupID   string   `json:"group_id"`
	OldLeader uint64   `json:"old_leader"`
	NewLeader uint64   `json:"new_leader"`
	OldVoters []uint64 `json:"old_voters"`
	NewVoters []uint64 `json:"new_voters"`
	Changed   bool     `json:"changed"`
	Applied   bool     `json:"applied"`
	Error     string   `json:"error,omitempty"`
}

func affinityRebalanceCommandFunc(cmd *cobra.Command, _ []string) {
	tableID, _ := cmd.Flags().GetUint64("table-id")
	if tableID == 0 {
		cmd.Println("--table-id is required")
		return
	}
	apply, _ := cmd.Flags().GetBool("apply")
	force, _ := cmd.Flags().GetBool("force")
	if apply && !force {
		cmd.Println("--force is required with --apply")
		return
	}

	groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get affinity groups: %v\n", err)
		return
	}
	stores, err := PDCli.GetStores(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get stores: %v\n", err)
		return
	}

	tableGroups := make([]*affinity.Group, 0)
	for _, state := range groups {
		if !isTableAffinityGroup(state.ID, tableID) {
			continue
		}
		tableGroups = append(tableGroups, &affinity.Group{
			ID:            state.ID,
			LeaderStoreID: state.LeaderStoreID,
			VoterStoreIDs: append([]uint64(nil), state.VoterStoreIDs...),
			BalancePolicy: affinity.BalancePolicy(state.BalancePolicy),
		})
	}
	if len(tableGroups) == 0 {
		cmd.Printf("no affinity groups found for table %d\n", tableID)
		return
	}

	availableStores := make([]uint64, 0, len(stores.Stores))
	for _, store := range stores.Stores {
		if (strings.EqualFold(store.Store.StateName, "up") || strings.EqualFold(store.Store.StateName, "serving")) && isTiKVStore(store) {
			availableStores = append(availableStores, uint64(store.Store.ID))
		}
	}
	replicaCount := 0
	for _, group := range tableGroups {
		if len(group.VoterStoreIDs) > 0 {
			replicaCount = len(group.VoterStoreIDs)
			break
		}
	}
	if replicaCount == 0 {
		cmd.Println("affinity groups do not have voter targets")
		return
	}
	plans, err := affinity.PlanBalance(tableGroups, affinity.BalanceOptions{
		Stores:       availableStores,
		ReplicaCount: replicaCount,
	})
	if err != nil {
		cmd.Printf("Failed to plan affinity rebalance: %v\n", err)
		return
	}

	byID := make(map[string]*affinity.Group, len(tableGroups))
	for _, group := range tableGroups {
		byID[group.ID] = group
	}
	result := make([]affinityRebalanceResult, 0, len(plans))
	for _, plan := range plans {
		old := byID[plan.GroupID]
		item := affinityRebalanceResult{
			GroupID:   plan.GroupID,
			OldLeader: old.LeaderStoreID,
			NewLeader: plan.LeaderStoreID,
			OldVoters: append([]uint64(nil), old.VoterStoreIDs...),
			NewVoters: append([]uint64(nil), plan.VoterStoreIDs...),
			Changed:   plan.Changed,
		}
		if apply && plan.Changed {
			_, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), plan.GroupID, plan.LeaderStoreID, plan.VoterStoreIDs)
			if err != nil {
				item.Error = err.Error()
			} else {
				item.Applied = true
			}
		}
		result = append(result, item)
		if item.Error != "" {
			break
		}
	}
	jsonPrint(cmd, result)
}

func isTiKVStore(store pdhttp.StoreInfo) bool {
	labels := make([]*metapb.StoreLabel, 0, len(store.Store.Labels))
	for _, label := range store.Store.Labels {
		labels = append(labels, &metapb.StoreLabel{Key: label.Key, Value: label.Value})
	}
	return core.NewStoreInfo(&metapb.Store{Id: uint64(store.Store.ID), Labels: labels}).IsTiKV()
}

func isTableAffinityGroup(groupID string, tableID uint64) bool {
	table := strconv.FormatUint(tableID, 10)
	return groupID == "_tidb_t_"+table || strings.HasPrefix(groupID, "_tidb_pt_"+table+"_")
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group state (all groups if no --table-id is specified)",
		Run:   affinityShowCommandFunc,
	}
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the affinity group for the target ID",
		Run:   affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "force delete the affinity group even if it has key ranges")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		Run:   affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) {
	tableID, _ := cmd.Flags().GetUint64("table-id")

	// If no table-id is provided, show all affinity groups
	if tableID == 0 {
		groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
		if err != nil {
			cmd.Printf("Failed to get affinity groups: %v\n", err)
			return
		}
		jsonPrint(cmd, groups)
		return
	}

	// Otherwise, show the specific affinity group
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	state, err := PDCli.GetAffinityGroup(cmd.Context(), groupID)
	if err != nil {
		cmd.Printf("Failed to show affinity group %s: %v\n", groupID, err)
		return
	}
	jsonPrint(cmd, state)
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) {
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	force, _ := cmd.Flags().GetBool("force")
	if err := PDCli.DeleteAffinityGroup(cmd.Context(), groupID, force); err != nil {
		cmd.Printf("Failed to delete affinity group: %v\n", err)
		return
	}
	cmd.Printf("Affinity group %s deleted\n", groupID)
}

func affinityUpdatePeersCommandFunc(cmd *cobra.Command, _ []string) {
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	leader, _ := cmd.Flags().GetUint64("leader")
	if leader == 0 {
		cmd.Println("leader is required")
		return
	}

	voterStr, _ := cmd.Flags().GetString("voters")
	voters, err := parseUint64List(voterStr)
	if err != nil {
		cmd.Printf("Failed to parse voters: %v\n", err)
		return
	}
	if len(voters) == 0 {
		cmd.Println("voters is required")
		return
	}

	state, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), groupID, leader, voters)
	if err != nil {
		cmd.Printf("Failed to update affinity group peers: %v\n", err)
		return
	}
	jsonPrint(cmd, state)
}

func getGroupID(cmd *cobra.Command) (string, error) {
	tableID, _ := cmd.Flags().GetUint64("table-id")
	if tableID == 0 {
		return "", errors.New("--table-id is required")
	}
	partitionID, _ := cmd.Flags().GetUint64("partition-id")
	groupID := FormatGroupID(tableID, partitionID)
	if err := affinity.ValidateGroupID(groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

// FormatGroupID builds the affinity group ID from table/partition IDs following TiDB convention.
func FormatGroupID(tableID, partitionID uint64) string {
	if partitionID > 0 {
		return fmt.Sprintf("_tidb_pt_%d_p%d", tableID, partitionID)
	}
	return fmt.Sprintf("_tidb_t_%d", tableID)
}

func parseUint64List(input string) ([]uint64, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	parts := strings.Split(input, ",")
	res := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		res = append(res, v)
	}
	return res, nil
}
