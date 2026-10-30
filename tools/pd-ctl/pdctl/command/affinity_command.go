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

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands",
		Long:              `Manage affinity groups by table ID, optional partition ID, and optional keyspace ID.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().Uint64("table-id", 0, "table ID for the affinity group")
	cmd.PersistentFlags().Uint64("partition-id", 0, "partition ID for partitioned tables")
	cmd.PersistentFlags().Uint32("keyspace-id", 0, "keyspace ID (omit for classical cluster groups)")

	cmd.AddCommand(
		newAffinityShowCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
	)
	return cmd
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group state",
		Long:  "Show all affinity groups when --table-id is omitted or 0 and neither --partition-id nor --keyspace-id is specified. Otherwise, a nonzero --table-id is required.",
		RunE:  affinityShowCommandFunc,
	}
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the affinity group for the target ID",
		RunE:  affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "force delete the affinity group even if it has key ranges")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		RunE:  affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) error {
	tableID, err := cmd.Flags().GetUint64("table-id")
	if err != nil {
		return err
	}
	// Preserve zero as the legacy list-all sentinel unless a scope is selected.
	if tableID == 0 && !cmd.Flags().Changed("partition-id") && !cmd.Flags().Changed("keyspace-id") {
		groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
		if err != nil {
			return errors.Annotate(err, "failed to get affinity groups")
		}
		jsonPrint(cmd, groups)
		return nil
	}

	// Otherwise, show the specific affinity group
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}

	state, err := PDCli.GetAffinityGroup(cmd.Context(), groupID)
	if err != nil {
		return errors.Annotatef(err, "failed to show affinity group %s", groupID)
	}
	jsonPrint(cmd, state)
	return nil
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}

	force, _ := cmd.Flags().GetBool("force")
	if err := PDCli.DeleteAffinityGroup(cmd.Context(), groupID, force); err != nil {
		return errors.Annotate(err, "failed to delete affinity group")
	}
	cmd.Printf("Affinity group %s deleted\n", groupID)
	return nil
}

func affinityUpdatePeersCommandFunc(cmd *cobra.Command, _ []string) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}

	leader, _ := cmd.Flags().GetUint64("leader")
	if leader == 0 {
		return errors.New("leader is required")
	}

	voterStr, _ := cmd.Flags().GetString("voters")
	voters, err := parseUint64List(voterStr)
	if err != nil {
		return errors.Annotate(err, "failed to parse voters")
	}
	if len(voters) == 0 {
		return errors.New("voters is required")
	}

	state, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), groupID, leader, voters)
	if err != nil {
		return errors.Annotate(err, "failed to update affinity group peers")
	}
	jsonPrint(cmd, state)
	return nil
}

func getGroupID(cmd *cobra.Command) (string, error) {
	tableID, _ := cmd.Flags().GetUint64("table-id")
	if tableID == 0 {
		return "", errors.New("--table-id is required")
	}
	partitionID, _ := cmd.Flags().GetUint64("partition-id")
	var keyspaceID *uint32
	if cmd.Flags().Changed("keyspace-id") {
		id, err := cmd.Flags().GetUint32("keyspace-id")
		if err != nil {
			return "", errors.Annotate(err, "failed to parse --keyspace-id")
		}
		if id > constants.MaxKeyspaceID {
			return "", errors.Errorf("--keyspace-id must be between %d and %d", constants.DefaultKeyspaceID, constants.MaxKeyspaceID)
		}
		keyspaceID = &id
	}
	groupID := formatAffinityGroupID(tableID, partitionID, keyspaceID)
	if err := affinity.ValidateGroupID(groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

// FormatGroupID builds the affinity group ID from table/partition IDs following TiDB convention.
func FormatGroupID(tableID, partitionID uint64) string {
	return formatAffinityGroupID(tableID, partitionID, nil)
}

func formatAffinityGroupID(tableID, partitionID uint64, keyspaceID *uint32) string {
	prefix := "_tidb_"
	if keyspaceID != nil {
		prefix = fmt.Sprintf("_tidb_ks%d_", *keyspaceID)
	}
	if partitionID > 0 {
		return fmt.Sprintf("%spt_%d_p%d", prefix, tableID, partitionID)
	}
	return fmt.Sprintf("%st_%d", prefix, tableID)
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
