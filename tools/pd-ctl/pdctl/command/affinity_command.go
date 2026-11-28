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
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/constants"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

const tidbGroupIDPrefix = "_tidb_"

var (
	rawGroupIDRegexp = regexp.MustCompile(`^[-A-Za-z0-9_]{1,64}$`)
	// tidbObjectGroupIDRegexp matches the IDs of TiDB table and partition groups,
	// with or without a keyspace.
	tidbObjectGroupIDRegexp = regexp.MustCompile(`^_tidb_(?:ks(0|[1-9][0-9]*)_)?(?:t_([1-9][0-9]*)|pt_([1-9][0-9]*)_p([1-9][0-9]*))$`)
	// tidbTableGroupIDRegexp matches the IDs of the groups of TiDB affinity table groups.
	tidbTableGroupIDRegexp = regexp.MustCompile(`^_tidb_(?:ks[0-9]+_)?tg_`)
)

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "affinity",
		Short: "affinity group commands",
		Long: `Manage affinity groups. Select a group by its raw ID with --group-id, or by table ID, optional partition ID, and optional keyspace ID.
IDs that start with _tidb_ are reserved for TiDB. Creating a group in PD does not set the table's AFFINITY attribute in TiDB;
see create --help for lifecycle details.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().String("group-id", "", "raw affinity group ID; cannot be used with --table-id, --partition-id or --keyspace-id")
	cmd.PersistentFlags().Uint64("table-id", 0, "table ID for the affinity group")
	cmd.PersistentFlags().Uint64("partition-id", 0, "partition ID for partitioned tables")
	cmd.PersistentFlags().Uint32("keyspace-id", 0, "keyspace ID (omit for classical cluster groups)")

	cmd.AddCommand(
		newAffinityShowCommand(),
		newAffinityCreateCommand(),
		newAffinityRangeCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityUpdatePolicyCommand(),
	)
	return cmd
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group state",
		Long: `Show all affinity groups when no group is selected, or the groups whose IDs start with --prefix.
Selecting no group means omitting --group-id, --partition-id and --keyspace-id, and omitting --table-id or setting it to 0.`,
		Args: cobra.NoArgs,
		RunE: affinityShowCommandFunc,
	}
	cmd.Flags().String("prefix", "", "show the groups whose IDs start with the prefix, such as _tidb_pt_100_p or _tidb_ks1_; end it with a separator to avoid matching longer IDs")
	return cmd
}

func newAffinityCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create an affinity group with key ranges",
		Long: `Create an affinity group.
A group with a custom ID needs --range. In Classic, either endpoint may be empty, and a range with both endpoints empty covers
all keys. In NextGen, both endpoints are required and all ranges of the group must lie in one transaction keyspace.

The key ranges of a TiDB table or partition group are derived from its ID, for example --table-id 100
--partition-id 101, or --group-id _tidb_pt_100_p101. Groups of TiDB affinity table groups are created by TiDB.

Creating a _tidb_ group in PD does not set the table's AFFINITY attribute in TiDB. For a table without that attribute,
SHOW AFFINITY does not list the group, and DROP TABLE does not delete it from PD. Manage such groups explicitly with pd-ctl,
including deletion after dropping their tables.`,
		Args: cobra.NoArgs,
		RunE: affinityCreateCommandFunc,
	}
	cmd.Flags().StringArray("range", nil, "key range as <start_key>,<end_key> in hex, encoded like Region keys; repeat the flag for more ranges")
	return cmd
}

func newAffinityRangeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "range",
		Short: "show or change the key ranges of an affinity group",
	}
	show := &cobra.Command{
		Use:   "show",
		Short: "show the key ranges of an affinity group",
		Args:  cobra.NoArgs,
		RunE:  affinityRangeShowCommandFunc,
	}
	add := &cobra.Command{
		Use:   "add",
		Short: "add key ranges to an affinity group",
		Long: `Add key ranges to an affinity group.
For a group with a custom ID in Classic, either endpoint may be empty, and a range with both endpoints empty covers all keys.
In NextGen, both endpoints are required and all ranges of the group, including existing ranges, must lie in one transaction
keyspace.`,
		Args: cobra.NoArgs,
		RunE: affinityRangeAddCommandFunc,
	}
	remove := &cobra.Command{
		Use:   "remove",
		Short: "remove key ranges from an affinity group",
		Args:  cobra.NoArgs,
		RunE:  affinityRangeRemoveCommandFunc,
	}
	for _, sub := range []*cobra.Command{add, remove} {
		sub.Flags().StringArray("range", nil, "key range as <start_key>,<end_key> in hex, encoded like Region keys; repeat the flag for more ranges")
		sub.Flags().Bool("force", false, "change the key ranges of a group whose ID starts with _tidb_, which TiDB manages")
	}
	cmd.AddCommand(show, add, remove)
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the affinity group for the target ID",
		Args:  cobra.NoArgs,
		RunE:  affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "delete the affinity group even if it has key ranges, and delete its label rule even if the group no longer exists")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		Args:  cobra.NoArgs,
		RunE:  affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func hasGroupSelector(cmd *cobra.Command) bool {
	tableID, _ := cmd.Flags().GetUint64("table-id")
	return tableID != 0 || cmd.Flags().Changed("group-id") ||
		cmd.Flags().Changed("partition-id") || cmd.Flags().Changed("keyspace-id")
}

func newAffinityUpdatePolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "set the balance policy for one or more affinity groups",
		Args:  cobra.NoArgs,
		RunE:  affinityUpdatePolicyCommandFunc,
	}
	cmd.Flags().String("balance-policy", "", "balance policy: auto or fixed")
	cmd.Flags().StringSlice("group-ids", nil, "comma separated group IDs for a batch update; cannot be used with a single group selector")
	_ = cmd.MarkFlagRequired("balance-policy")
	return cmd
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) error {
	if cmd.Flags().Changed("prefix") {
		if hasGroupSelector(cmd) {
			return errors.New("--prefix cannot be used with a group selector")
		}
		prefix, _ := cmd.Flags().GetString("prefix")
		if err := affinity.ValidateGroupIDPrefix(prefix); err != nil {
			return err
		}
		groups, err := PDCli.GetAffinityGroupsByPrefix(cmd.Context(), prefix)
		if err != nil {
			return errors.Annotate(err, "failed to get affinity groups")
		}
		// A PD without the filter returns every group.
		for id := range groups {
			if !strings.HasPrefix(id, prefix) {
				delete(groups, id)
			}
		}
		jsonPrint(cmd, groups)
		return nil
	}
	// Preserve zero as the legacy list-all sentinel unless a scope is selected.
	if !hasGroupSelector(cmd) {
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

func affinityCreateCommandFunc(cmd *cobra.Command, _ []string) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}
	if tidbTableGroupIDRegexp.MatchString(groupID) {
		return errors.Errorf("group %s belongs to a TiDB affinity table group, which TiDB creates; repair it with ADMIN REPAIR AFFINITY TABLE GROUP", groupID)
	}

	rawRanges, _ := cmd.Flags().GetStringArray("range")
	var ranges []pd.AffinityGroupKeyRange
	switch {
	case cmd.Flags().Changed("group-id") && !strings.HasPrefix(groupID, tidbGroupIDPrefix):
		if len(rawRanges) == 0 {
			return errors.New("--range is required for a group with a custom ID")
		}
		ranges, err = parseAffinityKeyRanges(rawRanges)
		if err != nil {
			return err
		}
	default:
		if len(rawRanges) > 0 {
			return errors.Errorf("the key range of TiDB group %s is derived from its ID; do not set --range", groupID)
		}
		keyRange, err := tidbGroupKeyRange(groupID)
		if err != nil {
			return err
		}
		ranges = []pd.AffinityGroupKeyRange{keyRange}
	}

	groups, err := PDCli.CreateAffinityGroups(cmd.Context(), map[string][]pd.AffinityGroupKeyRange{groupID: ranges})
	if err != nil {
		return errors.Annotatef(err, "failed to create affinity group %s", groupID)
	}
	jsonPrint(cmd, groups)
	return nil
}

// affinityGroupRanges is the output of range show.
type affinityGroupRanges struct {
	ID     string             `json:"id"`
	Ranges []affinityKeyRange `json:"ranges"`
}

type affinityKeyRange struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

func affinityRangeShowCommandFunc(cmd *cobra.Command, _ []string) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}
	result, err := getAffinityGroupRanges(cmd, groupID)
	if err != nil {
		return err
	}
	jsonPrint(cmd, result)
	return nil
}

// getAffinityGroupRanges returns the key ranges of a group, which PD keeps in
// the label rule of the group, with the keys in hex.
func getAffinityGroupRanges(cmd *cobra.Command, groupID string) (affinityGroupRanges, error) {
	ruleID := affinity.GetLabelRuleID(groupID)
	result := affinityGroupRanges{ID: groupID, Ranges: []affinityKeyRange{}}
	for attempt := range 2 {
		rules, err := PDCli.GetRegionLabelRulesByIDs(cmd.Context(), []string{ruleID})
		if err != nil {
			return affinityGroupRanges{}, errors.Annotatef(err, "failed to get the key ranges of affinity group %s", groupID)
		}
		for _, rule := range rules {
			if rule == nil || rule.ID != ruleID {
				continue
			}
			data, _ := rule.Data.([]any)
			for _, item := range data {
				keyRange, _ := item.(map[string]any)
				start, _ := keyRange["start_key"].(string)
				end, _ := keyRange["end_key"].(string)
				result.Ranges = append(result.Ranges, affinityKeyRange{StartKey: start, EndKey: end})
			}
			return result, nil
		}
		if attempt == 1 {
			break
		}
		// A group with ranges may have been created after the first rule lookup.
		state, err := PDCli.GetAffinityGroup(cmd.Context(), groupID)
		if err != nil {
			return affinityGroupRanges{}, errors.Annotatef(err, "failed to get the key ranges of affinity group %s", groupID)
		}
		if state.RangeCount == 0 {
			return result, nil
		}
	}
	return affinityGroupRanges{}, errors.Errorf("affinity group %s reports key ranges but its label rule is missing, please retry", groupID)
}

func affinityRangeAddCommandFunc(cmd *cobra.Command, _ []string) error {
	return changeAffinityGroupRanges(cmd, true)
}

func affinityRangeRemoveCommandFunc(cmd *cobra.Command, _ []string) error {
	return changeAffinityGroupRanges(cmd, false)
}

func changeAffinityGroupRanges(cmd *cobra.Command, add bool) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}
	if force, _ := cmd.Flags().GetBool("force"); !force && strings.HasPrefix(groupID, tidbGroupIDPrefix) {
		return errors.Errorf("group %s is managed by TiDB; use --force to change its key ranges", groupID)
	}
	rawRanges, _ := cmd.Flags().GetStringArray("range")
	if len(rawRanges) == 0 {
		return errors.New("--range is required")
	}
	ranges, err := parseAffinityKeyRanges(rawRanges)
	if err != nil {
		return err
	}
	change := map[string][]pd.AffinityGroupKeyRange{groupID: ranges}
	var groups map[string]*pd.AffinityGroupState
	if add {
		groups, err = PDCli.AddAffinityGroupKeyRanges(cmd.Context(), change)
	} else {
		groups, err = PDCli.RemoveAffinityGroupKeyRanges(cmd.Context(), change)
	}
	if err != nil {
		return errors.Annotatef(err, "failed to change the key ranges of affinity group %s", groupID)
	}
	jsonPrint(cmd, groups)
	return nil
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) error {
	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}

	force, _ := cmd.Flags().GetBool("force")
	if !force {
		if err := PDCli.DeleteAffinityGroup(cmd.Context(), groupID, false); err != nil {
			return errors.Annotate(err, "failed to delete affinity group")
		}
		cmd.Printf("Affinity group %s deleted\n", groupID)
		return nil
	}
	// The batch API deletes the label rule even if the group no longer exists,
	// which the single DELETE of older PD versions skips, so a label rule left
	// without its group is removed as well.
	if err := PDCli.BatchDeleteAffinityGroups(cmd.Context(), []string{groupID}, true); err != nil {
		return errors.Annotate(err, "failed to delete affinity group")
	}
	cmd.Printf("Affinity group %s and its label rule are deleted if they existed\n", groupID)
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

func affinityUpdatePolicyCommandFunc(cmd *cobra.Command, _ []string) error {
	policy, _ := cmd.Flags().GetString("balance-policy")
	if policy != string(affinity.BalancePolicyAuto) && policy != string(affinity.BalancePolicyFixed) {
		return errors.Errorf("balance-policy must be %q or %q", affinity.BalancePolicyAuto, affinity.BalancePolicyFixed)
	}
	if cmd.Flags().Changed("group-ids") {
		for _, name := range []string{"group-id", "table-id", "partition-id", "keyspace-id"} {
			if cmd.Flags().Changed(name) {
				return errors.Errorf("--group-ids cannot be used with --%s", name)
			}
		}
		groupIDs, err := cmd.Flags().GetStringSlice("group-ids")
		if err != nil {
			return err
		}
		if len(groupIDs) == 0 {
			return errors.New("--group-ids must specify at least one group ID")
		}
		states, err := PDCli.UpdateAffinityGroupPolicies(cmd.Context(), groupIDs, policy)
		if err != nil {
			return errors.Annotate(err, "failed to update affinity group balance policies")
		}
		jsonPrint(cmd, pd.AffinityGroupsResponse{AffinityGroups: states})
		return nil
	}

	groupID, err := getGroupID(cmd)
	if err != nil {
		return err
	}
	state, err := PDCli.UpdateAffinityGroupPolicy(cmd.Context(), groupID, policy)
	if err != nil {
		return errors.Annotate(err, "failed to update affinity group balance policy")
	}
	jsonPrint(cmd, state)
	return nil
}

func getGroupID(cmd *cobra.Command) (string, error) {
	if cmd.Flags().Changed("group-id") {
		for _, name := range []string{"table-id", "partition-id", "keyspace-id"} {
			if cmd.Flags().Changed(name) {
				return "", errors.Errorf("--group-id cannot be used with --%s", name)
			}
		}
		groupID, _ := cmd.Flags().GetString("group-id")
		if !rawGroupIDRegexp.MatchString(groupID) {
			return "", errors.Errorf("invalid --group-id %q: it must be 1 to 64 letters, digits, hyphens or underscores", groupID)
		}
		return groupID, nil
	}
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
	return formatAffinityGroupID(tableID, partitionID, keyspaceID), nil
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

// tidbGroupKeyRange returns the key range that TiDB gives the group of a table
// or partition: all keys of its physical ID, encoded like Region keys, under
// the transaction keyspace prefix when the ID has a keyspace.
func tidbGroupKeyRange(groupID string) (pd.AffinityGroupKeyRange, error) {
	parts := tidbObjectGroupIDRegexp.FindStringSubmatch(groupID)
	if parts == nil {
		return pd.AffinityGroupKeyRange{}, errors.Errorf("group %s is not a TiDB table or partition group; the %s prefix is reserved for TiDB", groupID, tidbGroupIDPrefix)
	}
	physicalID := parts[2]
	if parts[3] != "" {
		tableID, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil || tableID <= 0 {
			return pd.AffinityGroupKeyRange{}, errors.Errorf("invalid table ID %s in group %s", parts[3], groupID)
		}
		physicalID = parts[4]
	}
	id, err := strconv.ParseInt(physicalID, 10, 64)
	if err != nil || id == math.MaxInt64 {
		return pd.AffinityGroupKeyRange{}, errors.Errorf("invalid physical ID %s in group %s", physicalID, groupID)
	}
	var keyspacePrefix []byte
	if parts[1] != "" {
		keyspaceID, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil || keyspaceID > uint64(constants.MaxKeyspaceID) {
			return pd.AffinityGroupKeyRange{}, errors.Errorf("invalid keyspace ID %s in group %s", parts[1], groupID)
		}
		keyspacePrefix = codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, uint32(keyspaceID))
	}
	encode := func(physicalID int64) []byte {
		key := append(append([]byte{}, keyspacePrefix...), codec.GenerateTableKey(physicalID)...)
		return codec.EncodeBytes(key)
	}
	return pd.AffinityGroupKeyRange{StartKey: encode(id), EndKey: encode(id + 1)}, nil
}

// parseAffinityKeyRanges parses ranges given as <start_key>,<end_key> in hex.
// PD checks the ranges themselves, for example that start_key is less than end_key.
func parseAffinityKeyRanges(rawRanges []string) ([]pd.AffinityGroupKeyRange, error) {
	ranges := make([]pd.AffinityGroupKeyRange, 0, len(rawRanges))
	for _, raw := range rawRanges {
		start, end, ok := strings.Cut(raw, ",")
		if !ok || strings.Contains(end, ",") {
			return nil, errors.Errorf("invalid --range %q: it must be <start_key>,<end_key> in hex", raw)
		}
		startKey, err := hex.DecodeString(strings.TrimSpace(start))
		if err != nil {
			return nil, errors.Errorf("invalid start key in --range %q: %v", raw, err)
		}
		endKey, err := hex.DecodeString(strings.TrimSpace(end))
		if err != nil {
			return nil, errors.Errorf("invalid end key in --range %q: %v", raw, err)
		}
		ranges = append(ranges, pd.AffinityGroupKeyRange{StartKey: startKey, EndKey: endKey})
	}
	return ranges, nil
}

// formatAffinityKeyRange formats a key range as --range takes it.
func formatAffinityKeyRange(r pd.AffinityGroupKeyRange) string {
	return hex.EncodeToString(r.StartKey) + "," + hex.EncodeToString(r.EndKey)
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
