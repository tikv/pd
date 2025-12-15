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
	"bytes"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pingcap/errors"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands",
		Long:              `Manage affinity groups by manually specifying group IDs and key ranges.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().String("group-id", "", "affinity group ID")

	cmd.AddCommand(
		newAffinityCreateCommand(),
		newAffinityShowCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityListCommand(),
	)
	return cmd
}

func newAffinityCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create an affinity group with manual key ranges",
		Run:   affinityCreateCommandFunc,
	}
	cmd.Flags().StringArray("range", nil, "key range in format start:end; repeat to add multiple ranges; use ':' for entire key space")
	cmd.Flags().String("format", "hex", "the key format (raw|encode|hex)")
	return cmd
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group state for the target ID",
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

func newAffinityListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list affinity groups",
		Run:   affinityListCommandFunc,
	}
	return cmd
}

func affinityCreateCommandFunc(cmd *cobra.Command, _ []string) {
	groupID, err := getGroupID(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}
	ranges, err := loadKeyRanges(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	resp, err := PDCli.CreateAffinityGroups(cmd.Context(), map[string][]pd.AffinityGroupKeyRange{
		groupID: ranges,
	})
	if err != nil {
		cmd.Printf("Failed to create affinity group: %v\n", err)
		return
	}
	jsonPrint(cmd, resp)
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) {
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

func affinityListCommandFunc(cmd *cobra.Command, _ []string) {
	groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get affinity groups: %v\n", err)
		return
	}
	jsonPrint(cmd, groups)
}

func getGroupID(cmd *cobra.Command) (string, error) {
	groupID, _ := cmd.Flags().GetString("group-id")
	if groupID == "" {
		return "", errors.New("--group-id is required")
	}
	if err := affinity.ValidateGroupID(groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

func loadKeyRanges(cmd *cobra.Command) ([]pd.AffinityGroupKeyRange, error) {
	specs, _ := cmd.Flags().GetStringArray("range")
	if len(specs) == 0 {
		return nil, errors.New("--range is required")
	}
	ranges := make([]pd.AffinityGroupKeyRange, 0, len(specs))
	for _, spec := range specs {
		kr, err := parseRangeSpec(cmd.Flags(), spec)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, kr)
	}
	return ranges, nil
}

func parseRangeSpec(flags *pflag.FlagSet, spec string) (pd.AffinityGroupKeyRange, error) {
	parts := strings.SplitN(strings.TrimSpace(spec), ":", 2)
	if len(parts) != 2 {
		return pd.AffinityGroupKeyRange{}, errors.New("invalid range format, expected start:end")
	}
	start, err := parseKey(flags, parts[0])
	if err != nil {
		return pd.AffinityGroupKeyRange{}, errors.Wrap(err, "failed to parse start key")
	}
	end, err := parseKey(flags, parts[1])
	if err != nil {
		return pd.AffinityGroupKeyRange{}, errors.Wrap(err, "failed to parse end key")
	}
	kr := pd.AffinityGroupKeyRange{
		StartKey: []byte(start),
		EndKey:   []byte(end),
	}
	if err := validateKeyRange(kr.StartKey, kr.EndKey); err != nil {
		return pd.AffinityGroupKeyRange{}, err
	}
	return kr, nil
}

// validateKeyRange mirrors the server-side validation logic to catch errors early.
func validateKeyRange(startKey, endKey []byte) error {
	if len(startKey) == 0 && len(endKey) == 0 {
		return nil
	}
	if len(startKey) == 0 || len(endKey) == 0 {
		return errors.New("key range must have both start_key and end_key, or both empty for entire key space")
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return errors.New("start_key must be less than end_key")
	}
	return nil
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
