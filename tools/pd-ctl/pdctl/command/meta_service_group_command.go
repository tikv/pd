// Copyright 2026 TiKV Project Authors.
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
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/pkg/keyspace"
)

const (
	metaServiceGroupPrefix = "pd/api/v2/meta-service-groups"
	nmGroup                = "group"
)

// NewMetaServiceGroupCommand returns a meta-service group subcommand of rootCmd.
func NewMetaServiceGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "meta-service-group <command> [flags]",
		Short: "meta-service group commands",
	}
	cmd.AddCommand(newListMetaServiceGroupCommand())
	cmd.AddCommand(newUpsertMetaServiceGroupCommand())
	cmd.AddCommand(newDeleteMetaServiceGroupCommand())
	cmd.AddCommand(newSetMetaServiceGroupEnabledCommand())
	return cmd
}

func newListMetaServiceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "list",
		Short: "list all meta-service groups and their current status",
		Run:   listMetaServiceGroupFunc,
	}
	return r
}

func listMetaServiceGroupFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}
	resp, err := doRequest(cmd, metaServiceGroupPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.PrintErrln("Failed to get the meta-service group information: ", err)
		return
	}
	cmd.Println(resp)
}

func newUpsertMetaServiceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "upsert",
		Short: "upsert one or more meta-service groups",
		Run:   newUpsertMetaServiceGroupFunc,
	}
	r.Flags().StringArrayP(nmGroup, "g", nil, "meta-service group in format id=addr1,addr2,... (for add/update)")
	_ = r.MarkFlagRequired(nmGroup)
	return r
}

func newUpsertMetaServiceGroupFunc(cmd *cobra.Command, _ []string) {
	metaServiceGroups, err := cmd.Flags().GetStringArray("group")
	if err != nil {
		cmd.PrintErrln("Failed to read --group flag:", err)
		return
	}
	if len(metaServiceGroups) == 0 {
		cmd.PrintErrln("At least one --group must be specified")
		cmd.Usage()
		return
	}
	patch := make(map[string]*string)
	for _, group := range metaServiceGroups {
		parts := strings.SplitN(group, "=", 2)
		if len(parts) != 2 {
			cmd.PrintErrf("Invalid --group format: %q (expected id=addr1,addr2,...)", group)
			return
		}
		id := strings.TrimSpace(parts[0])
		if id == "" {
			cmd.PrintErrf("Invalid --group: ID cannot be empty in %q", group)
			return
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			cmd.PrintErrf("Invalid --group: address cannot be empty in %q", group)
			return
		}
		for _, addrSegment := range strings.Split(addr, ",") {
			if strings.TrimSpace(addrSegment) == "" {
				cmd.PrintErrf("Invalid --group: address cannot contain empty segment in %q", group)
				return
			}
		}
		if _, exists := patch[id]; exists {
			cmd.PrintErrf("Invalid --group: duplicate ID %q after trimming", id)
			return
		}
		patch[id] = &addr
	}
	body, err := json.Marshal(patch)
	if err != nil {
		cmd.PrintErrln("Failed to marshal request:", err)
		return
	}
	resp, err := doRequest(cmd, metaServiceGroupPrefix, http.MethodPatch,
		http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.PrintErrln("Failed to update meta-service group:", err)
		return
	}
	cmd.Println(resp)
}

func newDeleteMetaServiceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "delete <id> [<id> ...]",
		Short: "delete one or more meta-service groups by id",
		Args:  cobra.MinimumNArgs(1),
		Run:   newDeleteMetaServiceGroupFunc,
	}
	return r
}

func newDeleteMetaServiceGroupFunc(cmd *cobra.Command, args []string) {
	patch := make(map[string]*string)
	for _, id := range args {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == "" {
			cmd.PrintErrf("Invalid ID: cannot be empty or whitespace-only")
			return
		}
		patch[trimmedID] = nil
	}
	body, err := json.Marshal(patch)
	if err != nil {
		cmd.PrintErrln("Failed to marshal request:", err)
		return
	}
	resp, err := doRequest(cmd, metaServiceGroupPrefix, http.MethodPatch,
		http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.PrintErrln("Failed to delete meta-service group:", err)
		return
	}
	cmd.Println(resp)
}

func newSetMetaServiceGroupEnabledCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "set-enabled <id> <true|false>",
		Short: "enable or disable a meta-service group",
		Args:  cobra.ExactArgs(2),
		Run:   newSetMetaServiceGroupEnabledFunc,
	}
	return r
}

func newSetMetaServiceGroupEnabledFunc(cmd *cobra.Command, args []string) {
	groupID := strings.TrimSpace(args[0])
	enabled, err := strconv.ParseBool(strings.ToLower(args[1]))
	if err != nil {
		cmd.PrintErrln("Invalid value for enabled flag, must be true or false:", err)
		return
	}
	patch := &keyspace.MetaServiceGroupStatusPatch{
		Enabled: &enabled,
	}
	body, err := json.Marshal(patch)
	if err != nil {
		cmd.PrintErrln("Failed to marshal request:", err)
		return
	}
	resp, err := doRequest(cmd, metaServiceGroupPrefix+"/"+url.PathEscape(groupID)+"/status", http.MethodPatch,
		http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.PrintErrln("Failed to set meta-service group enabled status:", err)
		return
	}
	cmd.Println(resp)
}
