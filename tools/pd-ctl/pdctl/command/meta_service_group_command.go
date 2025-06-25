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
	"encoding/json"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
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
	cmd.AddCommand(newUpdateMetaServiceGroupCommand())
	cmd.AddCommand(newDeleteMetaServiceGroupCommand())
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

func newUpdateMetaServiceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update",
		Short: "add or update one or more meta-service groups",
		Run:   newUpdateMetaServiceGroupFunc,
	}
	r.Flags().StringArrayP(nmGroup, "g", nil, "meta-service group in format id=addr1,addr2,... (for add/update)")
	_ = r.MarkFlagRequired(nmGroup)
	return r
}

func newUpdateMetaServiceGroupFunc(cmd *cobra.Command, _ []string) {
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
		addr := strings.TrimSpace(parts[1])
		patch[strings.TrimSpace(parts[0])] = &addr
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
		patch[strings.TrimSpace(id)] = nil
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
