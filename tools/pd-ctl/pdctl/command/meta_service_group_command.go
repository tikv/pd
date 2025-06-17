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
	"github.com/tikv/pd/server/apiv2/handlers"
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
	cmd.AddCommand(newAddMetaServiceGroupCommand())
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

func newAddMetaServiceGroupCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "add",
		Short: "add one or more meta-service groups",
		Run:   newAddMetaServiceGroupFunc,
	}
	r.Flags().StringArrayP(nmGroup, "g", nil, "meta-service group in format id=addr1,addr2,...")
	_ = r.MarkFlagRequired(nmGroup)
	return r
}

func newAddMetaServiceGroupFunc(cmd *cobra.Command, _ []string) {
	// Parse the new groups.
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
	var params []handlers.AddMetaServiceGroupRequest
	for _, group := range metaServiceGroups {
		parts := strings.SplitN(group, "=", 2)
		if len(parts) != 2 {
			cmd.PrintErrf("Invalid --group format: %q (expected id=addr1,addr2,...)\n", group)
			return
		}
		params = append(params, handlers.AddMetaServiceGroupRequest{
			ID:        strings.TrimSpace(parts[0]),
			Addresses: strings.TrimSpace(parts[1]),
		})
	}

	body, err := json.Marshal(params)
	if err != nil {
		cmd.PrintErrln("Failed to marshal request:", err)
		return
	}
	resp, err := doRequest(cmd, metaServiceGroupPrefix, http.MethodPost,
		http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.PrintErrln("Failed to add meta-service group:", err)
		return
	}

	cmd.Println(resp)
}
