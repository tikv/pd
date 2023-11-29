// Copyright 2023 TiKV Project Authors.
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
	"strings"

	"github.com/spf13/cobra"
	"github.com/tikv/pd/server/apiv2/handlers"
)

const (
	keyspacePrefix = "pd/api/v2/keyspaces"
	// flags
	nmConfig              = "config"
	nmLimit               = "limit"
	nmPageToken           = "page_token"
	nmRemove              = "remove"
	nmUpdate              = "update"
	nmForceRefreshGroupID = "force_refresh_group_id"
)

// NewKeyspaceCommand returns a keyspace subcommand of rootCmd.
func NewKeyspaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keyspace <command> [flags]",
		Short: "keyspace commands",
	}
	cmd.AddCommand(newShowKeyspaceCommand())
	cmd.AddCommand(newCreateKeyspaceCommand())
	cmd.AddCommand(newUpdateKeyspaceConfigCommand())
	cmd.AddCommand(newUpdateKeyspaceStateCommand())
	cmd.AddCommand(newListKeyspaceCommand())
	return cmd
}

func newShowKeyspaceCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "show",
		Short: "show keyspace metadata",
	}
	showByID := &cobra.Command{
		Use:   "id <id>",
		Short: "show keyspace metadata specified by keyspace id",
		Run:   showKeyspaceIDCommandFunc,
	}
	showByName := &cobra.Command{
		Use:   "name <name>",
		Short: "show keyspace metadata specified by keyspace name",
		Run:   showKeyspaceNameCommandFunc,
	}
	showByName.Flags().Bool(nmForceRefreshGroupID, true, "force refresh keyspace group id")
	r.AddCommand(showByID)
	r.AddCommand(showByName)
	return r
}

func showKeyspaceIDCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	resp, err := doRequest(cmd, fmt.Sprintf("%s/id/%s", keyspacePrefix, args[0]), http.MethodGet, http.Header{})
	if err != nil {
		cmd.PrintErrln("Failed to get the keyspace information: ", err)
		return
	}
	cmd.Println(resp)
}

func showKeyspaceNameCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	refreshGroupID, err := cmd.Flags().GetBool(nmForceRefreshGroupID)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	url := fmt.Sprintf("%s/%s", keyspacePrefix, args[0])
	if refreshGroupID {
		url += "?force_refresh_group_id=true"
	}
	resp, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	// Retry without the force_refresh_group_id if the keyspace group manager is not initialized.
	// This can happen when PD is not running in API mode.
	if err != nil && refreshGroupID && strings.Contains(err.Error(), handlers.GroupManagerUninitializedErr) {
		resp, err = doRequest(cmd, fmt.Sprintf("%s/%s", keyspacePrefix, args[0]), http.MethodGet, http.Header{})
	}
	if err != nil {
		cmd.PrintErrln("Failed to get the keyspace information: ", err)
		return
	}
	cmd.Println(resp)
}

func newCreateKeyspaceCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "create <keyspace-name> [flags]",
		Short: "create a keyspace",
		Run:   createKeyspaceCommandFunc,
	}
	r.Flags().StringSlice(nmConfig, nil, "keyspace configs for the new keyspace\n"+
		"specify as comma separated key value pairs, e.g. --config k1=v1,k2=v2")
	return r
}

func createKeyspaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	configPairs, err := cmd.Flags().GetStringSlice(nmConfig)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	config := map[string]string{}
	for _, flag := range configPairs {
		kvs := strings.Split(flag, ",")
		for _, kv := range kvs {
			pair := strings.Split(kv, "=")
			if len(pair) != 2 {
				cmd.PrintErrf("Failed to create keyspace: invalid kv pair %s\n", kv)
				return
			}
			if _, exist := config[pair[0]]; exist {
				cmd.PrintErrf("Failed to create keyspace: key %s is specified multiple times\n", pair[0])
				return
			}
			config[pair[0]] = pair[1]
		}
	}
	params := handlers.CreateKeyspaceParams{
		Name:   args[0],
		Config: config,
	}
	body, err := json.Marshal(params)
	if err != nil {
		cmd.PrintErrln("Failed to encode the request body: ", err)
		return
	}
	resp, err := doRequest(cmd, keyspacePrefix, http.MethodPost, http.Header{}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.PrintErrln("Failed to create the keyspace: ", err)
		return
	}
	cmd.Println(resp)
}

func newUpdateKeyspaceConfigCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update-config <keyspace-name>",
		Short: "update keyspace config",
		Run:   updateKeyspaceConfigCommandFunc,
	}
	r.Flags().StringSlice(nmRemove, nil, "keys to remove from keyspace config\n"+
		"specify as comma separated keys, e.g. --remove k1,k2")
	r.Flags().StringSlice(nmUpdate, nil, "kv pairs to upsert into keyspace config\n"+
		"specify as comma separated key value pairs, e.g. --update k1=v1,k2=v2")
	return r
}

func updateKeyspaceConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	configPatch := map[string]*string{}
	removeFlags, err := cmd.Flags().GetStringSlice(nmRemove)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	for _, flag := range removeFlags {
		keys := strings.Split(flag, ",")
		for _, key := range keys {
			if _, exist := configPatch[key]; exist {
				cmd.PrintErrf("Failed to update keyspace config: key %s is specified multiple times\n", key)
				return
			}
			configPatch[key] = nil
		}
	}
	updateFlags, err := cmd.Flags().GetStringSlice(nmUpdate)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	for _, flag := range updateFlags {
		kvs := strings.Split(flag, ",")
		for _, kv := range kvs {
			pair := strings.Split(kv, "=")
			if len(pair) != 2 {
				cmd.PrintErrf("Failed to update keyspace config: invalid kv pair %s\n", kv)
				return
			}
			if _, exist := configPatch[pair[0]]; exist {
				cmd.PrintErrf("Failed to update keyspace config: key %s is specified multiple times\n", pair[0])
				return
			}
			configPatch[pair[0]] = &pair[1]
		}
	}
	params := handlers.UpdateConfigParams{Config: configPatch}
	data, err := json.Marshal(params)
	if err != nil {
		cmd.PrintErrln("Failed to update keyspace config:", err)
		return
	}
	url := fmt.Sprintf("%s/%s/config", keyspacePrefix, args[0])
	resp, err := doRequest(cmd, url, http.MethodPatch, http.Header{}, WithBody(bytes.NewBuffer(data)))
	if err != nil {
		cmd.PrintErrln("Failed to update the keyspace config: ", err)
		return
	}
	cmd.Println(resp)
}

func newUpdateKeyspaceStateCommand() *cobra.Command {
	r := &cobra.Command{
		Use:  "update-state <keyspace-name> <state>",
		Long: "update keyspace state, state can be one of: ENABLED, DISABLED, ARCHIVED, TOMBSTONE",
		Run:  updateKeyspaceStateCommandFunc,
	}
	return r
}

func updateKeyspaceStateCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	params := handlers.UpdateStateParam{
		State: args[1],
	}
	data, err := json.Marshal(params)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}
	url := fmt.Sprintf("%s/%s/state", keyspacePrefix, args[0])
	resp, err := doRequest(cmd, url, http.MethodPut, http.Header{}, WithBody(bytes.NewBuffer(data)))
	if err != nil {
		cmd.PrintErrln("Failed to update the keyspace state: ", err)
		return
	}
	cmd.Println(resp)
}

func newListKeyspaceCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "list [flags]",
		Short: "list keyspaces according to filters",
		Run:   listKeyspaceCommandFunc,
	}
	r.Flags().String(nmLimit, "", "The maximum number of keyspace metas to return. If not set, no limit is posed.")
	r.Flags().String(nmPageToken, "", "The keyspace id of the scan start. If not set, scan from keyspace/keyspace group with id 0")
	return r
}

func listKeyspaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}

	url := keyspacePrefix
	limit, err := cmd.Flags().GetString(nmLimit)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	if limit != "" {
		url += fmt.Sprintf("?limit=%s", limit)
	}
	pageToken, err := cmd.Flags().GetString(nmPageToken)
	if err != nil {
		cmd.PrintErrln("Failed to parse flag: ", err)
		return
	}
	if pageToken != "" {
		url += fmt.Sprintf("&page_token=%s", pageToken)
	}
	resp, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		cmd.PrintErrln("Failed to list keyspace: ", err)
		return
	}
	cmd.Println(resp)
}
