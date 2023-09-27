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

	"github.com/spf13/cobra"
	"github.com/tikv/pd/server/apiv2/handlers"
)

const (
	keyspacePrefix = "pd/api/v2/keyspaces"
	// flags
	nmUseID     = "use-id"
	nmConfig    = "config"
	nmLimit     = "limit"
	nmPageToken = "page-token"
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
		Use:   "show <keyspace-name> [flags]",
		Short: "show keyspace metadata specified by keyspace name/id",
		Run:   showKeyspaceCommandFunc,
	}
	r.Flags().Bool(nmUseID, false, "use keyspace id instead of keyspace name")
	return r
}

func showKeyspaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	useID, err := cmd.Flags().GetBool(nmUseID)
	if err != nil {
		cmd.Println("Failed to parse flag: ", err)
		return
	}
	var url string
	if useID {
		url = fmt.Sprintf("%s/id/%s", keyspacePrefix, args[0])
	} else {
		url = fmt.Sprintf("%s/%s?force_refresh_group_id=true", keyspacePrefix, args[0])
	}
	resp, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the keyspace information: %s\n", err)
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
	r.Flags().String(nmConfig, "", "keyspace config, in json format")
	return r
}

type CreateKeyspaceParams struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

func createKeyspaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	configStr, err := cmd.Flags().GetString(nmConfig)
	if err != nil {
		cmd.Println("Failed to parse flag: ", err)
		return
	}
	var config map[string]string
	if err = json.Unmarshal([]byte(configStr), &config); err != nil {
		cmd.Println("Failed to parse flag: ", err)
		return
	}
	params := handlers.CreateKeyspaceParams{
		Name:   args[0],
		Config: config,
	}
	body, err := json.Marshal(params)
	if err != nil {
		cmd.Println(err)
		return
	}
	resp, err := doRequest(cmd, keyspacePrefix, http.MethodPost, http.Header{}, WithBody(bytes.NewBuffer(body)))
	if err != nil {
		cmd.Printf("Failed to create the keyspace: %s\n", err)
		return
	}
	cmd.Println(resp)
}

func newUpdateKeyspaceConfigCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update-config <keyspace-name> <config-patch>",
		Short: "update keyspace config, a json merge patch is expected",
		Run:   updateKeyspaceConfigCommandFunc,
	}
	return r
}

func updateKeyspaceConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}

	url := fmt.Sprintf("%s/%s/config", keyspacePrefix, args[0])
	resp, err := doRequest(cmd, url, http.MethodPatch, http.Header{}, WithBody(bytes.NewBufferString(args[1])))
	if err != nil {
		cmd.Printf("Failed to update the keyspace config: %s\n", err)
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
		cmd.Println(err)
		return
	}
	url := fmt.Sprintf("%s/%s/state", keyspacePrefix, args[0])
	resp, err := doRequest(cmd, url, http.MethodPut, http.Header{}, WithBody(bytes.NewBuffer(data)))
	if err != nil {
		cmd.Printf("Failed to update the keyspace state: %s\n", err)
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
	r.Flags().String("limit", "", "The maximum number of keyspace metas to return. If not set, no limit is posed.")
	r.Flags().String("page-token", "", "The keyspace id of the scan start. If not set, scan from keyspace/keyspace group with id 0")
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
		cmd.Println("Failed to parse flag: ", err)
		return
	}
	if limit != "" {
		url += fmt.Sprintf("?limit=%s", limit)
	}
	pageToken, err := cmd.Flags().GetString(nmPageToken)
	if err != nil {
		cmd.Println("Failed to parse flag: ", err)
		return
	}
	if pageToken != "" {
		url += fmt.Sprintf("&page-token=%s", pageToken)
	}
	resp, err := doRequest(cmd, url, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to list keyspace: %s\n", err)
		return
	}
	cmd.Println(resp)
}
