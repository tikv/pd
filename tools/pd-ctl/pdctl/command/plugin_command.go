// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/spf13/cobra"
)

var (
	pluginPrefix = "pd/api/v1/plugin"
)

func loadPluginCommandFunc(cmd *cobra.Command, args []string) {
	sendPluginCommand(cmd, cluster.PluginLoad, args)
}

func unloadPluginCommandFunc(cmd *cobra.Command, args []string) {
	sendPluginCommand(cmd, cluster.PluginUnload, args)
}

func sendPluginCommand(cmd *cobra.Command, action string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	data := map[string]interface{}{
		"plugin-path": args[0],
	}
	reqData, err := json.Marshal(data)
	if err != nil {
		cmd.Println(err)
		return
	}
	switch action {
	case cluster.PluginLoad:
		_, err = doRequest(cmd, pluginPrefix, http.MethodPost, WithBody("application/json", bytes.NewBuffer(reqData)))
	case cluster.PluginUnload:
		_, err = doRequest(cmd, pluginPrefix, http.MethodDelete, WithBody("application/json", bytes.NewBuffer(reqData)))
	default:
		cmd.Printf("Unknown action %s\n", action)
		return
	}
	if err != nil {
		cmd.Printf("Failed to %s plugin %s: %s\n", action, args[0], err)
		return
	}
	cmd.Println("Success!")
}
