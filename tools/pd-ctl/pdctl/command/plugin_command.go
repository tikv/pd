// Copyright 2019 TiKV Project Authors.
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

	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/schedule"
)

var (
	pluginPrefix = "pd/api/v1/plugin"
)

// NewPluginCommand a set subcommand of plugin command
func NewPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "plugin <subcommand>",
		Short: "plugin commands",
	}
	r.AddCommand(NewLoadPluginCommand())
	r.AddCommand(NewUnloadPluginCommand())
	return r
}

// NewLoadPluginCommand return a load subcommand of plugin command
func NewLoadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "load <plugin_path>",
		Short: "load a plugin, path must begin with ./pd/plugin/",
		Run:   loadPluginCommandFunc,
	}
	return r
}

// NewUnloadPluginCommand return a unload subcommand of plugin command
func NewUnloadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "unload <plugin_path>",
		Short: "unload a plugin, path must begin with ./pd/plugin/",
		Run:   unloadPluginCommandFunc,
	}
	return r
}

func loadPluginCommandFunc(cmd *cobra.Command, args []string) {
	sendPluginCommand(cmd, schedule.PluginLoad, args)
}

func unloadPluginCommandFunc(cmd *cobra.Command, args []string) {
	sendPluginCommand(cmd, schedule.PluginUnload, args)
}

func sendPluginCommand(cmd *cobra.Command, action string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	data := map[string]any{
		"plugin-path": args[0],
	}
	reqData, err := json.Marshal(data)
	if err != nil {
		cmd.Println(err)
		return
	}
	switch action {
	case schedule.PluginLoad:
		_, err = doRequest(cmd, pluginPrefix, http.MethodPost, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(reqData)))
	case schedule.PluginUnload:
		_, err = doRequest(cmd, pluginPrefix, http.MethodDelete, http.Header{"Content-Type": {"application/json"}}, WithBody(bytes.NewBuffer(reqData)))
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
