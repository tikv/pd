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

	"github.com/spf13/cobra"
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
	r.AddCommand(NewUpdatePluginCommand())
	r.AddCommand(NewUnloadPluginCommand())
	return r
}

// NewLoadPluginCommand return a load subcommand of plugin command
func NewLoadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "load <plugin_path>",
		Short: "load a plugin",
		Run:   loadPluginCommandFunc,
	}
	return r
}

// NewUpdatePluginCommand return a update subcommand of plugin command
func NewUpdatePluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update <plugin_path>",
		Short: "update plugin",
		Run:   updatePluginCommandFunc,
	}
	return r
}

// NewUnloadPluginCommand return a unload subcommand of plugin command
func NewUnloadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "unload <plugin_path>",
		Short: "unload a plugin",
		Run:   unloadPluginCommandFunc,
	}
	return r
}

func loadPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
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
	_, err = doRequest(cmd, pluginPrefix, http.MethodPost, WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		cmd.Printf("Failed to load plugin %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func updatePluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
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
	_, err = doRequest(cmd, pluginPrefix, http.MethodPut, WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		cmd.Printf("Failed to load plugin %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}

func unloadPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
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
	_, err = doRequest(cmd, pluginPrefix, http.MethodDelete, WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		cmd.Printf("Failed to load plugin %s: %s\n", args[0], err)
		return
	}
	cmd.Println("Success!")
}
