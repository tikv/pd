// Copyright 2020 PingCAP, Inc.
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
	"path"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	componentConfigPrefix = "pd/api/v1/component"
)

// NewComponentCommand returns a component subcommand of rootCmd
func NewComponentCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "component <subcommand>",
		Short: "manipulate components' configs",
	}
	conf.AddCommand(NewShowComponentConfigCommand())
	conf.AddCommand(NewSetComponentConfigCommand())
	conf.AddCommand(NewDeleteComponentConfigCommand())
	conf.AddCommand(NewGetComponentIDCommand())
	return conf
}

// NewShowComponentConfigCommand returns a show subcommand of componentCmd.
func NewShowComponentConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show <component ID>",
		Short: "show component config with a given component ID (e.g. 127.0.0.1:20160)",
		Run:   showComponentConfigCommandFunc,
	}
	return sc
}

// NewDeleteComponentConfigCommand returns a delete subcommand of componentCmd.
func NewDeleteComponentConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "delete <component ID>",
		Short: "delete component config with a given component ID (e.g. 127.0.0.1:20160)",
		Run:   deleteComponentConfigCommandFunc,
	}
	return sc
}

// NewSetComponentConfigCommand return a set subcommand of componentCmd.
func NewSetComponentConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set [<component>|<component ID>] <option> <value>",
		Short: "set the component config (set option with value)",
		Run:   setComponentConfigCommandFunc,
	}
	return sc
}

// NewGetComponentIDCommand returns a id subcommand of componentCmd.
func NewGetComponentIDCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "ids <component>",
		Short: "get all component IDs with a given component (e.g. tikv)",
		Run:   getComponentIDCommandFunc,
	}
	return sc
}

func showComponentConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	prefix := path.Join(componentConfigPrefix, args[0])
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get component config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func deleteComponentConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	prefix := path.Join(componentConfigPrefix, args[0])
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to delete component config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func getComponentIDCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	prefix := path.Join(componentConfigPrefix, "ids", args[0])
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get component %s's id: %s\n", args[0], err)
		return
	}
	cmd.Println(r)
}

func postComponentConfigData(cmd *cobra.Command, componentInfo, key, value string) error {
	var val interface{}
	data := make(map[string]interface{})
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	data[key] = val
	data["componentInfo"] = componentInfo

	reqData, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	_, err = doRequest(cmd, componentConfigPrefix, http.MethodPost,
		WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func setComponentConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	componentInfo, opt, val := args[0], args[1], args[2]
	err := postComponentConfigData(cmd, componentInfo, opt, val)
	if err != nil {
		cmd.Printf("Failed to set component config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}
