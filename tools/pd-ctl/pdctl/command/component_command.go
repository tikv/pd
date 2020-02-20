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

// NewComponentConfigCommand returns a component subcommand of rootCmd
func NewComponentConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "component <subcommand>",
		Short: "manipulate components' configs",
	}
	conf.AddCommand(NewShowComponentConfigCommand())
	conf.AddCommand(NewSetComponentConfigCommand())
	return conf
}

// NewShowComponentConfigCommand returns a show subcommand of componentCmd.
func NewShowComponentConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show [<component>|<componentID>]",
		Short: "show component config",
		Run:   showComponentConfigCommandFunc,
	}
	return sc
}

// NewSetComponentConfigCommand return a set subcommand of componentCmd.
func NewSetComponentConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set [<component>|<componentID>] <option> <value>",
		Short: "set the option with value",
		Run:   setComponentConfigCommandFunc,
	}
	return sc
}

func showComponentConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	prefix := path.Join(componentConfigPrefix, args[0])
	r, err := doRequest(cmd, prefix, http.MethodGet, WithAccept("application/toml"))
	if err != nil {
		cmd.Printf("Failed to get component config: %s\n", err)
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
