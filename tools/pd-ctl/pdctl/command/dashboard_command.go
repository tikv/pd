// Copyright 2016 PingCAP, Inc.
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
	dashboardKvAuthPrefix = "pd/api/v1/dashboard/auth/kvmode"
)

// NewDashboardCommand New a dashboard subcommand of the rootCmd
func NewDashboardCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "dashboard",
		Short: "manipulate dashboard settings",
	}
	conf.AddCommand(NewClearKvModePassCommand())
	return conf
}

// NewResetKvModePassCommand returns a delete subcommand of dashboardCmd.
func NewResetKvModePassCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "reset <password>",
		Short: "reset kv mode password",
		Run:   resetKvModePassCommandFunc,
	}
	return sc
}

// NewClearKvModePassCommand returns a clear subcommand of dashboardCmd.
func NewClearKvModePassCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "clear",
		Short: "clear kv mode password",
		Run:   clearKvModePassCommandFunc,
	}
	return sc
}

func resetKvModePassCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	data := make(map[string]interface{})
	data["password"] = args[0]

	reqData, err := json.Marshal(&data)
	if err != nil {
		cmd.Printf("Failed to set kv mode password %s", err)
	}

	_, err = doRequest(cmd, dashboardKvAuthPrefix, http.MethodPost,
		WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		cmd.Printf("Failed to set kv mode password %s", err)
	}
	cmd.Println("Success!")
}

func clearKvModePassCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}

	_, err := doRequest(cmd, dashboardKvAuthPrefix, http.MethodDelete)
	if err != nil {
		cmd.Printf("Failed to clear kv mode auth config")
		return
	}
	cmd.Println("Success!")
}
