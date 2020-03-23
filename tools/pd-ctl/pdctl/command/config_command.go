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
	"path"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	configPrefix         = "pd/api/v1/config"
	schedulePrefix       = "pd/api/v1/config/schedule"
	replicationPrefix    = "pd/api/v1/config/replicate"
	labelPropertyPrefix  = "pd/api/v1/config/label-property"
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
)

func showConfigCommandFunc(cmd *cobra.Command, args []string) {
	allR, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	allData := make(map[string]interface{})
	err = json.Unmarshal([]byte(allR), &allData)
	if err != nil {
		cmd.Printf("Failed to unmarshal config: %s\n", err)
		return
	}

	data := make(map[string]interface{})
	data["replication"] = allData["replication"]
	scheduleConfig := make(map[string]interface{})
	scheduleConfigData, err := json.Marshal(allData["schedule"])
	if err != nil {
		cmd.Printf("Failed to marshal schedule config: %s\n", err)
		return
	}
	err = json.Unmarshal(scheduleConfigData, &scheduleConfig)
	if err != nil {
		cmd.Printf("Failed to unmarshal schedule config: %s\n", err)
		return
	}

	delete(scheduleConfig, "schedulers-v2")
	delete(scheduleConfig, "schedulers-payload")
	data["schedule"] = scheduleConfig
	r, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal config: %s\n", err)
		return
	}
	cmd.Println(string(r))
}

func showScheduleConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicationPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, labelPropertyPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get config: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, clusterVersionPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get cluster version: %s\n", err)
		return
	}
	cmd.Println(r)
}

func postConfigDataWithPath(cmd *cobra.Command, key, value, path string) error {
	var val interface{}
	data := make(map[string]interface{})
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	data[key] = val
	reqData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = doRequest(cmd, path, http.MethodPost,
		WithBody("application/json", bytes.NewBuffer(reqData)))
	if err != nil {
		return err
	}
	return nil
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	opt, val := args[0], args[1]
	err := postConfigDataWithPath(cmd, opt, val, configPrefix)
	if err != nil {
		cmd.Printf("Failed to set config: %s\n", err)
		return
	}
	cmd.Println("Success!")
}

func setLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	postLabelProperty(cmd, "set", args)
}

func deleteLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	postLabelProperty(cmd, "delete", args)
}

func postLabelProperty(cmd *cobra.Command, action string, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"type":        args[0],
		"action":      action,
		"label-key":   args[1],
		"label-value": args[2],
	}
	prefix := path.Join(labelPropertyPrefix)
	postJSON(cmd, prefix, input)
}

func setClusterVersionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"cluster-version": args[0],
	}
	postJSON(cmd, clusterVersionPrefix, input)
}
