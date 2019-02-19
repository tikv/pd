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
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/spf13/cobra"
)

// It is for query with --prefix
type parameter struct {
	Key       string `json:"key"`
	Range_end string `json:"range_end"`
}

var (
	rangeQueryPrefix       = "v3/kv/range"
	rangeDelPrefix         = "v3/kv/deleterange"
	delOwnerCampaignPrefix = "/tidb/ddl/fg/owner/"
	delSchemaVersionPrefix = "/tidb/ddl/all_schema_bersions/DDL_ID/"
)

func base64Encode(str string) string {
	return base64.StdEncoding.EncodeToString([]byte(str))
}

func base64Decode(str string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func formatJSON(str string) (string, error) {
	var t interface{}
	err := json.Unmarshal([]byte(str), &t)
	for _, v := range t.(map[string]interface{}) {
		for kk, vv := range v.(map[string]interface{}) {
			if kk == "key" || kk == "value" {
				vv, err = base64Decode(vv.(string))
				if err != nil {
					return "", err
				}
			}

		}
	}
	if err != nil {
		return "", err
	}
	resByte, err := json.MarshalIndent(&t, "", "\t")
	if err != nil {
		return "", err
	}
	res := string(resByte)
	return res, nil
}

var (
	rangeQueryDDLInfo = &parameter{
		Key:       base64Encode("/tidb/ddl"),
		Range_end: base64Encode("/tidb/ddm"),
	}
)

// NewEtcdCommand return a etcd subcommand of rootCmd
func NewEtcdCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "etcd",
		Short: "control the info about etcd by grpc_gateway",
	}
	m.AddCommand(NewShowDDLInfoCommand())
	m.AddCommand(NewDelOwnerCampaign())
	m.AddCommand(NewDelSchemaVersion())
	return m
}

func NewShowDDLInfoCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "ddlinfo",
		Short: "Show All Information about DDL",
		Run:   showDDLInfoCommandFunc,
	}
	return m
}

func NewDelOwnerCampaign() *cobra.Command {
	m := &cobra.Command{
		Use:   "delowner",
		Short: "delete DDL Owner Campaign by LeaseID",
		Run:   delOwnerCampaign,
	}
	return m
}

func NewDelSchemaVersion() *cobra.Command {
	m := &cobra.Command{
		Use:   "delschema",
		Short: "delete schema version by DDLID",
		Run:   delSchemaVersion,
	}
	return m
}

func showDDLInfoCommandFunc(cmd *cobra.Command, args []string) {
	reqData, _ := json.Marshal(rangeQueryDDLInfo)
	req, err := getRequest(cmd, rangeQueryPrefix, http.MethodPost, "application/json",
		bytes.NewBuffer(reqData))
	if err != nil {
		cmd.Printf("Failed to show DDLInfo: %v\n", err)
		return
	}
	res, err := dail(req)
	if err != nil {
		cmd.Printf("Failed to show DDLInfo: %v\n", err)
		return
	}

	res, err = formatJSON(res)
	if err != nil {
		cmd.Printf("Failed to show DDLInfo: %v\n", err)
		return
	}

	cmd.Println(res)
}

func delOwnerCampaign(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	leaseID := args[0]

	var para = &parameter{
		Key: base64Encode(delOwnerCampaignPrefix + leaseID),
	}

	reqData, _ := json.Marshal(para)
	req, err := getRequest(cmd, rangeDelPrefix, http.MethodPost, "application/json",
		bytes.NewBuffer(reqData))
	if err != nil {
		cmd.Printf("Failed to delete owner campaign : %v\n", err)
		return
	}
	res, err := dail(req)
	if err != nil {
		cmd.Printf("Failed to delete owner campaign : %v\n", err)
		return
	}

	res, err = formatJSON(res)
	if err != nil {
		cmd.Printf("Failed to delete schema version: %v\n", err)
		return
	}
	cmd.Println(res)
}

func delSchemaVersion(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	leaseID := args[0]

	var para = &parameter{
		Key: base64Encode(delSchemaVersionPrefix + leaseID),
	}

	reqData, _ := json.Marshal(para)
	req, err := getRequest(cmd, rangeDelPrefix, http.MethodPost, "application/json",
		bytes.NewBuffer(reqData))
	if err != nil {
		cmd.Printf("Failed to delete schema version: %v\n", err)
		return
	}
	res, err := dail(req)
	if err != nil {
		cmd.Printf("Failed to delete schema version: %v\n", err)
		return
	}

	res, err = formatJSON(res)
	if err != nil {
		cmd.Printf("Failed to delete schema version: %v\n", err)
		return
	}
	cmd.Println(res)
}
