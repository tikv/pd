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
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/pd/server"
	"github.com/spf13/cobra"
)

const (
	scheduleOpt = iota
	replicateOpt
)

type field struct {
	tag  string
	typ  reflect.Kind
	kind int
}

var (
	configPrefix    = "pd/api/v1/config"
	schedulePrefix  = "pd/api/v1/config/schedule"
	replicatePrefix = "pd/api/v1/config/replicate"
	optionRel       = make(map[string]field)
)

func init() {
	s := server.ScheduleConfig{}
	r := server.ReplicationConfig{}
	fs := dumpConfig(s)
	for _, f := range fs {
		f.kind = scheduleOpt
		optionRel[f.tag] = f
	}
	fs = dumpConfig(r)
	for _, f := range fs {
		f.kind = replicateOpt
		optionRel[f.tag] = f
	}
}

func dumpConfig(v interface{}) (ret []field) {
	var f field
	val := reflect.ValueOf(v)
	for i := 0; i < val.Type().NumField(); i++ {
		f.tag = val.Type().Field(i).Tag.Get("json")
		f.typ = val.Type().Field(i).Type.Kind()
		ret = append(ret, f)
	}
	return ret
}

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	conf.AddCommand(NewSetConfigCommand())
	return conf
}

// NewShowConfigCommand return a show subcommand of configCmd
func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show",
		Short: "show config of PD",
		Run:   showConfigCommandFunc,
	}
	sc.AddCommand(NewShowAllConfigCommand())
	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of PD",
		Run:   showAllConfigCommandFunc,
	}
	return sc
}

// NewSetConfigCommand return a set subcommand of configCmd
func NewSetConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set <option> <value>",
		Short: "set the option with value",
		Run:   setConfigCommandFunc,
	}
	return sc
}

func showConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s", err)
		return
	}
	fmt.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s", err)
		return
	}
	fmt.Println(r)
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}
	var (
		value interface{}
		path  string
	)
	opt, val := args[0], args[1]

	f, ok := optionRel[opt]
	if !ok {
		fmt.Println("Failed to set config: unknow option")
		return
	}
	switch f.kind {
	case scheduleOpt:
		path = schedulePrefix
	case replicateOpt:
		path = replicatePrefix
	}

	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	data := make(map[string]interface{})
	err = json.Unmarshal([]byte(r), &data)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	switch f.typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		value, err = strconv.ParseFloat(val, 64)
		if err != nil {
			fmt.Printf("Failed to set config: %s", err)
			return
		}
	case reflect.Slice:
		value = []string{val}
		if strings.Contains(val, ",") {
			value = strings.Split(val, ",")
		}
	default:
		value = val
	}
	data[opt] = value

	reqData, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	req, err := getRequest(cmd, path, http.MethodPost, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	_, err = dail(req)
	if err != nil {
		fmt.Printf("Failed to set config: %s", err)
		return
	}
	fmt.Println("Success!")
}
