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
	"fmt"

	"github.com/spf13/cobra"
	"net/http"
	"io/ioutil"
	"encoding/json"
)

var TableIdCmd = NewTableIdCommand()

type tableInfo struct {
	Name string  `json:"name"`
	Id int64 `json:"id"`
}

func (t tableInfo) String() string {
	return fmt.Sprintf("tableInfo{name: %s, id: %d}", t.Name, t.Id)
}

// NewPingCommand return a ping subcommand of rootCmd
func NewTableIdCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "showTableId <tidb_addr> <db_name> <table_name>",
		Short: "show the table id given the table name",
		Run:   showTableIdCommandFunc,
	}
	return m
}

func showTableIdCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: showTableId <tidb_addr> <db_name> <table_name>")
		return

	}
	host:= args[0]
	dbName:= args[1]
	tableName:= args[2]

	if host == "" || dbName == "" || tableName == "" {
		fmt.Printf("host, dbName and tableName are all required, but now\n " +
			"host: %s\n dbName: %s\n tableName: %s\n", host, dbName, tableName)
		return
	}

	urlString := fmt.Sprintf("%s/tables/%s/%s/regions", host, dbName, tableName)

	fmt.Println("the url is", urlString)

	resp, err := http.Get(urlString)

	if err != nil {
		fmt.Println(err)
		return
	}

	// fmt.Println(resp)
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	var ti tableInfo
	err = json.Unmarshal(content, &ti)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(ti)
}
