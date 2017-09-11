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
	_ "net/http"
	_ "time"

	"github.com/spf13/cobra"
)

// NewPingCommand return a ping subcommand of rootCmd
func NewTableIdCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "show table id by table name",
		Short: "show the table id given the table name",
		Run:   showTableIdCommandFunc,
	}
	return m
}

func showTableIdCommandFunc(cmd *cobra.Command, args []string) {
	fmt.Println("This function is not implemented yet")
}
