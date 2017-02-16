// Copyright 2017 PingCAP, Inc.
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
)

var (
	labelsPrefix      = "pd/api/v1/labels"
	labelsStorePrefix = "pd/api/v1/labels/store"
)

// NewLabelCommand return a member subcommand of rootCmd
func NewLabelCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "label [store <name> <value>]",
		Short: "show the labels",
		Run:   showLabelsCommandFunc,
	}
	l.AddCommand(NewLabelListStoresCommand())
	return l
}

// NewLabelListStoresCommand return a label subcommand of labelCmd
func NewLabelListStoresCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "store <name> <value>",
		Short: "show the stores with specify label",
		Run:   showLabelListStoresCommandFunc,
	}
	l.Flags().Bool("name", false, "use regexp for name")
	l.Flags().Bool("value", false, "use regexp for value")
	return l
}

func showLabelsCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, labelsPrefix, "GET")
	if err != nil {
		fmt.Printf("Failed to get labels: %s", err)
		return
	}
	fmt.Println(r)
}

func showLabelListStoresCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: label store name value")
		return
	}
	isRegName, err := cmd.Flags().GetBool("name")
	if err != nil {
		fmt.Println("error: ", err.Error())
	}
	isRegValue, err := cmd.Flags().GetBool("value")
	if err != nil {
		fmt.Println("error: ", err.Error())
	}

	namePrefix := fmt.Sprintf("name=%s", args[0])
	valuePrefix := fmt.Sprintf("value=%s", args[1])
	if isRegName {
		namePrefix = fmt.Sprintf("name_re=%s", args[0])
	}
	if isRegValue {
		valuePrefix = fmt.Sprintf("value_re=%s", args[1])
	}
	prefix := fmt.Sprintf("%s?%s&%s", labelsStorePrefix, namePrefix, valuePrefix)
	r, err := doRequest(cmd, prefix, "GET")
	if err != nil {
		fmt.Printf("Failed to get stores through label: %s", err)
		return
	}
	fmt.Println(r)

}
