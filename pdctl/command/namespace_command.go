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
	"net/http"
)

const namespacePrefix = "pd/api/v1/namespace"

// NewNamespaceCommand return a namespace sub-command of rootCmd
func NewNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "namespace",
		Short: "show the namespace information",
		Run:   showNamespaceCommandFunc,
	}
	return cmd
}

func showNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, namespacePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the namespace information: %s\n", err)
		return
	}
	fmt.Println(r)
}
