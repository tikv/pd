// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"github.com/spf13/cobra"
)

// NewHealthCommand return a health subcommand of rootCmd
func NewHealthCommand() *cobra.Command {
	m := &cobra.Command{
		Use:               "health",
		Short:             "show all node's health information of the PD cluster",
		PersistentPreRunE: requirePDClient,
		Run:               showHealthCommandFunc,
	}
	return m
}

func showHealthCommandFunc(cmd *cobra.Command, _ []string) {
	health, err := PDCli.GetHealthStatus(cmd.Context())
	if err != nil {
		cmd.Println(err)
		return
	}
	jsonPrint(cmd, health)
}
