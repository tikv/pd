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

import "github.com/spf13/cobra"

// NewClusterCommand return a cluster subcommand of rootCmd
func NewClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "cluster",
		Short:             "show the cluster information",
		PersistentPreRunE: requirePDClient,
		Run:               showClusterCommandFunc,
	}
	cmd.AddCommand(NewClusterStatusCommand())
	return cmd
}

// NewClusterStatusCommand return a cluster status subcommand of clusterCmd
func NewClusterStatusCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "status",
		Short: "show the cluster status",
		Run:   showClusterStatusCommandFunc,
	}
	return r
}

func showClusterCommandFunc(cmd *cobra.Command, _ []string) {
	info, err := PDCli.GetCluster(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get the cluster information: %s\n", err)
		return
	}
	jsonPrint(cmd, info)
}

func showClusterStatusCommandFunc(cmd *cobra.Command, _ []string) {
	status, err := PDCli.GetClusterStatus(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get the cluster status: %s\n", err)
		return
	}
	jsonPrint(cmd, status)
}
