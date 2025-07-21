// Copyright 2025 TiKV Project Authors.
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

// Package command provides CLI commands for maintenance task management.
// This implementation follows the design specified in RFC-0118:
// https://github.com/tikv/rfcs/blob/master/text/0118-pd-maintenance-endpoints.md
//
// The CLI commands provide a user-friendly interface to manage maintenance tasks
// that serialize TiKV maintenance operations to prevent Raft quorum loss.

package command

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

const maintenancePrefixURI = "pd/api/v2/maintenance"

// NewMaintenanceCommand returns the maintenance subcommand for pd-ctl
func NewMaintenanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Manage cluster maintenance tasks",
	}

	cmd.AddCommand(newMaintenanceSetCommand())
	cmd.AddCommand(newMaintenanceShowCommand())
	cmd.AddCommand(newMaintenanceDeleteCommand())
	return cmd
}

func newMaintenanceSetCommand() *cobra.Command {
	var desc string
	cmd := &cobra.Command{
		Use:   "set <task_type> <task_id>",
		Short: "Start a maintenance task",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			taskType := args[0]
			taskID := args[1]
			path := fmt.Sprintf(maintenancePrefixURI+"/%s/%s", taskType, taskID)

			var resp string
			var err error
			if desc != "" {
				resp, err = doRequest(cmd, path, http.MethodPut, http.Header{"Content-Type": {"text/plain"}}, WithBody(strings.NewReader(desc)))
			} else {
				resp, err = doRequest(cmd, path, http.MethodPut, http.Header{})
			}

			if err != nil {
				cmd.Printf("Failed to start maintenance task: %s\n", err)
				return
			}

			// Print the raw response
			cmd.Println(resp)
		},
	}
	cmd.Flags().StringVar(&desc, "desc", "", "Description for the maintenance task")
	return cmd
}

func newMaintenanceShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show [task_type]",
		Short: "Show maintenance task(s)",
		Long:  "Show maintenance task(s). If no task_type is provided, shows all current maintenance tasks.",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			var path string
			if len(args) == 0 {
				path = maintenancePrefixURI
			} else {
				path = fmt.Sprintf(maintenancePrefixURI+"/%s", args[0])
			}

			resp, err := doRequest(cmd, path, http.MethodGet, http.Header{})
			if err != nil {
				cmd.Printf("Failed to get maintenance task: %s\n", err)
				return
			}

			// Print the raw response
			cmd.Println(resp)
		},
	}
	return cmd
}

func newMaintenanceDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <task_type> <task_id>",
		Short: "Delete a maintenance task",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			taskType := args[0]
			taskID := args[1]
			path := fmt.Sprintf(maintenancePrefixURI+"/%s/%s", taskType, taskID)

			resp, err := doRequest(cmd, path, http.MethodDelete, http.Header{})
			if err != nil {
				cmd.Printf("Failed to delete maintenance task: %s\n", err)
				return
			}

			// Print the raw response
			cmd.Println(resp)
		},
	}
	return cmd
}
