// Copyright 2020 TiKV Project Authors.
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
	"encoding/json"
	"net/http"
	"sort"

	"github.com/spf13/cobra"
	"github.com/tikv/pd/server/api"
)

var (
	serviceGCSafepointPrefix = "pd/api/v1/gc/safepoint"
)

// NewServiceGCSafepointCommand return a service gc safepoint subcommand of rootCmd
func NewServiceGCSafepointCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "service-gc-safepoint",
		Short: "show all service gc safepoint",
		Run:   showSSPs,
	}
	l.AddCommand(NewDeleteServiceGCSafepointCommand())
	return l
}

// NewDeleteServiceGCSafepointCommand return a subcommand to delete service gc safepoint
func NewDeleteServiceGCSafepointCommand() *cobra.Command {
	l := &cobra.Command{
		Use:    "delete <service ID>",
		Short:  "delete a service gc safepoint",
		Run:    deleteSSP,
		Hidden: true,
	}
	return l
}

func showSSPs(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, serviceGCSafepointPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get service GC safepoint: %s\n", err)
		return
	}
	var safepoint api.ListServiceGCSafepoint
	if err := json.Unmarshal([]byte(r), &safepoint); err != nil {
		cmd.Printf("Failed to unmarshal service GC safepoint: %s\n", err)
		return
	}
	sort.Slice(safepoint.ServiceGCSafepoints, func(i, j int) bool {
		return safepoint.ServiceGCSafepoints[i].SafePoint < safepoint.ServiceGCSafepoints[j].SafePoint
	})
	data, err := json.MarshalIndent(safepoint, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal service GC safepoint: %s\n", err)
		return
	}
	cmd.Println(string(data))
}

func deleteSSP(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	serviceID := args[0]
	deleteURL := serviceGCSafepointPrefix + "/" + serviceID
	r, err := doRequest(cmd, deleteURL, http.MethodDelete, http.Header{})
	if err != nil {
		cmd.Printf("Failed to delete service GC safepoint: %s\n", err)
		return
	}
	cmd.Println(r)
}
