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

package command

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var (
	msPrimaryPrefix = "/pd/api/v2/ms/primary/%s"
	msMembersPrefix = "/pd/api/v2/ms/members/%s"
)

// NewMsCommand return a microservice subcommand of rootCmd
func NewMsCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "ms <command>",
		Short: "microservice commands",
	}
	m.AddCommand(newMSTsoCommand())
	m.AddCommand(newMSSchedulerCommand())
	return m
}

func newMSTsoCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "tso <primary|members>",
		Short: "ms commands",
	}
	d.AddCommand(&cobra.Command{
		Use:   "primary",
		Short: "show the primary status",
		Run:   getPrimaryCommandFunc,
	})
	d.AddCommand(&cobra.Command{
		Use:   "members",
		Short: "show the members status",
		Run:   getMembersCommandFunc,
	})
	return d
}

func newMSSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduling <primary|members>",
		Short: "micro-service commands",
	}
	c.AddCommand(&cobra.Command{
		Use:   "primary",
		Short: "show the primary member status",
		Run:   getPrimaryCommandFunc,
	})
	c.AddCommand(&cobra.Command{
		Use:   "members",
		Short: "show the primary member status",
		Run:   getMembersCommandFunc,
	})
	return c
}

func getMembersCommandFunc(cmd *cobra.Command, _ []string) {
	parent := cmd.Parent().Name()
	uri := fmt.Sprintf(msMembersPrefix, parent)
	r, err := doRequest(cmd, uri, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the microservice members: %s\n", err)
		return
	}
	cmd.Println(r)
}

func getPrimaryCommandFunc(cmd *cobra.Command, _ []string) {
	parent := cmd.Parent().Name()
	uri := fmt.Sprintf(msPrimaryPrefix, parent)
	r, err := doRequest(cmd, uri, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the microservice primary: %s\n", err)
		return
	}
	cmd.Println(r)
}
