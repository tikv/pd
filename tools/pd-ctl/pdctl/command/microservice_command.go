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
	"net/url"

	"github.com/spf13/cobra"
)

var (
	msPrimaryPrefix = "/pd/api/v2/ms/primary/%s"
	msMembersPrefix = "/pd/api/v2/ms/members/%s"
	// msTSOEvictPrimaryPrefix is the PD endpoint that forwards an eviction of all
	// keyspace group primaries to the given tso node. It intentionally has no
	// leading slash: doRequest joins it to the endpoint with a "/", and a POST to
	// the resulting double-slash path would not be redirected the way GETs are.
	msTSOEvictPrimaryPrefix = "pd/api/v2/ms/tso/primary/evict?node=%s"
)

// NewMicroServicesCommand return a microservice subcommand of rootCmd
func NewMicroServicesCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "microservice <tso|scheduling>",
		Short: "microservice commands",
	}
	m.AddCommand(newMSTsoCommand())
	m.AddCommand(newMSSchedulingCommand())
	m.AddCommand(newMSRouterCommand())
	return m
}

func newMSTsoCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "tso <primary|members|evict-primary>",
		Short: "tso microservice commands",
	}
	d.AddCommand(&cobra.Command{
		Use:   "primary",
		Short: "show the tso primary status",
		Run:   getPrimaryCommandFunc,
	})
	d.AddCommand(&cobra.Command{
		Use:   "members",
		Short: "show the tso members status",
		Run:   getMembersCommandFunc,
	})
	d.AddCommand(newEvictTSOPrimaryCommand())
	return d
}

func newEvictTSOPrimaryCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "evict-primary <tso_node_address>",
		Short: "evict all keyspace group primaries held by the given tso node",
		Run:   evictTSOPrimaryCommandFunc,
	}
}

func newMSSchedulingCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduling <primary|members>",
		Short: "scheduling microservice commands",
	}
	c.AddCommand(&cobra.Command{
		Use:   "primary",
		Short: "show the scheduling primary member status",
		Run:   getPrimaryCommandFunc,
	})
	c.AddCommand(&cobra.Command{
		Use:   "members",
		Short: "show the scheduling members status",
		Run:   getMembersCommandFunc,
	})
	return c
}

func newMSRouterCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "router <members>",
		Short: "router microservice commands",
	}
	c.AddCommand(&cobra.Command{
		Use:   "members",
		Short: "show the router members status",
		Run:   getMembersCommandFunc,
	})
	return c
}

func getMembersCommandFunc(cmd *cobra.Command, _ []string) {
	parent := cmd.Parent().Name()
	uri := fmt.Sprintf(msMembersPrefix, parent)
	r, err := doRequest(cmd, uri, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the %s microservice members: %s\n", parent, err)
		return
	}
	cmd.Println(r)
}

func evictTSOPrimaryCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	// The request goes to PD, which forwards it to the given tso node. The
	// eviction only drains the primaries held by that node.
	uri := fmt.Sprintf(msTSOEvictPrimaryPrefix, url.QueryEscape(args[0]))
	r, err := doRequest(cmd, uri, http.MethodPost, http.Header{})
	if err != nil {
		cmd.Printf("Failed to evict the tso primaries on %s: %s\n", args[0], err)
		return
	}
	cmd.Println(r)
}

func getPrimaryCommandFunc(cmd *cobra.Command, _ []string) {
	parent := cmd.Parent().Name()
	uri := fmt.Sprintf(msPrimaryPrefix, parent)
	r, err := doRequest(cmd, uri, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get the %s microservice primary: %s\n", parent, err)
		return
	}
	cmd.Println(r)
}
