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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/pingcap/pd/pd-client"
	"github.com/spf13/cobra"
)

var (
	regionsPrefix = "pd/api/v1/regions"
	regionPrefix  = "pd/api/v1/region/%s"
)

// NewRegionCommand return a region subcommand of rootCmd
func NewRegionCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "region <region_id>",
		Short: "show the region status",
		Run:   showRegionCommandFunc,
	}
	r.AddCommand(NewRegionTableCommand())
	return r
}

func showRegionCommandFunc(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = regionsPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("region_id should be a number")
			return
		}
		prefix = fmt.Sprintf(regionPrefix, args[0])
	}
	url := getAddressFromCmd(cmd, prefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get region:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		printResponseError(r)
		return
	}

	io.Copy(os.Stdout, r.Body)
}

func NewRegionTableCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "table <key>",
		Short: "show the regions with table key",
		Run:   showRegionWithTableCommandFunc,
	}
	return r
}

func showRegionWithTableCommandFunc(cmd *cobra.Command, args []string) {
	key := args[0]
	addr, err := cmd.Flags().GetString("pd")
	client := pd.NewClient([]string{addr})
	regions, peers := client.GetRegion([]byte(key))
	regionsInfo, err := json.Marshal(regions)
	if err != nil {
		fmt.Printlf("err: %s", err)
	}
	fmt.Println(string(regionsInfo))
	peersInfo, err := json.Marshal(peers)
	if err != nil {
		fmt.Printlf("err: %s", err)
	}
	fmt.Println(string(peersInfo))
}
