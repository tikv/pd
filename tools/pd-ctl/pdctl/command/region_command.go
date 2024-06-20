// Copyright 2016 TiKV Project Authors.
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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	regionsPrefix           = "pd/api/v1/regions"
	regionsStorePrefix      = "pd/api/v1/regions/store"
	regionsCheckPrefix      = "pd/api/v1/regions/check"
	regionsWriteFlowPrefix  = "pd/api/v1/regions/writeflow"
	regionsReadFlowPrefix   = "pd/api/v1/regions/readflow"
	regionsWriteQueryPrefix = "pd/api/v1/regions/writequery"
	regionsReadQueryPrefix  = "pd/api/v1/regions/readquery"
	regionsConfVerPrefix    = "pd/api/v1/regions/confver"
	regionsVersionPrefix    = "pd/api/v1/regions/version"
	regionsSizePrefix       = "pd/api/v1/regions/size"
	regionTopKeysPrefix     = "pd/api/v1/regions/keys"
	regionTopCPUPrefix      = "pd/api/v1/regions/cpu"
	regionsKeyPrefix        = "pd/api/v1/regions/key"
	regionsSiblingPrefix    = "pd/api/v1/regions/sibling"
	regionsRangeHolesPrefix = "pd/api/v1/regions/range-holes"
	regionsKeyspacePrefix   = "pd/api/v1/regions/keyspace"
	regionIDPrefix          = "pd/api/v1/region/id"
	regionKeyPrefix         = "pd/api/v1/region/key"
)

// NewRegionCommand returns a region subcommand of rootCmd
func NewRegionCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   `region <region_id> [--jq="<query string>"]`,
		Short: "show the region status",
		Run:   showRegionCommandFunc,
	}
	r.AddCommand(NewRegionWithKeyCommand())
	r.AddCommand(NewRegionWithCheckCommand())
	r.AddCommand(NewRegionWithSiblingCommand())
	r.AddCommand(NewRegionWithStoreCommand())
	r.AddCommand(NewRegionWithKeyspaceCommand())
	r.AddCommand(NewRegionsByKeysCommand())
	r.AddCommand(NewRangesWithRangeHolesCommand())

	topRead := &cobra.Command{
		Use:   `topread [byte|query] <limit> [--jq="<query string>"]`,
		Short: "show regions with top read flow or query",
		Run:   showTopReadRegions,
	}
	topRead.Flags().String("jq", "", "jq query")
	r.AddCommand(topRead)

	topWrite := &cobra.Command{
		Use:   `topwrite [byte|query] <limit> [--jq="<query string>"]`,
		Short: "show regions with top write flow or query",
		Run:   showTopWriteRegions,
	}
	topWrite.Flags().String("jq", "", "jq query")
	r.AddCommand(topWrite)

	topConfVer := &cobra.Command{
		Use:   `topconfver <limit> [--jq="<query string>"]`,
		Short: "show regions with top conf version",
		Run:   showRegionsTopCommand(regionsConfVerPrefix),
	}
	topConfVer.Flags().String("jq", "", "jq query")
	r.AddCommand(topConfVer)

	topVersion := &cobra.Command{
		Use:   `topversion <limit> [--jq="<query string>"]`,
		Short: "show regions with top version",
		Run:   showRegionsTopCommand(regionsVersionPrefix),
	}
	topVersion.Flags().String("jq", "", "jq query")
	r.AddCommand(topVersion)

	topSize := &cobra.Command{
		Use:   `topsize <limit> [--jq="<query string>"]`,
		Short: "show regions with top size",
		Run:   showRegionsTopCommand(regionsSizePrefix),
	}
	topSize.Flags().String("jq", "", "jq query")
	r.AddCommand(topSize)

	topKeys := &cobra.Command{
		Use:   `topkeys <limit> [--jq="<query string>"]`,
		Short: "show regions with top keys",
		Run:   showRegionsTopCommand(regionTopKeysPrefix),
	}
	topKeys.Flags().String("jq", "", "jq query")
	r.AddCommand(topKeys)

	topCPU := &cobra.Command{
		Use:   `topcpu <limit> [--jq="<query string>"]`,
		Short: "show regions with top CPU usage",
		Run:   showRegionsTopCommand(regionTopCPUPrefix),
	}
	topCPU.Flags().String("jq", "", "jq query")
	r.AddCommand(topCPU)

	scanRegion := &cobra.Command{
		Use:   `scan [--jq="<query string>"]`,
		Short: "scan all regions",
		Run:   scanRegionCommandFunc,
	}
	scanRegion.Flags().String("jq", "", "jq query")
	r.AddCommand(scanRegion)

	r.Flags().String("jq", "", "jq query")

	return r
}

func showRegionCommandFunc(cmd *cobra.Command, args []string) {
	prefix := regionsPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			cmd.Println("region_id should be a number")
			return
		}
		prefix = regionIDPrefix + "/" + args[0]
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
		printWithJQFilter(r, flag.Value.String())
		return
	}

	cmd.Println(r)
}

func scanRegionCommandFunc(cmd *cobra.Command, _ []string) {
	const limit = 1024
	var key []byte
	for {
		uri := fmt.Sprintf("%s?key=%s&limit=%d", regionsKeyPrefix, url.QueryEscape(string(key)), limit)
		r, err := doRequest(cmd, uri, http.MethodGet, http.Header{})
		if err != nil {
			cmd.Printf("Failed to scan regions: %s\n", err)
			return
		}

		if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
			printWithJQFilter(r, flag.Value.String())
		} else {
			cmd.Println(r)
		}

		// Extract last region's endkey for next batch.
		type regionsInfo struct {
			Regions []*struct {
				EndKey string `json:"end_key"`
			} `json:"regions"`
		}

		var regions regionsInfo
		if err = json.Unmarshal([]byte(r), &regions); err != nil {
			cmd.Printf("Failed to unmarshal regions: %s\n", err)
			return
		}
		if len(regions.Regions) == 0 {
			return
		}

		lastEndKey := regions.Regions[len(regions.Regions)-1].EndKey
		if lastEndKey == "" {
			return
		}

		key, err = hex.DecodeString(lastEndKey)
		if err != nil {
			cmd.Println("Bad format region key: ", key)
			return
		}
	}
}

type run = func(cmd *cobra.Command, args []string)

func showRegionsTopCommand(prefix string) run {
	return func(cmd *cobra.Command, args []string) {
		if len(args) == 1 {
			if _, err := strconv.Atoi(args[0]); err != nil {
				cmd.Println("limit should be a number")
				return
			}
			prefix += "?limit=" + args[0]
		} else if len(args) > 1 {
			cmd.Println(cmd.UsageString())
			return
		}
		r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
		if err != nil {
			cmd.Printf("Failed to get regions: %s\n", err)
			return
		}
		if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
			printWithJQFilter(r, flag.Value.String())
			return
		}
		cmd.Println(r)
	}
}

func showTopReadRegions(cmd *cobra.Command, args []string) {
	// default to show top read flow
	if len(args) == 0 {
		showRegionsTopCommand(regionsReadFlowPrefix)(cmd, args)
		return
	}
	// default to show top read flow with limit
	switch args[0] {
	case "query":
		showRegionsTopCommand(regionsReadQueryPrefix)(cmd, args[1:])
	case "byte":
		showRegionsTopCommand(regionsReadFlowPrefix)(cmd, args[1:])
	default:
		showRegionsTopCommand(regionsReadFlowPrefix)(cmd, args)
	}
}

func showTopWriteRegions(cmd *cobra.Command, args []string) {
	// default to show top write flow
	if len(args) == 0 {
		showRegionsTopCommand(regionsWriteFlowPrefix)(cmd, args)
		return
	}
	// default to show top write flow with limit
	switch args[0] {
	case "query":
		showRegionsTopCommand(regionsWriteQueryPrefix)(cmd, args[1:])
	case "byte":
		showRegionsTopCommand(regionsWriteFlowPrefix)(cmd, args[1:])
	default:
		showRegionsTopCommand(regionsWriteFlowPrefix)(cmd, args)
	}
}

// NewRegionWithKeyCommand return a region with key subcommand of regionCmd
func NewRegionWithKeyCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "key [--format=raw|encode|hex] <key>",
		Short: "show the region with key",
		Run:   showRegionWithTableCommandFunc,
	}
	r.Flags().String("format", "hex", "the key format")
	return r
}

func showRegionWithTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	key, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	key = url.QueryEscape(key)
	prefix := regionKeyPrefix + "/" + key
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	cmd.Println(r)
}

func parseKey(flags *pflag.FlagSet, key string) (string, error) {
	switch flags.Lookup("format").Value.String() {
	case "raw":
		return key, nil
	case "encode":
		return decodeKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return "", errors.WithStack(err)
		}
		return string(key), nil
	}
	return "", errors.New("unknown format")
}

func decodeKey(text string) (string, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return "", errors.WithStack(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return "", io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return "", errors.WithStack(err)
			}
			buf = append(buf, c)
		}
	}
	return string(buf), nil
}

// NewRegionsByKeysCommand returns regions in a given range [startkey, endkey) subcommand of regionCmd.
func NewRegionsByKeysCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "keys [--format=raw|encode|hex] <start_key> <end_key> <limit>",
		Short: "show regions in a given range [startkey, endkey)",
		Run:   showRegionsByKeysCommandFunc,
	}

	r.Flags().String("format", "hex", "the key format")
	return r
}

func showRegionsByKeysCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 || len(args) > 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	startKey, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	startKey = url.QueryEscape(startKey)
	prefix := regionsKeyPrefix + "?key=" + startKey
	if len(args) >= 2 {
		endKey, err := parseKey(cmd.Flags(), args[1])
		if err != nil {
			cmd.Println("Error: ", err)
			return
		}
		endKey = url.QueryEscape(endKey)
		prefix += "&end_key=" + endKey
	}
	if len(args) == 3 {
		if _, err = strconv.Atoi(args[2]); err != nil {
			cmd.Println("limit should be a number")
			return
		}
		prefix += "&limit=" + args[2]
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewRegionWithCheckCommand returns a region with check subcommand of regionCmd
func NewRegionWithCheckCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   `check [miss-peer|extra-peer|down-peer|learner-peer|pending-peer|offline-peer|empty-region|oversized-region|undersized-region|hist-size|hist-keys] [--jq="<query string>"]`,
		Short: "show the region with check specific status",
		Run:   showRegionWithCheckCommandFunc,
	}

	r.Flags().String("jq", "", "jq query")
	return r
}

func showRegionWithCheckCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 || len(args) > 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	state := args[0]
	prefix := regionsCheckPrefix + "/" + state
	if strings.EqualFold(state, "hist-size") {
		if len(args) == 2 {
			if _, err := strconv.Atoi(args[1]); err != nil {
				cmd.Println("region size histogram bound should be a number")
				return
			}
			prefix += "?bound=" + args[1]
		} else {
			prefix += "?bound=10"
		}
	} else if strings.EqualFold(state, "hist-keys") {
		if len(args) == 2 {
			if _, err := strconv.Atoi(args[1]); err != nil {
				cmd.Println("region keys histogram bound should be a number")
				return
			}
			prefix += "?bound=" + args[1]
		} else {
			prefix += "?bound=10000"
		}
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	if flag := cmd.Flag("jq"); flag != nil && flag.Value.String() != "" {
		printWithJQFilter(r, flag.Value.String())
		return
	}

	cmd.Println(r)
}

// NewRegionWithSiblingCommand returns a region with sibling subcommand of regionCmd
func NewRegionWithSiblingCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "sibling <region_id>",
		Short: "show the sibling regions of specific region",
		Run:   showRegionWithSiblingCommandFunc,
	}
	return r
}

func showRegionWithSiblingCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	regionID := args[0]
	prefix := regionsSiblingPrefix + "/" + regionID
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get region sibling: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewRegionWithStoreCommand returns regions with store subcommand of regionCmd
func NewRegionWithStoreCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "store <store_id>",
		Short: "show the regions of a specific store",
		Run:   showRegionWithStoreCommandFunc,
	}
	r.Flags().String("type", "all", "the type of the regions, could be 'all', 'leader', 'learner' or 'pending'")
	return r
}

func showRegionWithStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	storeID := args[0]
	prefix := regionsStorePrefix + "/" + storeID
	flagType := cmd.Flag("type")
	prefix += "?type=" + flagType.Value.String()
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get regions with the given storeID: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewRegionWithKeyspaceCommand returns regions with keyspace subcommand of regionCmd
func NewRegionWithKeyspaceCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "keyspace <subcommand>",
		Short: "show region information of the given keyspace",
	}
	r.AddCommand(&cobra.Command{
		Use:   "id <keyspace_id> <limit>",
		Short: "show region information for the given keyspace id",
		Run:   showRegionWithKeyspaceCommandFunc,
	})
	return r
}

func showRegionWithKeyspaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 || len(args) > 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	keyspaceID := args[0]
	prefix := regionsKeyspacePrefix + "/id/" + keyspaceID
	if len(args) == 2 {
		if _, err := strconv.Atoi(args[1]); err != nil {
			cmd.Println("limit should be a number")
			return
		}
		prefix += "?limit=" + args[1]
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get regions with the given keyspace: %s\n", err)
		return
	}
	cmd.Println(r)
}

const (
	rangeHolesLongDesc = `There are some cases that the region range is not continuous, for example, the region doesn't send the heartbeat to PD after a splitting.
This command will output all empty ranges without any region info.`
	rangeHolesExample = `
  If PD now holds the region ranges info like ["", "a"], ["b", "x"], ["x", "z"]. The the output will be like:

  [
    [
      "a",
      "b"
    ],
    [
      "z",
      ""
    ],
  ]
`
)

// NewRangesWithRangeHolesCommand returns ranges with range-holes subcommand of regionCmd
func NewRangesWithRangeHolesCommand() *cobra.Command {
	r := &cobra.Command{
		Use:     "range-holes",
		Short:   "show all empty ranges without any region info.",
		Long:    rangeHolesLongDesc,
		Example: rangeHolesExample,
		Run:     showRangesWithRangeHolesCommandFunc,
	}
	return r
}

func showRangesWithRangeHolesCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, regionsRangeHolesPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get range holes: %s\n", err)
		return
	}
	cmd.Println(r)
}

func printWithJQFilter(data, filter string) {
	cmd := exec.Command("jq", "-c", filter)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		fmt.Println(err)
		return
	}

	go func() {
		defer stdin.Close()
		_, err = io.WriteString(stdin, data)
		if err != nil {
			fmt.Println(err)
		}
	}()

	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(out), err)
		return
	}

	fmt.Printf("%s\n", out)
}
