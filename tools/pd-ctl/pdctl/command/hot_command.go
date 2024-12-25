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
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/storage"
)

const (
	hotReadRegionsPrefix    = "pd/api/v1/hotspot/regions/read"
	hotWriteRegionsPrefix   = "pd/api/v1/hotspot/regions/write"
	hotStoresPrefix         = "pd/api/v1/hotspot/stores"
	hotRegionsHistoryPrefix = "pd/api/v1/hotspot/regions/history"
	hotBucketsPrefix        = "pd/api/v1/hotspot/buckets"
)

// NewHotSpotCommand return a hot subcommand of rootCmd
func NewHotSpotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hot",
		Short: "show the hotspot status of the cluster",
	}
	cmd.AddCommand(NewHotWriteRegionCommand())
	cmd.AddCommand(NewHotReadRegionCommand())
	cmd.AddCommand(NewHotStoreCommand())
	cmd.AddCommand(NewHotRegionsHistoryCommand())
	cmd.AddCommand(NewHotBucketsCommand())
	return cmd
}

// NewHotWriteRegionCommand return a hot regions subcommand of hotSpotCmd
func NewHotWriteRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write [<store id> ...]",
		Short: "show the hot write regions",
		Run:   showHotWriteRegionsCommandFunc,
	}
	return cmd
}

func showHotWriteRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(hotWriteRegionsPrefix, "store_id", args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get write hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotReadRegionCommand return a hot read regions subcommand of hotSpotCmd
func NewHotReadRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read [<store id> ...]",
		Short: "show the hot read regions",
		Run:   showHotReadRegionsCommandFunc,
	}
	return cmd
}

func showHotReadRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(hotReadRegionsPrefix, "store_id", args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get read hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotStoreCommand return a hot stores subcommand of hotSpotCmd
func NewHotStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "show the hot stores",
		Run:   showHotStoresCommandFunc,
	}
	return cmd
}

func showHotStoresCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, hotStoresPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get store hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotRegionsHistoryCommand return a hot history regions subcommand of hotSpotCmd
func NewHotRegionsHistoryCommand() *cobra.Command {
	cmd := &cobra.Command{
		// TODO
		// Need a better description.
		Use:   "history <start_time> <end_time> [<key> <value>]",
		Short: "show the hot history regions",
		Run:   showHotRegionsHistoryCommandFunc,
	}
	return cmd
}

// NewHotBucketsCommand return a hot buckets subcommand of hotSpotCmd
func NewHotBucketsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "buckets [region_id]",
		Short: "show the hot buckets",
		Run:   showHotBucketsCommandFunc,
	}
	return cmd
}

func showHotBucketsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(hotBucketsPrefix, "region_id", args)
	if err != nil {
		cmd.Printf("Failed to get hotspot buckets: %s\n", err)
		return
	}

	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get hotspot buckets: %s\n", err)
		return
	}
	cmd.Println(r)
}

func showHotRegionsHistoryCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 2 || len(args)%2 != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	input, err := parseHotRegionsHistoryArgs(args)
	if err != nil {
		cmd.Printf("Failed to get history hotspot: %s\n", err)
		return
	}
	data, _ := json.Marshal(input)
	endpoints := getEndpoints(cmd)
	hotRegions := &storage.HistoryHotRegions{}
	for _, endpoint := range endpoints {
		tempHotRegions := storage.HistoryHotRegions{}
		resp, err := doRequestSingleEndpoint(cmd, endpoint, hotRegionsHistoryPrefix,
			http.MethodGet, http.Header{
				"Content-Type": {"application/json"},
			}, WithBody(bytes.NewBuffer(data)))
		if err != nil {
			cmd.Printf("Failed to get history hotspot: %s\n", err)
			return
		}
		err = json.Unmarshal([]byte(resp), &tempHotRegions)
		if err != nil {
			cmd.Printf("Failed to get history hotspot: %s\n", err)
			return
		}
		hotRegions.HistoryHotRegion = append(hotRegions.HistoryHotRegion, tempHotRegions.HistoryHotRegion...)
	}
	historyHotRegions := hotRegions.HistoryHotRegion
	sort.SliceStable(historyHotRegions, func(i, j int) bool {
		if historyHotRegions[i].UpdateTime > historyHotRegions[j].UpdateTime {
			return true
		}
		return historyHotRegions[i].RegionID < historyHotRegions[j].RegionID
	})
	resp, err := json.Marshal(hotRegions)
	if err != nil {
		cmd.Printf("Failed to get history hotspot: %s\n", err)
		return
	}
	cmd.Println(string(resp))
}

func parseOptionalArgs(prefix string, param string, args []string) (string, error) {
	argsLen := len(args)
	if argsLen > 0 {
		prefix += "?"
	}
	for i, arg := range args {
		if _, err := strconv.Atoi(arg); err != nil {
			return "", errors.Errorf("args should be a number, but got %s", arg)
		}
		if i != argsLen {
			prefix = prefix + param + "=" + arg + "&"
		} else {
			prefix = prefix + param + "=" + arg
		}
	}
	return prefix, nil
}

func parseHotRegionsHistoryArgs(args []string) (map[string]any, error) {
	startTime, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, errors.Errorf("start_time should be a number,but got %s", args[0])
	}
	endTime, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, errors.Errorf("end_time should be a number,but got %s", args[1])
	}
	input := map[string]any{
		"start_time": startTime,
		"end_time":   endTime,
	}
	stringToIntSlice := func(s string) ([]int64, error) {
		results := make([]int64, 0)
		args := strings.Split(s, ",")
		for _, arg := range args {
			result, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		return results, nil
	}
	for index := 2; index < len(args); index += 2 {
		switch args[index] {
		case "hot_region_type":
			input["hot_region_type"] = []string{args[index+1]}
		case "region_id":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("region_id should be a number slice,but got %s", args[index+1])
			}
			input["region_ids"] = results
		case "store_id":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("store_id should be a number slice,but got %s", args[index+1])
			}
			input["store_ids"] = results
		case "peer_id":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("peer_id should be a number slice,but got %s", args[index+1])
			}
			input["peer_ids"] = results
		case "is_leader":
			isLeader, err := strconv.ParseBool(args[index+1])
			if err != nil {
				return nil, errors.Errorf("is_leader should be a bool,but got %s", args[index+1])
			}
			input["is_leaders"] = []bool{isLeader}
		case "is_learner":
			isLearner, err := strconv.ParseBool(args[index+1])
			if err != nil {
				return nil, errors.Errorf("is_learner should be a bool,but got %s", args[index+1])
			}
			input["is_learners"] = []bool{isLearner}
		default:
			return nil, errors.Errorf("key should be one of hot_region_type,region_id,store_id,peer_id,is_leader,is_learner")
		}
	}
	if _, ok := input["is_leaders"]; !ok {
		input["is_leaders"] = []bool{
			true,
			false,
		}
	}
	if _, ok := input["is_learners"]; !ok {
		input["is_learners"] = []bool{
			true,
			false,
		}
	}
	return input, nil
}
