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
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	schedulersPrefix         = "pd/api/v1/schedulers"
	schedulerConfigPrefix    = "pd/api/v1/scheduler-config"
	evictLeaderSchedulerName = "evict-leader-scheduler"
	grantLeaderSchedulerName = "grant-leader-scheduler"
	lastStoreDeleteInfo      = "The last store has been deleted"
)

func pauseOrResumeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 && len(args) != 1 {
		cmd.Usage()
		return
	}
	path := schedulersPrefix + "/" + args[0]
	input := make(map[string]interface{})
	input["delay"] = 0
	if len(args) == 2 {
		dealy, err := strconv.Atoi(args[1])
		if err != nil {
			cmd.Usage()
			return
		}
		input["delay"] = dealy
	}
	postJSON(cmd, path, input)
}

func showSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func checkSchedulerExist(cmd *cobra.Command, schedulerName string) (bool, error) {
	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return false, err
	}
	var scheudlerList []string
	json.Unmarshal([]byte(r), &scheudlerList)
	for idx := range scheudlerList {
		if strings.Contains(scheudlerList[idx], schedulerName) {
			return true, nil
		}
	}
	return false, nil
}

func addSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	// we should ensure whether it is the first time to create evict-leader-scheduler
	// or just update the evict-leader. But is add one ttl time.
	switch cmd.Name() {
	case evictLeaderSchedulerName, grantLeaderSchedulerName:
		exist, err := checkSchedulerExist(cmd, cmd.Name())
		if err != nil {
			return
		}
		if exist {
			addStoreToSchedulerConfig(cmd, cmd.Name(), args)
			return
		}
		fallthrough
	default:
		storeID, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println(err)
			return
		}

		input := make(map[string]interface{})
		input["name"] = cmd.Name()
		input["store_id"] = storeID
		postJSON(cmd, schedulersPrefix, input)
	}

}

func addSchedulerForShuffleHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	limit := uint64(1)
	if len(args) == 1 {
		l, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println("Error: ", err)
			return
		}
		limit = l
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["limit"] = limit
	postJSON(cmd, schedulersPrefix, input)
}

func addSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

func addSchedulerForScatterRangeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	startKey, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	endKey, err := parseKey(cmd.Flags(), args[1])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["start_key"] = url.QueryEscape(startKey)
	input["end_key"] = url.QueryEscape(endKey)
	input["range_name"] = args[2]
	postJSON(cmd, schedulersPrefix, input)
}

func addSchedulerForBalanceAdjacentRegionCommandFunc(cmd *cobra.Command, args []string) {
	l := len(args)
	input := make(map[string]interface{})
	if l > 2 {
		cmd.Println(cmd.UsageString())
		return
	} else if l == 1 {
		input["leader_limit"] = url.QueryEscape(args[0])
	} else if l == 2 {
		input["leader_limit"] = url.QueryEscape(args[0])
		input["peer_limit"] = url.QueryEscape(args[1])
	}
	input["name"] = cmd.Name()

	postJSON(cmd, schedulersPrefix, input)
}

func setCommandUse(cmd *cobra.Command, targetUse string) {
	cmd.Use = targetUse + " "
}

func restoreCommandUse(cmd *cobra.Command, origionCommandUse string) {
	cmd.Use = origionCommandUse
}

func redirectReomveSchedulerToDeleteConfig(cmd *cobra.Command, schedulerName string, args []string) {
	args = strings.Split(args[0], "-")
	args = args[len(args)-1:]
	deleteStoreFromSchedulerConfig(cmd, schedulerName, args)
}

func removeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	// FIXME: maybe there is a more graceful method to handler it
	switch {
	case strings.HasPrefix(args[0], evictLeaderSchedulerName) && args[0] != evictLeaderSchedulerName:
		redirectReomveSchedulerToDeleteConfig(cmd, evictLeaderSchedulerName, args)
	case strings.HasPrefix(args[0], grantLeaderSchedulerName) && args[0] != grantLeaderSchedulerName:
		redirectReomveSchedulerToDeleteConfig(cmd, grantLeaderSchedulerName, args)
	default:
		path := schedulersPrefix + "/" + args[0]
		_, err := doRequest(cmd, path, http.MethodDelete)
		if err != nil {
			cmd.Println(err)
			return
		}
		cmd.Println("Success!")
	}

}

func addStoreToSchedulerConfigSub(cmd *cobra.Command, args []string) {
	addStoreToSchedulerConfig(cmd, cmd.Parent().Name(), args)
}

func addStoreToSchedulerConfig(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}
	input := make(map[string]interface{})
	input["name"] = schedulerName
	input["store_id"] = storeID

	postJSON(cmd, path.Join(schedulerConfigPrefix, schedulerName, "config"), input)
}

func listSchedulerConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	path := path.Join(schedulerConfigPrefix, cmd.Name(), "list")
	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func postSchedulerConfigCommandFuncSub(cmd *cobra.Command, args []string) {
	postSchedulerConfigCommandFunc(cmd, cmd.Parent().Name(), args)
}

func postSchedulerConfigCommandFunc(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	var val interface{}
	input := make(map[string]interface{})
	key, value := args[0], args[1]
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	input[key] = val
	postJSON(cmd, path.Join(schedulerConfigPrefix, schedulerName, "config"), input)
}

// convertReomveConfigToReomveScheduler make cmd can be used at removeCommandFunc
func convertReomveConfigToReomveScheduler(cmd *cobra.Command) {
	setCommandUse(cmd, "remove")
}

func redirectDeleteConfigToRemoveScheduler(cmd *cobra.Command, schedulerName string, args []string) {
	args = append(args[:0], schedulerName)
	cmdStore := cmd.Use
	convertReomveConfigToReomveScheduler(cmd)
	defer restoreCommandUse(cmd, cmdStore)
	removeSchedulerCommandFunc(cmd, args)
}

func deleteStoreFromSchedulerConfigSub(cmd *cobra.Command, args []string) {
	deleteStoreFromSchedulerConfig(cmd, cmd.Parent().Name(), args)
}

func deleteStoreFromSchedulerConfig(cmd *cobra.Command, schedulerName string, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	path := path.Join(schedulerConfigPrefix, "/", schedulerName, "delete", args[0])
	resp, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		cmd.Println(err)
		return
	}
	// FIXME: remove the judge when the new command replace old command
	if strings.Contains(resp, lastStoreDeleteInfo) {
		redirectDeleteConfigToRemoveScheduler(cmd, schedulerName, args)
		return
	}
	cmd.Println("Success!")
}

func showShuffleRegionSchedulerRolesCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	path := path.Join(schedulerConfigPrefix, cmd.Parent().Name(), "roles")
	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func setSuffleRegionSchedulerRolesCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	var roles []string
	fields := strings.Split(strings.ToLower(args[0]), ",")
	for _, f := range fields {
		if f != "" {
			roles = append(roles, f)
		}
	}
	b, _ := json.Marshal(roles)
	path := path.Join(schedulerConfigPrefix, cmd.Parent().Name(), "roles")
	_, err := doRequest(cmd, path, http.MethodPost,
		WithBody("application/json", bytes.NewBuffer(b)))
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}
