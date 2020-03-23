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
	"fmt"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	operatorsPrefix = "pd/api/v1/operators"
)

func showOperatorCommandFunc(cmd *cobra.Command, args []string) {
	var path string
	if len(args) == 0 {
		path = operatorsPrefix
	} else if len(args) == 1 {
		path = fmt.Sprintf("%s?kind=%s", operatorsPrefix, args[0])
	} else {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func checkOperatorCommandFunc(cmd *cobra.Command, args []string) {
	var path string
	if len(args) == 0 {
		path = operatorsPrefix
	} else if len(args) == 1 {
		path = fmt.Sprintf("%s/%s", operatorsPrefix, args[0])
	} else {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func transferLeaderCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

func transferRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) <= 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_ids"] = ids[1:]
	postJSON(cmd, operatorsPrefix, input)
}

func transferPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["from_store_id"] = ids[1]
	input["to_store_id"] = ids[2]
	postJSON(cmd, operatorsPrefix, input)
}

func addPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

func addLearnerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

func mergeRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["source_region_id"] = ids[0]
	input["target_region_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

func removePeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

func withSplitPolicyFlag(flag *pflag.FlagSet) {
	flag.String("policy", "scan", "the policy to get region split key")
}

func splitRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	policy := cmd.Flags().Lookup("policy").Value.String()
	switch policy {
	case "scan", "approximate":
		break
	default:
		cmd.Println("Error: unknown policy")
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["policy"] = policy
	postJSON(cmd, operatorsPrefix, input)
}

func scatterRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	postJSON(cmd, operatorsPrefix, input)
}

func removeOperatorCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	path := operatorsPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println("Success!")
}

func parseUint64s(args []string) ([]uint64, error) {
	results := make([]uint64, 0, len(args))
	for _, arg := range args {
		v, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		results = append(results, v)
	}
	return results, nil
}
