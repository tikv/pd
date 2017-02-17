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
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
)

// NewScheduleCommand returns a schedule command.
func NewScheduleCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "schedule [command]",
		Short: "schedule region",
	}
	c.AddCommand(NewTransferLeaderCommand())
	c.AddCommand(NewTransferRegionCommand())
	return c
}

// NewTransferLeaderCommand returns a transfer leader command.
func NewTransferLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer_leader <region_id> <store_id>",
		Short: "transfer leader to the store",
		Run:   transferLeaderCommandFunc,
	}
	return c
}

// NewTransferRegionCommand returns a transfer region command.
func NewTransferRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer_region <region_id> <store_id>...",
		Short: "transfer region to the stores",
		Run:   transferRegionCommandFunc,
	}
	return c
}

func parseIDs(args []string) ([]uint64, error) {
	var ids []uint64
	for _, s := range args {
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func transferLeaderCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseIDs(args)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	s := newScheduler(pdAddr)
	s.transferLeader(ids[0], ids[1])
}

func transferRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseIDs(args)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	s := newScheduler(pdAddr)
	s.transferRegion(ids[0], ids[1:])
}
