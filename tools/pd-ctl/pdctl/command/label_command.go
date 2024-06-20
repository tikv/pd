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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/statistics"
)

var (
	labelsPrefix      = "pd/api/v1/labels"
	labelsStorePrefix = "pd/api/v1/labels/stores"
)

// NewLabelCommand return a member subcommand of rootCmd
func NewLabelCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "label [store]",
		Short: "show the labels",
		Run:   showLabelsCommandFunc,
	}
	l.AddCommand(NewLabelListStoresCommand())
	l.AddCommand(NewCheckLabels())
	return l
}

// NewLabelListStoresCommand return a label subcommand of labelCmd
func NewLabelListStoresCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "store <name> [value]",
		Short: "show the stores with specify label",
		Run:   showLabelListStoresCommandFunc,
	}
	return l
}

func showLabelsCommandFunc(cmd *cobra.Command, _ []string) {
	r, err := doRequest(cmd, labelsPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get labels: %s\n", err)
		return
	}
	cmd.Println(r)
}

func getValue(args []string, i int) string {
	if len(args) <= i {
		return ""
	}
	return args[i]
}

func showLabelListStoresCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		cmd.Println("Usage: label store name [value]")
		return
	}
	namePrefix := fmt.Sprintf("name=%s", getValue(args, 0))
	valuePrefix := fmt.Sprintf("value=%s", getValue(args, 1))
	prefix := fmt.Sprintf("%s?%s&%s", labelsStorePrefix, namePrefix, valuePrefix)
	r, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get stores through label: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewCheckLabels returns a isolation label check.
func NewCheckLabels() *cobra.Command {
	return &cobra.Command{
		Use:     "isolation [label]",
		Short:   "isolation labels",
		Example: "label-count: map[isolation-label:count], region-map: [region-id:isolation-label]",
		Run:     checkIsolationLabel,
	}
}

func getReplicationConfig(cmd *cobra.Command, _ []string) (*sc.ReplicationConfig, error) {
	prefix := configPrefix + "/replicate"
	body, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		return nil, err
	}
	var config sc.ReplicationConfig
	if err := json.Unmarshal([]byte(body), &config); err != nil {
		return nil, err
	}
	return &config, nil
}

func getStores(cmd *cobra.Command, _ []string) ([]*core.StoreInfo, error) {
	prefix := storesPrefix
	body, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		return nil, err
	}
	var storesInfo response.StoresInfo
	if err := json.Unmarshal([]byte(body), &storesInfo); err != nil {
		return nil, err
	}
	stores := make([]*core.StoreInfo, 0)
	for _, storeInfo := range storesInfo.Stores {
		stores = append(stores, core.NewStoreInfo(storeInfo.Store.Store))
	}
	return stores, nil
}

func getRegions(cmd *cobra.Command, _ []string) ([]response.RegionInfo, error) {
	prefix := regionsPrefix
	body, err := doRequest(cmd, prefix, http.MethodGet, http.Header{})
	if err != nil {
		return nil, err
	}
	var RegionsInfo response.RegionsInfo
	if err := json.Unmarshal([]byte(body), &RegionsInfo); err != nil {
		return nil, err
	}
	return RegionsInfo.Regions, nil
}

func checkIsolationLabel(cmd *cobra.Command, args []string) {
	var checkLabel string
	if len(args) == 1 {
		checkLabel = args[0]
	}

	storesInfo, err := getStores(cmd, args)
	if err != nil {
		cmd.Printf("Failed to get stores info: %s\n", err)
		return
	}

	regionsInfo, err := getRegions(cmd, args)
	if err != nil {
		cmd.Printf("Failed to get regions info: %s\n", err)
		return
	}

	config, err := getReplicationConfig(cmd, args)
	if err != nil {
		cmd.Printf("Failed to get labels: %s\n", err)
		return
	}
	locationLabels := config.LocationLabels

	storesMap := make(map[uint64]*core.StoreInfo, len(storesInfo))
	for _, store := range storesInfo {
		storesMap[store.GetID()] = store
	}

	regionMap := make(map[uint64]string, len(regionsInfo))
	labelCount := make(map[string]int, len(locationLabels))

	for _, region := range regionsInfo {
		stores := make([]*core.StoreInfo, 0)
		for _, peer := range region.Peers {
			if s, ok := storesMap[peer.StoreId]; ok {
				stores = append(stores, s)
			}
		}
		isolationLabel := statistics.GetRegionLabelIsolation(stores, locationLabels)
		if len(checkLabel) == 0 || isolationLabel == checkLabel {
			regionMap[region.ID] = isolationLabel
			labelCount[isolationLabel]++
		}
	}
	cmd.Printf("label-count:%+v,region-map:%+v\n", labelCount, regionMap)
}
