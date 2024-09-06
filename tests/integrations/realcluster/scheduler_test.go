// Copyright 2024 TiKV Authors
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

package realcluster

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/types"
)

// https://github.com/tikv/pd/issues/6988#issuecomment-1694924611
// https://github.com/tikv/pd/issues/6897
func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	oldLeader := resp.Name

	var newLeader string
	for i := 0; i < 2; i++ {
		if resp.Name != fmt.Sprintf("pd-%d", i) {
			newLeader = fmt.Sprintf("pd-%d", i)
		}
	}

	// record scheduler
	re.NoError(pdHTTPCli.CreateScheduler(ctx, types.EvictLeaderScheduler.String(), 1))
	defer func() {
		re.NoError(pdHTTPCli.DeleteScheduler(ctx, types.EvictLeaderScheduler.String()))
	}()
	res, err := pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	oldSchedulersLen := len(res)

	re.NoError(pdHTTPCli.TransferLeader(ctx, newLeader))
	// wait for transfer leader to new leader
	time.Sleep(1 * time.Second)
	resp, err = pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	re.Equal(newLeader, resp.Name)

	res, err = pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	re.Len(res, oldSchedulersLen)

	// transfer leader to old leader
	re.NoError(pdHTTPCli.TransferLeader(ctx, oldLeader))
	// wait for transfer leader
	time.Sleep(1 * time.Second)
	resp, err = pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	re.Equal(oldLeader, resp.Name)

	res, err = pdHTTPCli.GetSchedulers(ctx)
	re.NoError(err)
	re.Len(res, oldSchedulersLen)
}

func TestRegionLabelDenyScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	regions, err := pdHTTPCli.GetRegions(ctx)
	re.NoError(err)
	re.NotEmpty(regions.Regions)
	region1 := regions.Regions[0]

	err = pdHTTPCli.DeleteScheduler(ctx, types.BalanceLeaderScheduler.String())
	if err == nil {
		defer func() {
			pdHTTPCli.CreateScheduler(ctx, types.BalanceLeaderScheduler.String(), 0)
		}()
	}

	re.NoError(pdHTTPCli.CreateScheduler(ctx, types.GrantLeaderScheduler.String(), uint64(region1.Leader.StoreID)))
	defer func() {
		pdHTTPCli.DeleteScheduler(ctx, types.GrantLeaderScheduler.String())
	}()

	// wait leader transfer
	testutil.Eventually(re, func() bool {
		regions, err := pdHTTPCli.GetRegions(ctx)
		re.NoError(err)
		for _, region := range regions.Regions {
			if region.Leader.StoreID != region1.Leader.StoreID {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(time.Minute))

	// disable schedule for region1
	labelRule := &pd.LabelRule{
		ID:       "rule1",
		Labels:   []pd.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges(region1.StartKey, region1.EndKey),
	}
	re.NoError(pdHTTPCli.SetRegionLabelRule(ctx, labelRule))
	defer func() {
		pdHTTPCli.PatchRegionLabelRules(ctx, &pd.LabelRulePatch{DeleteRules: []string{labelRule.ID}})
	}()
	labelRules, err := pdHTTPCli.GetAllRegionLabelRules(ctx)
	re.NoError(err)
	re.Len(labelRules, 2)
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Equal(labelRule.ID, labelRules[1].ID)
	re.Equal(labelRule.Labels, labelRules[1].Labels)
	re.Equal(labelRule.RuleType, labelRules[1].RuleType)

	// enable evict leader scheduler, and check it works
	re.NoError(pdHTTPCli.DeleteScheduler(ctx, types.GrantLeaderScheduler.String()))
	re.NoError(pdHTTPCli.CreateScheduler(ctx, types.EvictLeaderScheduler.String(), uint64(region1.Leader.StoreID)))
	defer func() {
		pdHTTPCli.DeleteScheduler(ctx, types.EvictLeaderScheduler.String())
	}()
	testutil.Eventually(re, func() bool {
		regions, err := pdHTTPCli.GetRegions(ctx)
		re.NoError(err)
		for _, region := range regions.Regions {
			if region.Leader.StoreID == region1.Leader.StoreID {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(time.Minute))

	re.NoError(pdHTTPCli.DeleteScheduler(ctx, types.EvictLeaderScheduler.String()))
	re.NoError(pdHTTPCli.CreateScheduler(ctx, types.GrantLeaderScheduler.String(), uint64(region1.Leader.StoreID)))
	defer func() {
		pdHTTPCli.DeleteScheduler(ctx, types.GrantLeaderScheduler.String())
	}()
	testutil.Eventually(re, func() bool {
		regions, err := pdHTTPCli.GetRegions(ctx)
		re.NoError(err)
		for _, region := range regions.Regions {
			if region.ID == region1.ID {
				continue
			}
			if region.Leader.StoreID != region1.Leader.StoreID {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(time.Minute))

	pdHTTPCli.PatchRegionLabelRules(ctx, &pd.LabelRulePatch{DeleteRules: []string{labelRule.ID}})
	labelRules, err = pdHTTPCli.GetAllRegionLabelRules(ctx)
	re.NoError(err)
	re.Len(labelRules, 1)

	testutil.Eventually(re, func() bool {
		regions, err := pdHTTPCli.GetRegions(ctx)
		re.NoError(err)
		for _, region := range regions.Regions {
			if region.Leader.StoreID != region1.Leader.StoreID {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(time.Minute))
}
