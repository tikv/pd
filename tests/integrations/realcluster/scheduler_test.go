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

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/types"
	"go.uber.org/zap"
)

type schedulerSuite struct {
	clusterSuite
}

func TestScheduler(t *testing.T) {
	suite.Run(t, &schedulerSuite{
		clusterSuite: clusterSuite{
			suiteName: "scheduler",
		},
	})
}

// https://github.com/tikv/pd/issues/6988#issuecomment-1694924611
// https://github.com/tikv/pd/issues/6897
func (s *schedulerSuite) TestTransferLeader() {
	re := require.New(s.T())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pdHTTPCli := http.NewClient("pd-real-cluster-test", getPDEndpoints(s.T()))
	resp, err := pdHTTPCli.GetLeader(ctx)
	re.NoError(err)
	oldLeader := resp.Name

	var newLeader string
	for i := range 2 {
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

func (s *schedulerSuite) TestRegionLabelDenyScheduler() {
	re := require.New(s.T())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pdHTTPCli := http.NewClient("pd-real-cluster-test", getPDEndpoints(s.T()))
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
	labelRule := &http.LabelRule{
		ID:       "rule1",
		Labels:   []http.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges(region1.StartKey, region1.EndKey),
	}
	re.NoError(pdHTTPCli.SetRegionLabelRule(ctx, labelRule))
	defer func() {
		pdHTTPCli.PatchRegionLabelRules(ctx, &http.LabelRulePatch{DeleteRules: []string{labelRule.ID}})
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

	pdHTTPCli.PatchRegionLabelRules(ctx, &http.LabelRulePatch{DeleteRules: []string{labelRule.ID}})
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

func (s *schedulerSuite) TestGrantOrEvictLeaderTwice() {
	re := require.New(s.T())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pdHTTPCli := http.NewClient("pd-real-cluster-test", getPDEndpoints(s.T()))
	regions, err := pdHTTPCli.GetRegions(ctx)
	re.NoError(err)
	re.NotEmpty(regions.Regions)
	region1 := regions.Regions[0]

	var i int
	evictLeader := func() {
		re.NoError(pdHTTPCli.CreateScheduler(ctx, types.EvictLeaderScheduler.String(), uint64(region1.Leader.StoreID)))
		// if the second evict leader scheduler cause the pause-leader-filter
		// disable, the balance-leader-scheduler need some time to transfer
		// leader. See details in https://github.com/tikv/pd/issues/8756.
		if i == 1 {
			time.Sleep(3 * time.Second)
		}
		testutil.Eventually(re, func() bool {
			regions, err := pdHTTPCli.GetRegions(ctx)
			if err != nil {
				log.Error("get regions failed", zap.Error(err))
				return false
			}
			for _, region := range regions.Regions {
				if region.Leader.StoreID == region1.Leader.StoreID {
					return false
				}
			}
			return true
		}, testutil.WithWaitFor(time.Minute))

		i++
	}

	evictLeader()
	evictLeader()
	pdHTTPCli.DeleteScheduler(ctx, types.EvictLeaderScheduler.String())

	i = 0
	grantLeader := func() {
		re.NoError(pdHTTPCli.CreateScheduler(ctx, types.GrantLeaderScheduler.String(), uint64(region1.Leader.StoreID)))
		if i == 1 {
			time.Sleep(3 * time.Second)
		}
		testutil.Eventually(re, func() bool {
			regions, err := pdHTTPCli.GetRegions(ctx)
			if err != nil {
				log.Error("get regions failed", zap.Error(err))
				return false
			}
			for _, region := range regions.Regions {
				if region.Leader.StoreID != region1.Leader.StoreID {
					return false
				}
			}
			return true
		}, testutil.WithWaitFor(2*time.Minute))

		i++
	}

	grantLeader()
	grantLeader()
	pdHTTPCli.DeleteScheduler(ctx, types.GrantLeaderScheduler.String())
}
