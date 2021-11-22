// Copyright 2021 TiKV Project Authors.
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

package checker

import (
	"context"
	"encoding/hex"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
)

var _ = Suite(&testSplitCheckerSuite{})

type testSplitCheckerSuite struct {
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	labeler     *labeler.RegionLabeler
	sc          *SplitChecker
	ctx         context.Context
	cancel      context.CancelFunc
}

func (s *testSplitCheckerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testSplitCheckerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testSplitCheckerSuite) SetUpTest(c *C) {
	cfg := config.NewTestOptions()
	cfg.GetReplicationConfig().EnablePlacementRules = true
	s.cluster = mockcluster.NewCluster(s.ctx, cfg)
	s.ruleManager = s.cluster.RuleManager
	s.labeler = s.cluster.RegionLabeler
	s.sc = NewSplitChecker(s.cluster, s.ruleManager, s.labeler)
}

func (s *testSplitCheckerSuite) TestSplit(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "aa",
		EndKeyHex:   "cc",
		Role:        placement.Voter,
		Count:       1,
	})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1)
	op := s.sc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Len(), Equals, 1)
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	c.Assert(hex.EncodeToString(splitKeys[0]), Equals, "aa")
	c.Assert(hex.EncodeToString(splitKeys[1]), Equals, "cc")

	// region label has higher priority.
	s.labeler.SetLabelRule(&labeler.LabelRule{
		ID:       "test",
		Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
		RuleType: labeler.KeyRange,
		Data:     makeKeyRanges("bb", "dd"),
	})
	op = s.sc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Len(), Equals, 1)
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	c.Assert(hex.EncodeToString(splitKeys[0]), Equals, "bb")
	c.Assert(hex.EncodeToString(splitKeys[1]), Equals, "dd")
}
