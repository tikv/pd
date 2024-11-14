// Copyright 2024 TiKV Project Authors.
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

package schedulers

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

type balanceKeyrangeSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *operator.Controller
	conf   config.SchedulerConfigProvider
}

func TestBalanceKeyrangeSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceKeyrangeSchedulerTestSuite))
}

func (suite *balanceKeyrangeSchedulerTestSuite) SetupTest() {
	suite.cancel, suite.conf, suite.tc, suite.oc = prepareSchedulersTest()
}

func (suite *balanceKeyrangeSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeNormal() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", ""}))
	re.NoError(err)

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 12)

	ops, _ := sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 10, 12)

	sb, err = CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", url.QueryEscape("11"), ""}))
	ops, _ = sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	re.Empty(ops)
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeFinish() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", ""}))
	re.NoError(err)

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 11)
	tc.AddLeaderRegion(4, 10, 11)
	tc.AddLeaderRegion(5, 10, 11)
	tc.AddLeaderRegion(6, 10, 12)

	sb.Schedule(tc, false)
	re.False(sb.IsFinished())
	sb.Schedule(tc, false)
	re.False(sb.IsFinished())
	sb.Schedule(tc, false)
	re.True(sb.IsFinished())
}
