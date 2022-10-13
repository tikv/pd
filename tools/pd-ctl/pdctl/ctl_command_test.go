// Copyright 2022 TiKV Project Authors.
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

package pdctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/assertutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/goleak"
)

const (
	storeID = 1
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type commandTestSuite struct {
	suite.Suite
	cluster []*server.Server
	cancel  context.CancelFunc
}

func TestCommandTestSuite(t *testing.T) {
	suite.Run(t, new(commandTestSuite))
}

func (suite *commandTestSuite) SetupSuite() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cluster = mustNewCluster(ctx, re, 1)
	suite.cancel = cancel
}

func (suite *commandTestSuite) TearDownSuite() {
	suite.cancel()
	for _, server := range suite.cluster {
		server.Close()
		testutil.CleanServer(server.GetConfig().DataDir)
	}
}
func (suite *commandTestSuite) TestClusterCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl cluster --pd <addr>
	args := fmt.Sprintf("cluster --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actual metapb.Cluster
	err = json.Unmarshal(bufOut.Bytes(), &actual)
	re.NoError(err)
	re.Equal(suite.cluster[0].ClusterID(), actual.GetId())

	// $ pdctl cluster status --pd <addr>
	args = fmt.Sprintf("cluster status --pd %s", addr)
	cmd.SetArgs(strings.Split(args, " "))

	bufOut.Reset()
	bufErr.Reset()

	err = cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actualStatus cluster.Status
	err = json.Unmarshal(bufOut.Bytes(), &actualStatus)
	re.NoError(err)
	suite.True(actualStatus.IsInitialized)
}

func (suite *commandTestSuite) TestConfigCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl show --pd <addr>
	args := fmt.Sprintf("config show all --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actual config.Config
	err = json.Unmarshal(bufOut.Bytes(), &actual)
	re.NoError(err)

	expected := suite.cluster[0].GetConfig()
	// this assignment is required per https://github.com/tikv/pd/blob/6c76647809cc73368820d17470554a9943ab9422/server/api/config.go#L58
	expected.Schedule.MaxMergeRegionKeys = expected.Schedule.GetMaxMergeRegionKeys()
	re.Equal(expected.String(), strings.TrimSpace(bufOut.String()))
}

func (suite *commandTestSuite) TestHealthCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl health --pd <addr>
	args := fmt.Sprintf("health --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actuals []api.Health
	err = json.Unmarshal(bufOut.Bytes(), &actuals)
	re.NoError(err)
	re.Len(actuals, 1)

	actual := actuals[0]
	re.Equal(suite.cluster[0].Name(), actual.Name)
	re.Equal(suite.cluster[0].GetMember().ID(), actual.MemberID)
	re.True(actual.Health)
	re.Equal(suite.cluster[0].GetMemberInfo().ClientUrls, actual.ClientUrls)
}

func (suite *commandTestSuite) TestHotCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl hot read --pd <addr>
	args := fmt.Sprintf("hot read --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var readStats statistics.StoreHotPeersInfos
	err = json.Unmarshal(bufOut.Bytes(), &readStats)
	re.NoError(err)
	re.NotEmpty(readStats)

	bufOut.Reset()
	bufErr.Reset()

	// $ pdctl hot write --pd <addr>
	args = fmt.Sprintf("hot write --pd %s", addr)
	cmd.SetArgs(strings.Split(args, " "))

	err = cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var writeStats statistics.StoreHotPeersInfos
	err = json.Unmarshal(bufOut.Bytes(), &writeStats)
	re.NoError(err)
	re.NotEmpty(writeStats)

	bufOut.Reset()
	bufErr.Reset()

	// $ pdctl hot store --pd <addr>
	args = fmt.Sprintf("hot store --pd %s", addr)
	cmd.SetArgs(strings.Split(args, " "))

	err = cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var storeStats api.HotStoreStats
	err = json.Unmarshal(bufOut.Bytes(), &storeStats)
	re.NoError(err)
	re.NotEmpty(storeStats)
}

func (suite *commandTestSuite) TestLabelCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl label --pd <addr>
	args := fmt.Sprintf("label --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actual []*metapb.StoreLabel
	err = json.Unmarshal(bufOut.Bytes(), &actual)
	re.NoError(err)

	expected := suite.cluster[0].GetBasicCluster().Stores.GetStore(storeID).GetLabels()
	re.Equal(expected, actual)

	bufOut.Reset()
	bufErr.Reset()

	// $ pdctl label store --pd <addr>
	args = fmt.Sprintf("label store label1 value1 --pd %s", addr)
	cmd.SetArgs(strings.Split(args, " "))

	err = cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var stores api.StoresInfo
	err = json.Unmarshal(bufOut.Bytes(), &stores)
	re.NoError(err)
	re.Len(stores.Stores, 1)
	re.Equal(1, stores.Count)

	expectedStore := suite.cluster[0].GetBasicCluster().Stores.GetStore(storeID).GetMeta()
	actualStore := stores.Stores[0].Store
	re.Equal(expectedStore.GetId(), actualStore.GetId())
	re.Equal(expectedStore.GetAddress(), actualStore.GetAddress())
	re.Equal(expectedStore.GetLabels(), actualStore.GetLabels())
	re.Equal(expectedStore.GetNodeState(), actualStore.GetNodeState())
	re.Equal(expectedStore.GetState(), actualStore.GetState())
}

func (suite *commandTestSuite) TestMemberCommand() {
	var (
		re     = suite.Require()
		bufOut = &bytes.Buffer{}
		bufErr = &bytes.Buffer{}
		addr   = suite.cluster[0].GetAddr()
	)

	// $ pdctl member --pd <addr>
	args := fmt.Sprintf("member --pd %s", addr)
	cmd := GetRootCmd()
	cmd.SetArgs(strings.Split(args, " "))
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)

	err := cmd.Execute()
	re.NoError(err)
	re.Empty(bufErr)

	var actual pdpb.GetMembersResponse
	err = json.Unmarshal(bufOut.Bytes(), &actual)
	re.NoError(err)

	var (
		clusterSize = len(suite.cluster)
		member      = actual.GetMembers()[0]
	)

	re.Equal(clusterSize, len(actual.GetMembers()))
	re.Equal("pd1", actual.GetLeader().GetName())
	re.Equal("pd1", actual.GetEtcdLeader().GetName())

	re.Equal(member.GetClientUrls(), actual.GetLeader().GetClientUrls())
	re.Equal(member.GetMemberId(), actual.GetLeader().GetMemberId())
	re.Equal(member.GetPeerUrls(), actual.GetLeader().GetPeerUrls())

	re.Equal(member.GetClientUrls(), actual.GetEtcdLeader().GetClientUrls())
	re.Equal(member.GetMemberId(), actual.GetEtcdLeader().GetMemberId())
	re.Equal(member.GetPeerUrls(), actual.GetEtcdLeader().GetPeerUrls())
}

var zapLogOnce sync.Once

func mustNewCluster(ctx context.Context, re *require.Assertions, clusterSize int) []*server.Server {
	svrs := make([]*server.Server, 0, clusterSize)
	cfgs := server.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), clusterSize)

	ch := make(chan *server.Server, clusterSize)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			err := cfg.SetupLogger()
			re.NoError(err)

			zapLogOnce.Do(func() {
				log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
			})

			s, err := server.CreateServer(ctx, cfg, api.NewHandler)
			re.NoError(err)

			err = s.Run()
			re.NoError(err)
			ch <- s
		}(cfg)
	}

	for i := 0; i < clusterSize; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)

	server.MustWaitLeader(re, svrs)
	mustBootstrapCluster(re, svrs[0].ClusterID(), svrs[0].GetAddr())
	svrs[0].SetReplicationConfig(config.ReplicationConfig{
		MaxReplicas: 1,
	})

	return svrs
}

func mustBootstrapCluster(re *require.Assertions, clusterID uint64, clusterAddr string) {
	var (
		store = &metapb.Store{
			Id:        storeID,
			Address:   "localhost",
			NodeState: metapb.NodeState_Serving,
			Labels: []*metapb.StoreLabel{
				{Key: "label1", Value: "value1"},
				{Key: "label2", Value: "value2"},
			},
		}
		peers = []*metapb.Peer{
			{
				Id:      2,
				StoreId: store.GetId(),
			},
		}
		region = &metapb.Region{
			Id: 8,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: peers,
		}
	)
	grpcPDClient := testutil.MustNewGrpcClient(re, clusterAddr)
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
		Region: region,
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
}
