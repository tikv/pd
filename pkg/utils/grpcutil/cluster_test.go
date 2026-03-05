// Copyright 2026 TiKV Project Authors.
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

package grpcutil

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/keypath"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

type clusterSuite struct {
	suite.Suite

	rc           *core.BasicCluster
	oldClusterID uint64
}

func TestFollowerForwardAndHandleTestSuite(t *testing.T) {
	suite.Run(t, new(clusterSuite))
}

func (suite *clusterSuite) SetupSuite() {
	suite.oldClusterID = keypath.ClusterID()
	keypath.SetClusterID(1)
	suite.rc = core.NewBasicCluster()
	suite.rc.PutRegion(core.NewTestRegionInfo(1, 1, []byte("a"), []byte("d")))
	suite.rc.PutRegion(core.NewTestRegionInfo(2, 1, []byte("f"), []byte("g")))
}

func (suite *clusterSuite) TearDownSuite() {
	keypath.SetClusterID(suite.oldClusterID)
}

func (suite *clusterSuite) TestScanRegions() {
	re := suite.Require()

	for _, isFollower := range []bool{true, false} {
		suite.T().Run("isFollower="+strconv.FormatBool(isFollower), func(t *testing.T) {
			resp, err := ScanRegions(suite.rc, &pdpb.ScanRegionsRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("e"),
				Limit:    10,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			re.Nil(resp.GetHeader().GetError())
			re.Len(resp.GetRegions(), 1)
			re.Equal(uint64(1), resp.GetRegions()[0].GetRegion().GetId())

			resp, err = ScanRegions(suite.rc, &pdpb.ScanRegionsRequest{
				StartKey: []byte(""),
				EndKey:   []byte("0"),
				Limit:    10,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Empty(resp.GetRegions())
			}
		})
	}
}

func (suite *clusterSuite) TestBatchScanRegions() {
	re := suite.Require()

	for _, isFollower := range []bool{true, false} {
		suite.T().Run("isFollower="+strconv.FormatBool(isFollower), func(t *testing.T) {
			resp, err := BatchScanRegions(suite.rc, &pdpb.BatchScanRegionsRequest{
				Ranges: []*pdpb.KeyRange{
					{
						StartKey: []byte("a"),
						EndKey:   []byte("e"),
					},
				},
				Limit: 10,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			re.Nil(resp.GetHeader().GetError())
			re.Len(resp.GetRegions(), 1)
			re.Equal(uint64(1), resp.GetRegions()[0].GetRegion().GetId())

			resp, err = BatchScanRegions(suite.rc, &pdpb.BatchScanRegionsRequest{
				Ranges: []*pdpb.KeyRange{
					{
						StartKey: []byte(""),
						EndKey:   []byte("0"),
					},
				},
				Limit: 10,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Empty(resp.GetRegions())
			}
		})
	}
}

func (suite *clusterSuite) TestGetRegion() {
	re := suite.Require()

	for _, isFollower := range []bool{true, false} {
		suite.T().Run("isFollower="+strconv.FormatBool(isFollower), func(t *testing.T) {
			resp, err := GetRegion(suite.rc, &pdpb.GetRegionRequest{
				RegionKey: []byte("c"),
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			re.Nil(resp.GetHeader().GetError())
			re.Equal(uint64(1), resp.GetRegion().GetId())

			resp, err = GetRegion(suite.rc, &pdpb.GetRegionRequest{
				RegionKey: []byte("0"),
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Nil(resp.GetRegion())
			}
		})
	}
}

func (suite *clusterSuite) TestGetRegionByID() {
	re := suite.Require()

	for _, isFollower := range []bool{true, false} {
		suite.T().Run("isFollower="+strconv.FormatBool(isFollower), func(t *testing.T) {
			resp, err := GetRegionByID(suite.rc, &pdpb.GetRegionByIDRequest{
				RegionId: 1,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			re.Nil(resp.GetHeader().GetError())
			re.Equal(uint64(1), resp.GetRegion().GetId())

			resp, err = GetRegionByID(suite.rc, &pdpb.GetRegionByIDRequest{
				RegionId: 0,
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Nil(resp.GetRegion())
			}
		})
	}
}

func (suite *clusterSuite) TestGetRegionByPreKey() {
	re := suite.Require()

	for _, isFollower := range []bool{true, false} {
		suite.T().Run("isFollower="+strconv.FormatBool(isFollower), func(t *testing.T) {
			resp, err := GetPrevRegion(suite.rc, &pdpb.GetRegionRequest{
				RegionKey: []byte("g"),
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Nil(resp.GetRegion())
			}

			resp, err = GetPrevRegion(suite.rc, &pdpb.GetRegionRequest{
				RegionKey: []byte("0"),
			}, isFollower)
			re.NoError(err)
			re.NotNil(resp)
			if isFollower {
				re.NotNil(resp.GetHeader().GetError())
				re.Equal(pdpb.ErrorType_REGION_NOT_FOUND, resp.GetHeader().GetError().GetType())
			} else {
				re.Nil(resp.GetHeader().GetError())
				re.Nil(resp.GetRegion())
			}
		})
	}
}
