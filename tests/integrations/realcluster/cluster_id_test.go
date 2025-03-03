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

package realcluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

type clusterIDSuite struct {
	clusterSuite
}

func TestClusterID(t *testing.T) {
	suite.Run(t, &clusterIDSuite{
		clusterSuite: clusterSuite{
			suiteName: "cluster_id",
		},
	})
}

func (s *clusterIDSuite) TestClientClusterID() {
	// create clusters manually
	s.TearDownSuite()
	re := s.Require()
	ctx := context.Background()
	// deploy first cluster
	cluster1 := newCluster(re, s.tag(), s.dataDir(), s.mode, map[string]int{"pd": 1, "tikv": 3, "tidb": 1, "tiflash": 0})
	cluster1.start()
	defer cluster1.stop()
	// deploy second cluster
	cluster2 := newCluster(re, s.tag(), s.dataDir(), s.mode, map[string]int{"pd": 1, "tikv": 3, "tidb": 1, "tiflash": 0})
	cluster2.start()
	defer cluster2.stop()

	pdEndpoints := getPDEndpoints(re)
	// Try to create a client with the mixed endpoints.
	_, err := pd.NewClientWithContext(
		ctx, caller.TestComponent, pdEndpoints,
		pd.SecurityOption{}, opt.WithMaxErrorRetry(1),
	)
	re.Error(err)
	re.Contains(err.Error(), "unmatched cluster id")
}
