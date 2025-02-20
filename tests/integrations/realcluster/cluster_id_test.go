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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
	s.T().Skip("skip the test because it is not stable")
	re := s.Require()
	ctx := context.Background()
	// deploy second cluster
	cluster2 := newCluster(re, s.tag(), s.dataDir(), s.mode)
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

func getPDEndpoints(re *require.Assertions) []string {
	output, err := runCommandWithOutput("ps -ef | grep tikv-server | awk -F '--pd-endpoints=' '{print $2}' | awk '{print $1}'")
	re.NoError(err)
	var pdAddrs []string
	for _, addr := range strings.Split(strings.TrimSpace(output), "\n") {
		// length of addr is less than 5 means it must not be a valid address
		if len(addr) < 5 {
			continue
		}
		pdAddrs = append(pdAddrs, strings.Split(addr, ",")...)
	}
	return removeDuplicates(pdAddrs)
}

func removeDuplicates(arr []string) []string {
	uniqueMap := make(map[string]bool)
	var result []string

	for _, item := range arr {
		if _, exists := uniqueMap[item]; !exists {
			uniqueMap[item] = true
			result = append(result, item)
		}
	}

	return result
}
