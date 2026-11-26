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
	"time"

	"github.com/stretchr/testify/require"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestClusterID(t *testing.T) {
	t.Run("TestClientClusterID", func(t *testing.T) {
		re := require.New(t)
		s := clusterSuite{suiteName: "cluster_id", mode: "tikv-slim"}
		ctx := context.Background()
		// deploy first cluster
		tag1 := s.tag()
		cluster1 := newCluster(re, tag1, dataDirForTag(tag1), s.mode, map[string]int{"pd": 1, "kv": 1, "db": 0, "tiflash": 0})
		defer cluster1.stop()
		cluster1.start()
		// deploy second cluster
		tag2 := s.tag()
		cluster2 := newCluster(re, tag2, dataDirForTag(tag2), s.mode, map[string]int{"pd": 1, "kv": 1, "db": 0, "tiflash": 0})
		cluster2.portOffset = 10000
		defer cluster2.stop()
		cluster2.start()

		var ep1, ep2 []string
		testutil.Eventually(re, func() bool {
			ep1 = cluster1.pdEndpoints()
			ep2 = cluster2.pdEndpoints()
			if len(ep1) != 1 || len(ep2) != 1 || ep1[0] == ep2[0] {
				return false
			}
			clusterID1, ok := fetchClusterID(ctx, ep1)
			if !ok || clusterID1 == 0 {
				return false
			}
			clusterID2, ok := fetchClusterID(ctx, ep2)
			return ok && clusterID2 != 0 && clusterID1 != clusterID2
		}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(500*time.Millisecond))

		pdEndpoints := []string{ep1[0], ep2[0]}
		// Try to create a client with the mixed endpoints.
		cli, err := pd.NewClientWithContext(
			ctx, caller.TestComponent, pdEndpoints,
			pd.SecurityOption{}, opt.WithMaxErrorRetry(1), opt.WithCustomTimeoutOption(10*time.Second),
		)
		if cli != nil {
			defer cli.Close()
		}
		re.Error(err)
		re.Contains(err.Error(), "unmatched cluster id")
	})
}

func fetchClusterID(ctx context.Context, endpoints []string) (uint64, bool) {
	cli, err := pd.NewClientWithContext(
		ctx, caller.TestComponent, endpoints,
		pd.SecurityOption{}, opt.WithMaxErrorRetry(1), opt.WithCustomTimeoutOption(5*time.Second),
	)
	if cli != nil {
		defer cli.Close()
	}
	if err != nil {
		return 0, false
	}
	return cli.GetClusterID(ctx), true
}
