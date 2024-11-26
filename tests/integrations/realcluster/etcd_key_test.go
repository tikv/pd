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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/utils/testutil"
)

type etcdKeySuite struct {
	clusterSuite
}

func TestEtcdKey(t *testing.T) {
	suite.Run(t, &etcdKeySuite{
		clusterSuite: clusterSuite{
			suiteName: "etcd_key",
		},
	})
}

func TestMSEtcdKey(t *testing.T) {
	suite.Run(t, &etcdKeySuite{
		clusterSuite: clusterSuite{
			suiteName: "etcd_key",
			ms:        true,
		},
	})
}

var (
	// The keys that prefix is `/pd`.
	pdKeys = []string{
		"",
		"/pd//alloc_id",
		"/pd//config",
		// If not call `UpdateGCSafePoint`, this key will not exist.
		// "/pd//gc/safe_point",
		"/pd//gc/safe_point/service/gc_worker",
		"/pd//keyspaces/id/DEFAULT",
		"/pd//keyspaces/meta/",
		"/pd//leader",
		"/pd//member//binary_version",
		"/pd//member//deploy_path",
		"/pd//member//git_hash",
		"/pd//raft",
		"/pd//raft/min_resolved_ts",
		"/pd//raft/r/",
		"/pd//raft/s/",
		"/pd//raft/status/raft_bootstrap_time",
		"/pd//region_label/keyspaces/",
		"/pd//rule_group/tiflash",
		"/pd//rules/-c", // Why -c? See https://github.com/tikv/pd/pull/8789#discussion_r1853341293
		"/pd//scheduler_config/balance-hot-region-scheduler",
		"/pd//scheduler_config/balance-leader-scheduler",
		"/pd//scheduler_config/balance-region-scheduler",
		"/pd//scheduler_config/evict-slow-store-scheduler",
		"/pd//timestamp",
		"/pd//tso/keyspace_groups/membership/", // ms
		"/pd/cluster_id",
	}
	// The keys that prefix is `/ms`.
	msKeys = []string{
		"",
		"/ms//scheduling/primary",
		"/ms//scheduling/primary/expected_primary",
		"/ms//scheduling/registry/http://...:",
		"/ms//tso//primary",
		"/ms//tso//primary/expected_primary",
		"/ms//tso/registry/http://...:",
	}
	// These keys with `/pd` are only in `ms` mode.
	pdMSKeys = []string{
		"/pd//tso/keyspace_groups/membership/",
	}
)

func (s *etcdKeySuite) TestEtcdKey() {
	var keysBackup []string
	if !s.ms {
		keysBackup = pdKeys
		pdKeys = slices.DeleteFunc(pdKeys, func(s string) bool {
			return slices.Contains(pdMSKeys, s)
		})
		defer func() {
			pdKeys = keysBackup
		}()
	}
	t := s.T()
	endpoints := getPDEndpoints(t)

	testutil.Eventually(require.New(t), func() bool {
		keys, err := getEtcdKey(endpoints[0], "/pd")
		if err != nil {
			return false
		}
		return checkEtcdKey(t, keys, pdKeys)
	})

	if s.ms {
		testutil.Eventually(require.New(t), func() bool {
			keys, err := getEtcdKey(endpoints[0], "/ms")
			if err != nil {
				return false
			}
			return checkEtcdKey(t, keys, msKeys)
		})
	}
}

func getEtcdKey(endpoints, prefix string) ([]string, error) {
	// `sed 's/[0-9]*//g'` is used to remove the number in the etcd key, such as the cluster id.
	etcdCmd := fmt.Sprintf("etcdctl --endpoints=%s get %s --prefix --keys-only | sed 's/[0-9]*//g' | sort | uniq",
		endpoints, prefix)
	return runCommandWithOutput(etcdCmd)
}

func checkEtcdKey(t *testing.T, keys, expectedKeys []string) bool {
	for i, key := range keys {
		if len(key) == 0 {
			continue
		}
		if expectedKeys[i] != key {
			t.Logf("expected key: %s, got key: %s", expectedKeys[i], key)
			return false
		}
	}
	return true
}
