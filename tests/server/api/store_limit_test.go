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

package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

func TestStoreLimitRemainsAvailableDuringRollingUpgrade(t *testing.T) {
	env := tests.NewSchedulingTestEnvironment(t)
	t.Cleanup(env.Cleanup)
	env.RunTestBasedOnMode(func(cluster *tests.TestCluster) {
		re := require.New(t)
		if cluster.GetSchedulingPrimaryServer() != nil {
			// A registered Scheduling Service member may be unreachable during a rolling upgrade.
			entry := &discovery.ServiceRegistryEntry{
				Name:        "pre-feature-scheduling",
				ServiceAddr: "http://127.0.0.1:1",
				Version:     versioninfo.PDReleaseVersion,
			}
			data, err := entry.Serialize()
			re.NoError(err)
			path := keypath.RegistryPath(constant.SchedulingServiceName, entry.ServiceAddr)
			client := cluster.GetEtcdClient()
			_, err = client.Put(t.Context(), path, data)
			re.NoError(err)
			defer func() {
				if _, err := client.Delete(t.Context(), path); err != nil {
					t.Errorf("remove test Scheduling Service registration: %v", err)
				}
			}()
		}

		leader := cluster.GetLeaderServer()
		url := leader.GetAddr() + "/pd/api/v1/stores/limit"
		options := leader.GetPersistOptions()
		newDefault := options.GetScheduleConfig().DefaultStoreLimit.AddPeer + 45
		body := []byte(fmt.Sprintf(`{"rate":%v,"type":"add-peer"}`, newDefault))
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, url, body, testutil.StatusOK(re)))
		re.Equal(newDefault, options.GetScheduleConfig().DefaultStoreLimit.AddPeer)
	})
}
