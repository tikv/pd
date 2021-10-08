// Copyright 2020 TiKV Project Authors.
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

package autoscalingtest

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"

	"go.uber.org/goleak"
)

const (
	prometheusAddressKey = "/topology/prometheus"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&apiTestSuite{})

type apiTestSuite struct{}

func (s *apiTestSuite) TestAPI(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.LeaderLease = 60
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	leaderServer := cluster.GetServer(cluster.WaitLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	serverAddr := strings.Split(leaderServer.GetAddr(), "//")[1]
	serverAddrList := strings.Split(serverAddr, ":")
	prometheusAddress := fmt.Sprintf(`{"ip": "%s", "port": %s, "path": "%s"}`, serverAddrList[0], serverAddrList[1], "/prometheus")
	_, err = cluster.GetEtcdClient().Put(ctx, prometheusAddressKey, prometheusAddress)
	c.Assert(err, IsNil)

	strategies := map[autoscaling.ComponentType][]byte{
		autoscaling.TiKV: []byte(`
{
    "rules":[
        {
            "component":"tikv",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "resource_types":[
                    "resource_a",
                    "default_homogeneous_tikv"
                ]
            },
            "storage_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "resource_types":[
                    "resource_a",
                    "default_homogeneous_tikv"
                ]
            }
        }
    ],
    "resources":[
        {
            "resource_type":"resource_a",
            "cpu":1000,
            "memory":8589934592,
            "storage":10737418240,
            "count": 2
        },
        {
            "resource_type":"default_homogeneous_tikv",
            "cpu":4000,
            "memory":17179869184,
            "storage":10737418240
        }
    ],
    "node_count": 5
}`),
		autoscaling.TiDB: []byte(`
{
    "rules":[
        {
            "component":"tidb",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "resource_types":[
                    "resource_b",
                    "default_homogeneous_tidb"
                ]
            },
            "storage_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "resource_types":[
                    "resource_b",
                    "default_homogeneous_tidb"
                ]
            }
        }
    ],
    "resources":[
        {
            "resource_type":"resource_b",
            "cpu":2000,
            "memory":8589934592,
            "storage":10737418240,
            "count": 4
        },
        {
            "resource_type":"default_homogeneous_tidb",
            "cpu":4000,
            "memory":8589934592,
            "storage":10737418240
        }
    ],
    "node_count": 5
}`),
	}

	resp, err := http.Post(leaderServer.GetAddr()+"/autoscaling", "application/json", bytes.NewBuffer(strategies[autoscaling.TiKV]))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, 200)

	resp, err = http.Post(leaderServer.GetAddr()+"/autoscaling", "application/json", bytes.NewBuffer(strategies[autoscaling.TiDB]))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, 200)
}
