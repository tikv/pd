// Copyright 2017 TiKV Project Authors.
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

package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/server/v3/embed"
)

// NewTestServer creates a pd server for testing.
func NewTestServer(re *require.Assertions, c *assertutil.Checker) (*Server, testutil.CleanupFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := NewTestSingleConfig(c)
	mockHandler := CreateMockHandler(re, "127.0.0.1")
	s, err := CreateServer(ctx, cfg, nil, mockHandler)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if err = s.Run(); err != nil {
		cancel()
		return nil, nil, err
	}

	cleanup := func() {
		cancel()
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

var zapLogOnce sync.Once

// NewTestSingleConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func NewTestSingleConfig(c *assertutil.Checker) *config.Config {
	schedulers.Register()
	cfg := &config.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     10,
		TSOSaveInterval: typeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = os.MkdirTemp("", "pd_tests")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.DisableStrictReconfigCheck = true
	cfg.TickInterval = typeutil.NewDuration(100 * time.Millisecond)
	cfg.ElectionInterval = typeutil.NewDuration(3 * time.Second)
	cfg.LeaderPriorityCheckInterval = typeutil.NewDuration(100 * time.Millisecond)
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	c.AssertNil(err)
	zapLogOnce.Do(func() {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	})

	c.AssertNil(cfg.Adjust(nil, false))

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because PD client also needs this, so export here.
func NewTestMultiConfig(c *assertutil.Checker, count int) []*config.Config {
	cfgs := make([]*config.Config, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig(c)
		cfg.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}

// MustWaitLeader return the leader until timeout.
func MustWaitLeader(re *require.Assertions, svrs []*Server) *Server {
	var leader *Server
	testutil.Eventually(re, func() bool {
		for _, svr := range svrs {
			// All servers' GetLeader should return the same leader.
			if svr.GetLeader() == nil || (leader != nil && svr.GetLeader().GetMemberId() != leader.GetLeader().GetMemberId()) {
				return false
			}
			if leader == nil && !svr.IsClosed() {
				leader = svr
			}
		}
		return true
	})
	return leader
}

// CreateMockHandler creates a mock handler for test.
func CreateMockHandler(re *require.Assertions, ip string) HandlerBuilder {
	return func(context.Context, *Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/apis/mock/v1/hello", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello World")
			// test getting ip
			clientIP, _ := apiutil.GetIPPortFromHTTPRequest(r)
			re.Equal(ip, clientIP)
		})
		info := apiutil.APIServiceGroup{
			Name:    "mock",
			Version: "v1",
		}
		return mux, info, nil
	}
}
