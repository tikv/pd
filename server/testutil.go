// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/tempurl"
	"github.com/pingcap/pd/pkg/typeutil"

	// Register namespace classifiers.
	_ "github.com/pingcap/pd/table"
)

// CleanupFunc closes test pd server(s) and deletes any files left behind.
type CleanupFunc func()

func cleanServer(cfg *Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}

// NewTestServer creates a pd server for testing.
func NewTestServer(c *check.C) (*Config, *Server, CleanupFunc, error) {
	cfg := NewTestSingleConfig(c)
	s, err := CreateServer(cfg, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = s.Run(context.TODO()); err != nil {
		return nil, nil, nil, err
	}

	cleanup := func() {
		s.Close()
		cleanServer(cfg)
	}
	return cfg, s, cleanup, nil
}

var zapLogOnce sync.Once

// NewTestSingleConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func NewTestSingleConfig(c *check.C) *Config {
	cfg := &Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     1,
		TsoSaveInterval: typeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.disableStrictReconfigCheck = true
	cfg.TickInterval = typeutil.NewDuration(100 * time.Millisecond)
	cfg.ElectionInterval = typeutil.NewDuration(3 * time.Second)
	cfg.LeaderPriorityCheckInterval = typeutil.NewDuration(100 * time.Millisecond)
	err := cfg.SetupLogger()
	c.Assert(err, check.IsNil)
	zapLogOnce.Do(func() {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	})

	c.Assert(cfg.Adjust(nil), check.IsNil)

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because PD client also needs this, so export here.
func NewTestMultiConfig(c *check.C, count int) []*Config {
	cfgs := make([]*Config, count)

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
