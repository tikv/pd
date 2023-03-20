// Copyright 2023 TiKV Project Authors.
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

package mcs

import (
	"context"
	"time"

	rm "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// SetupTSOClient creates a TSO client for test.
func SetupTSOClient(ctx context.Context, re *require.Assertions, endpoints []string, opts ...pd.ClientOption) pd.Client {
	cli, err := pd.NewClientWithKeyspace(ctx, utils.DefaultKeyspaceID, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

// StartSingleResourceManagerTestServer creates and starts a resource manager server with default config for testing.
func StartSingleResourceManagerTestServer(ctx context.Context, re *require.Assertions, backendEndpoints string) (*rm.Server, func()) {
	cfg, err := rm.NewTestDefaultConfig()
	re.NoError(err)
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = tempurl.Alloc()

	s, cleanup, err := rm.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// StartSingleTSOTestServer creates and starts a tso server with default config for testing.
func StartSingleTSOTestServer(ctx context.Context, re *require.Assertions, backendEndpoints string, listenAddrs ...string) (*tso.Server, func()) {
	cfg, err := tso.NewTSOTestDefaultConfig()
	re.NoError(err)
	cfg.BackendEndpoints = backendEndpoints
	if len(listenAddrs) > 0 {
		cfg.ListenAddr = listenAddrs[0]
	} else {
		cfg.ListenAddr = tempurl.Alloc()
	}

	s, cleanup, err := tso.NewTSOTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}
