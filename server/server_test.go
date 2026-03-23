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

package server

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	servercluster "github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

func newTestServer(t *testing.T, keyspaceGroupEnabled bool) *Server {
	t.Helper()

	cfg := config.NewConfig()
	s := &Server{
		ctx:                    context.Background(),
		cfg:                    cfg,
		persistOptions:         config.NewPersistOptions(cfg),
		isKeyspaceGroupEnabled: keyspaceGroupEnabled,
	}
	atomic.StoreInt64(&s.isRunning, 1)
	return s
}

func TestIsServiceIndependent(t *testing.T) {
	re := require.New(t)

	// Keyspace groups disabled: always false even if cluster says independent.
	s := newTestServer(t, false)
	s.cluster = &servercluster.RaftCluster{}
	s.cluster.SetServiceIndependent(constant.TSOServiceName)
	re.False(s.IsServiceIndependent(constant.TSOServiceName))

	// Keyspace groups enabled, dynamic switching disabled: TSO always independent
	// (microservice is expected to be running).
	s2 := newTestServer(t, true)
	re.True(s2.IsServiceIndependent(constant.TSOServiceName))

	// Keyspace groups enabled, dynamic switching enabled: depends on cluster state.
	s3 := newTestServer(t, true)
	s3.cfg.Microservice.EnableTSODynamicSwitching = true
	s3.cluster = &servercluster.RaftCluster{}
	re.False(s3.IsServiceIndependent(constant.TSOServiceName))

	// Service set independent: true.
	s3.cluster.SetServiceIndependent(constant.TSOServiceName)
	re.True(s3.IsServiceIndependent(constant.TSOServiceName))

	// Server closed: false.
	atomic.StoreInt64(&s3.isRunning, 0)
	re.False(s3.IsServiceIndependent(constant.TSOServiceName))
}
