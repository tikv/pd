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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestCleanupClusterResources(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hbStreams := hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, core.NewBasicCluster())
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cluster := &Cluster{}

	s := &Server{
		basicCluster: basicCluster,
		hbStreams:    hbStreams,
		storage:      storage,
	}
	s.cluster.Store(cluster)

	s.cleanupClusterResources()
	s.cleanupClusterResources()

	re.Nil(s.GetCluster())
	re.Nil(s.basicCluster)
	re.Nil(s.hbStreams)
	re.Nil(s.storage)
}
