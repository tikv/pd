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

package tso

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mcs/utils"
)

func TestNewKeyspaceGroupManager(t *testing.T) {
	re := require.New(t)
	backendpoints, etcdClient, clean := startEmbeddedEtcd(t)
	defer clean()

	cfg := &TestServiceConfig{
		Name:                      "tso-test-name",
		BackendEndpoints:          backendpoints,
		ListenAddr:                "http://127.0.0.1:3379",
		AdvertiseListenAddr:       "http://127.0.0.1:3379",
		LeaderLease:               utils.DefaultLeaderLease,
		LocalTSOEnabled:           false,
		TSOUpdatePhysicalInterval: 50 * time.Millisecond,
		TSOSaveInterval:           time.Duration(utils.DefaultLeaderLease) * time.Second,
		MaxResetTSGap:             time.Hour * 24,
		TLSConfig:                 nil,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defaultKsgStorageTSRootPath := path.Join("/pd/1")
	tsoSvcRootPath := "/ms/1/tso"
	electionNamePrefix := "tso-server-1"

	keyspaceGroupManager := NewKeyspaceGroupManager(
		ctx, etcdClient, electionNamePrefix, defaultKsgStorageTSRootPath, tsoSvcRootPath, cfg)
	keyspaceGroupManager.Initialize()

	re.Equal(etcdClient, keyspaceGroupManager.etcdClient)
	re.Equal(electionNamePrefix, keyspaceGroupManager.electionNamePrefix)
	re.Equal(defaultKsgStorageTSRootPath, keyspaceGroupManager.defaultKsgStorageTSRootPath)
	re.Equal(tsoSvcRootPath, keyspaceGroupManager.tsoSvcRootPath)
	re.Equal(cfg, keyspaceGroupManager.cfg)

	am := keyspaceGroupManager.GetAllocatorManager(utils.DefaultKeySpaceGroupID)
	re.False(am.enableLocalTSO)
	re.Equal(utils.DefaultKeySpaceGroupID, am.ksgID)
	re.Equal(utils.DefaultLeaderLease, am.leaderLease)
	re.Equal(time.Hour*24, am.maxResetTSGap())
	re.Equal(defaultKsgStorageTSRootPath, am.rootPath)
	re.Equal(time.Duration(utils.DefaultLeaderLease)*time.Second, am.saveInterval)
	re.Equal(time.Duration(50)*time.Millisecond, am.updatePhysicalInterval)

	keyspaceGroupManager.Close()
}
