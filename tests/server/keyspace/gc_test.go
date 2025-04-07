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

package keyspace

import (
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

func (suite *keyspaceTestSuite) TestGCBasic() {
	manager := suite.manager
	var (
		threeHoursAgo = time.Now().Add(-3 * time.Hour).Unix()
		oneHourAgo    = time.Now().Add(-time.Hour).Unix()
		err           error
	)

	// Set ks1 to be archived three hours ago.
	ks1 := suite.createArchivedKeyspace("ks1", threeHoursAgo)
	// Set ks2 to be archived one hour ago.
	ks2 := suite.createArchivedKeyspace("ks2", oneHourAgo)

	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/keyspace/doGC", "return(true)"))
	manager.UpdateConfig(&config.KeyspaceConfig{
		GCEnable:      true,
		GCRunInterval: typeutil.NewDuration(time.Second),
		// Here we archive everything archived two hours ago, which should gc ks1 but not ks2.
		GCLifeTime: typeutil.NewDuration(2 * time.Hour),
	})
	// Assert that ks1 will be gc eventually.
	suite.Eventually(func() bool {
		ks1, err = manager.LoadKeyspaceByID(ks1.GetId())
		suite.NoError(err)
		return ks1.GetState() == keyspacepb.KeyspaceState_TOMBSTONE
	}, 5*time.Second, time.Second)
	// After ks1 has been garbage collected, ks2 should still be archived.
	ks2, err = manager.LoadKeyspaceByID(ks2.GetId())
	suite.NoError(err)
	suite.True(ks2.GetState() == keyspacepb.KeyspaceState_ARCHIVED)
	manager.UpdateConfig(&config.KeyspaceConfig{
		GCEnable:      true,
		GCRunInterval: typeutil.NewDuration(time.Second),
		// Here we archive everything archived 1 second ago, which should gc ks2 as well.
		GCLifeTime: typeutil.NewDuration(time.Second),
	})
	suite.Eventually(func() bool {
		ks2, err = manager.LoadKeyspaceByID(ks2.GetId())
		suite.NoError(err)
		return ks2.GetState() == keyspacepb.KeyspaceState_TOMBSTONE
	}, 5*time.Second, time.Second)
	// Update GCLifeTime so that ks2 should also be archived eventually.
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/keyspace/doGC"))
}

// createArchivedKeyspace creates an archived keyspace with the given stateChangedAt.
func (suite *keyspaceTestSuite) createArchivedKeyspace(name string, stateChangedAt int64) *keyspacepb.KeyspaceMeta {
	manager := suite.manager
	meta, err := manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: name,
	})
	suite.NoError(err)
	_, err = manager.UpdateKeyspaceState(meta.GetName(), keyspacepb.KeyspaceState_DISABLED, stateChangedAt)
	suite.NoError(err)
	meta, err = manager.UpdateKeyspaceState(meta.GetName(), keyspacepb.KeyspaceState_ARCHIVED, stateChangedAt)
	suite.NoError(err)
	return meta
}
