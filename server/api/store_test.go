// Copyright 2025 TiKV Project Authors.
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
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/server/config"
)

func TestUrlStoreFilter(t *testing.T) {
	stores := []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        4,
			Address:   "tikv4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:        6,
			Address:   "tikv6",
			State:     metapb.StoreState_Offline,
			NodeState: metapb.NodeState_Removing,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:        7,
			Address:   "tikv7",
			State:     metapb.StoreState_Tombstone,
			NodeState: metapb.NodeState_Removed,
			Version:   "2.0.0",
		},
	}
	re := require.New(t)
	testCases := []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: stores[:3],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: stores[3:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2&state=1",
			want: stores[2:],
		},
	}

	for _, testCase := range testCases {
		uu, err := url.Parse(testCase.u)
		re.NoError(err)
		f, err := NewStoreStateFilter(uu)
		re.NoError(err)
		re.Equal(testCase.want, f.Filter(stores))
	}

	u, err := url.Parse("http://localhost:2379/pd/api/v1/stores?state=foo")
	re.NoError(err)
	_, err = NewStoreStateFilter(u)
	re.Error(err)

	u, err = url.Parse("http://localhost:2379/pd/api/v1/stores?state=999999")
	re.NoError(err)
	_, err = NewStoreStateFilter(u)
	re.Error(err)
}

func TestDownState(t *testing.T) {
	re := require.New(t)
	store := core.NewStoreInfo(
		&metapb.Store{
			State: metapb.StoreState_Up,
		},
		core.SetStoreStats(&pdpb.StoreStats{}),
		core.SetLastHeartbeatTS(time.Now()),
	)

	config := config.NewConfig()
	re.NoError(config.Adjust(nil, false))
	storeInfo := response.BuildStoreInfo(&config.Schedule, store)
	re.Equal(metapb.StoreState_Up.String(), storeInfo.Store.StateName)

	newStore := store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Minute * 2)))
	storeInfo = response.BuildStoreInfo(&config.Schedule, newStore)
	re.Equal(response.DisconnectedName, storeInfo.Store.StateName)

	newStore = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Hour * 2)))
	storeInfo = response.BuildStoreInfo(&config.Schedule, newStore)
	re.Equal(response.DownStateName, storeInfo.Store.StateName)
}
