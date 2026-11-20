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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func TestSetOnStoreTombstonedReplaysLoadedStores(t *testing.T) {
	cluster := core.NewBasicCluster()
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Removed}))
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 2, NodeState: metapb.NodeState_Serving}))
	w := &Watcher{basicCluster: cluster}
	var removed []uint64
	w.SetOnStoreTombstoned(func(storeID uint64) { removed = append(removed, storeID) })
	require.Equal(t, []uint64{1}, removed)
}
