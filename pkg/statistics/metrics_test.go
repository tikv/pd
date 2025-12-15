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

package statistics

import (
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
)

func TestDeleteClusterStatusMetrics(t *testing.T) {
	re := require.New(t)
	store := core.NewStoreInfo(&metapb.Store{Id: 9876543210})
	other := core.NewStoreInfo(&metapb.Store{Id: 9876543211})
	id := strconv.FormatUint(store.GetID(), 10)
	otherID := strconv.FormatUint(other.GetID(), 10)

	clusterStatusGauge.WithLabelValues(clusterStatusStoreTombstoneCount, id).Set(1)
	clusterStatusGauge.WithLabelValues(clusterStatusStorageSize, id).Set(1)
	clusterStatusGauge.WithLabelValues(clusterStatusStoreTombstoneCount, otherID).Set(1)
	DeleteClusterStatusMetrics(store)

	re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, id))
	re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStorageSize, id))
	re.True(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, otherID))
}
