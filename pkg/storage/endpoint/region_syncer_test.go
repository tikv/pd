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

package endpoint

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// newMemStorageEndpoint returns a StorageEndpoint backed by an in-memory KV,
// for tests that exercise endpoint encode/decode round-trips without etcd.
func newMemStorageEndpoint() *StorageEndpoint {
	return NewStorageEndpoint(kv.NewMemoryKV(), nil)
}

// TestRegionSyncerCommittedRegionCountAbsent verifies the back-compat path the
// leader-election gate relies on: an unwritten key (fresh or pre-upgrade
// cluster) must read back as (0, nil) rather than an error, so the gate treats
// it as "no committed regions" and allows the campaign.
func TestRegionSyncerCommittedRegionCountAbsent(t *testing.T) {
	re := require.New(t)
	se := newMemStorageEndpoint()

	count, err := se.LoadRegionSyncerCommittedRegionCount()
	re.NoError(err)
	re.Equal(uint64(0), count)
}

// TestRegionSyncerCommittedRegionCountRoundTrip verifies Save/Load preserves the
// value across the uint64<->string boundary, including the zero (empty cluster)
// and max-uint64 edges, and that a later Save overwrites the prior value.
func TestRegionSyncerCommittedRegionCountRoundTrip(t *testing.T) {
	re := require.New(t)
	se := newMemStorageEndpoint()

	for _, want := range []uint64{0, 1, 110, math.MaxUint64} {
		re.NoError(se.SaveRegionSyncerCommittedRegionCount(want))
		got, err := se.LoadRegionSyncerCommittedRegionCount()
		re.NoError(err)
		re.Equal(want, got)
	}

	// A subsequent write replaces the prior value (counts shrink on merges).
	re.NoError(se.SaveRegionSyncerCommittedRegionCount(42))
	got, err := se.LoadRegionSyncerCommittedRegionCount()
	re.NoError(err)
	re.Equal(uint64(42), got)
}

// TestRegionSyncerCommittedRegionCountCorruptValue verifies a non-numeric value
// (corruption or a manual edit) surfaces as an error so the gate falls back to
// allowing the campaign rather than silently treating it as zero.
func TestRegionSyncerCommittedRegionCountCorruptValue(t *testing.T) {
	re := require.New(t)
	se := newMemStorageEndpoint()

	re.NoError(se.Save(keypath.RegionSyncerCommittedRegionCountPath(), "not-a-number"))
	_, err := se.LoadRegionSyncerCommittedRegionCount()
	re.Error(err)
}
