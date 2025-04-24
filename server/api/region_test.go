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
	"context"
	"encoding/json"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
)

func TestTopN(t *testing.T) {
	re := require.New(t)
	writtenBytes := []uint64{10, 10, 9, 5, 3, 2, 2, 1, 0, 0}
	for n := 0; n <= len(writtenBytes)+1; n++ {
		regions := make([]*core.RegionInfo, 0, len(writtenBytes))
		for _, i := range rand.Perm(len(writtenBytes)) {
			id := uint64(i + 1)
			region := core.NewTestRegionInfo(id, id, nil, nil, core.SetWrittenBytes(writtenBytes[i]))
			regions = append(regions, region)
		}
		topN := TopNRegions(regions, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, n)
		if n > len(writtenBytes) {
			re.Len(topN, len(writtenBytes))
		} else {
			re.Len(topN, n)
		}
		for i := range topN {
			re.Equal(writtenBytes[i], topN[i].GetBytesWritten())
		}
	}
}

func TestRegionsInfoMarshal(t *testing.T) {
	re := require.New(t)
	regionWithNilPeer := core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1})
	core.SetPeers([]*metapb.Peer{{Id: 2}, nil})(regionWithNilPeer)
	cases := [][]*core.RegionInfo{
		{},
		{
			// leader is nil
			core.NewRegionInfo(&metapb.Region{Id: 1}, nil),
			// Peers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetPeers([]*metapb.Peer{})),
			// There is nil peer in peers.
			regionWithNilPeer,
		},
		{
			// PendingPeers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithPendingPeers([]*metapb.Peer{})),
			// There is nil peer in peers.
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithPendingPeers([]*metapb.Peer{nil})),
		},
		{
			// DownPeers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithDownPeers([]*pdpb.PeerStats{})),
			// There is nil peer in peers.
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithDownPeers([]*pdpb.PeerStats{{Peer: nil}})),
		},
		{
			// Buckets is nil
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetBuckets(nil)),
			// Buckets is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetBuckets(&metapb.Buckets{})),
		},
		{
			core.NewRegionInfo(&metapb.Region{Id: 1, StartKey: []byte{}, EndKey: []byte{},
				RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}},
				&metapb.Peer{Id: 1}, core.SetCPUUsage(10),
				core.SetApproximateKeys(10), core.SetApproximateSize(10),
				core.SetWrittenBytes(10), core.SetReadBytes(10),
				core.SetReadKeys(10), core.SetWrittenKeys(10)),
		},
	}
	regionsInfo := &response.RegionsInfo{}
	for _, regions := range cases {
		b, err := response.MarshalRegionsInfoJSON(context.Background(), regions)
		re.NoError(err)
		err = json.Unmarshal(b, regionsInfo)
		re.NoError(err)
	}
}

func BenchmarkHexRegionKey(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for range b.N {
		_ = core.HexRegionKey(key)
	}
}

func BenchmarkHexRegionKeyStr(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for range b.N {
		_ = core.HexRegionKeyStr(key)
	}
}
