// Copyright 2022 TiKV Project Authors.
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

package core

import (
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	// RegionFactory returns new region.
	RegionFactory = func() *metapb.Region { return &metapb.Region{} }
	// RegionPeerFactory returns new region peer.
	RegionPeerFactory = func() *metapb.Peer { return &metapb.Peer{} }
	// RegionHeartbeatResponseFactory returns new region heartbeat response.
	RegionHeartbeatResponseFactory = func() *pdpb.RegionHeartbeatResponse { return &pdpb.RegionHeartbeatResponse{} }
	// PeerStatsFactory returns new peer stats.
	PeerStatsFactory = func() *pdpb.PeerStats { return &pdpb.PeerStats{} }

	// StoreFactory returns new store.
	StoreFactory = func() *metapb.Store { return &metapb.Store{} }
	// StoreStatsFactory returns new store stats.
	StoreStatsFactory = func() *pdpb.StoreStats { return &pdpb.StoreStats{} }

	// KeyDictionaryFactory returns new key dictionary.
	KeyDictionaryFactory = func() *encryptionpb.KeyDictionary { return &encryptionpb.KeyDictionary{} }
	// MemberFactory returns new member.
	MemberFactory = func() *pdpb.Member { return &pdpb.Member{} }
	// ClusterFactory returns new cluster.
	ClusterFactory = func() *metapb.Cluster { return &metapb.Cluster{} }
	// TimeIntervalFactory returns new time interval.
	TimeIntervalFactory = func() *pdpb.TimeInterval { return &pdpb.TimeInterval{} }
	// QueryStatsFactory returns new query stats.
	QueryStatsFactory = func() *pdpb.QueryStats { return &pdpb.QueryStats{} }
)
