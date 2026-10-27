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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestStoreCapabilityAdmission(t *testing.T) {
	var missing *StoreInfo
	require.False(t, missing.SupportsPeerCapability(false))
	require.False(t, missing.SupportsPeerCapability(true))
	for _, tc := range []struct {
		capability    string
		full, witness bool
	}{
		{"", true, true}, {FullStoreCapability, true, false},
		{LogOnlyStoreCapability, false, true}, {"log-only-v2", false, false},
	} {
		t.Run(tc.capability, func(t *testing.T) {
			labels := map[string]string{}
			if tc.capability != "" {
				labels[StoreCapabilityLabel] = tc.capability
			}
			store := NewStoreInfoWithLabel(1, labels)
			require.Equal(t, tc.full, store.SupportsPeerCapability(false))
			require.Equal(t, tc.witness, store.SupportsPeerCapability(true))
		})
	}
	_, err := ParseStoreCapability(&metapb.Store{Labels: []*metapb.StoreLabel{
		{Key: StoreCapabilityLabel, Value: FullStoreCapability},
		{Key: "CSE-STORE-CAPABILITY", Value: FullStoreCapability},
	}})
	require.ErrorContains(t, err, "duplicate")
}

func TestDedicatedLogOnlyActivationRequiresEveryMemberProtocol(t *testing.T) {
	cluster := NewBasicCluster()
	for _, id := range []uint64{1, 2, 3, 4} {
		labels := []*metapb.StoreLabel{{Key: StoreCapabilityLabel, Value: FullStoreCapability}}
		if id == 3 {
			labels[0].Value = LogOnlyStoreCapability
		}
		cluster.PutStore(NewStoreInfo(&metapb.Store{Id: id, Labels: labels, LogOnlyProtocolVersion: LogOnlyProtocolVersion}))
	}
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
	require.NoError(t, CheckLogOnlyPeerProtocols(cluster, peers))
	cluster.PutStore(cluster.GetStore(1).Clone(SetStoreLogOnlyProtocolVersion(0)))
	require.ErrorContains(t, CheckLogOnlyPeerProtocols(cluster, peers), "Store 1")
	// Returning to an ordinary Full group does not require legacy Stores to v2.
	require.NoError(t, CheckLogOnlyPeerProtocols(cluster, peers[:2]))
	cluster.PutStore(cluster.GetStore(1).Clone(SetStoreLogOnlyProtocolVersion(LogOnlyProtocolVersion)))
	cluster.PutStore(cluster.GetStore(4).Clone(SetStoreLogOnlyProtocolVersion(0)))
	withReader := append(peers, &metapb.Peer{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner})
	require.ErrorContains(t, CheckLogOnlyPeerProtocols(cluster, withReader), "Store 4")
}
