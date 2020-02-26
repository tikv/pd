// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"fmt"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
)

var _ = Suite(&testFitSuite{})

type testFitSuite struct{}

func (s *testFitSuite) TestFitByLocation(c *C) {
	stores := make(map[uint64]*core.StoreInfo)
	for zone := 1; zone <= 5; zone++ {
		for rack := 1; rack <= 5; rack++ {
			for host := 1; host <= 5; host++ {
				for x := 1; x <= 5; x++ {
					id := uint64(zone*1000 + rack*100 + host*10 + x)
					labels := map[string]string{
						"zone": fmt.Sprintf("zone%d", zone),
						"rack": fmt.Sprintf("rack%d", rack),
						"host": fmt.Sprintf("host%d", host),
					}
					stores[id] = core.NewStoreInfoWithLabel(id, 0, labels)
				}
			}
		}
	}

	type Case struct {
		// peers info
		peerStoreID []uint64
		peerRole    []PeerRoleType // default: all Followers
		// rule
		locationLabels string       // default: ""
		count          int          // default: len(peerStoreID)
		role           PeerRoleType // default: Voter
		// expect result:
		expectedPeers          []uint64 // default: same as peerStoreID
		expectedIsolationLevel int      // default: 0
	}

	cases := []Case{
		// test count
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 1, expectedPeers: []uint64{1111}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 2, expectedPeers: []uint64{1111, 1112}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 3, expectedPeers: []uint64{1111, 1112, 1113}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 5, expectedPeers: []uint64{1111, 1112, 1113}},
		// test isolation level
		{peerStoreID: []uint64{1111}, locationLabels: "zone,rack,host", expectedIsolationLevel: 3},
		{peerStoreID: []uint64{1111}, locationLabels: "zone,rack", expectedIsolationLevel: 2},
		{peerStoreID: []uint64{1111}, locationLabels: "zone", expectedIsolationLevel: 1},
		{peerStoreID: []uint64{1111}, locationLabels: "", expectedIsolationLevel: 0},
		{peerStoreID: []uint64{1111, 2111}, locationLabels: "zone,rack,host", expectedIsolationLevel: 3},
		{peerStoreID: []uint64{1111, 2222, 3333}, locationLabels: "zone,rack,host", expectedIsolationLevel: 3},
		{peerStoreID: []uint64{1111, 1211, 3111}, locationLabels: "zone,rack,host", expectedIsolationLevel: 2},
		{peerStoreID: []uint64{1111, 1121, 3111}, locationLabels: "zone,rack,host", expectedIsolationLevel: 1},
		{peerStoreID: []uint64{1111, 1121, 1122}, locationLabels: "zone,rack,host", expectedIsolationLevel: 0},
		// test best location
		{
			peerStoreID:            []uint64{1111, 1112, 1113, 2111, 2222, 3222, 3333},
			locationLabels:         "zone,rack,host",
			count:                  3,
			expectedPeers:          []uint64{1111, 2111, 3222},
			expectedIsolationLevel: 3,
		},
		{
			peerStoreID:            []uint64{1111, 1121, 1211, 2111, 2211},
			locationLabels:         "zone,rack,host",
			count:                  3,
			expectedPeers:          []uint64{1111, 1211, 2111},
			expectedIsolationLevel: 2,
		},
		// test role match
		{
			peerStoreID:            []uint64{1111, 1112, 1113},
			peerRole:               []PeerRoleType{Learner, Follower, Follower},
			count:                  1,
			expectedPeers:          []uint64{1112},
			expectedIsolationLevel: 0,
		},
		{
			peerStoreID:            []uint64{1111, 1112, 1113},
			peerRole:               []PeerRoleType{Learner, Follower, Follower},
			count:                  2,
			expectedPeers:          []uint64{1112, 1113},
			expectedIsolationLevel: 0,
		},
		{
			peerStoreID:            []uint64{1111, 1112, 1113},
			peerRole:               []PeerRoleType{Learner, Follower, Follower},
			count:                  3,
			expectedPeers:          []uint64{1112, 1113, 1111},
			expectedIsolationLevel: 0,
		},
		{
			peerStoreID:            []uint64{1111, 1112, 1121, 1122, 1131, 1132, 1141, 1142},
			peerRole:               []PeerRoleType{Follower, Learner, Learner, Learner, Learner, Follower, Follower, Follower},
			locationLabels:         "zone,rack,host",
			count:                  3,
			expectedPeers:          []uint64{1111, 1132, 1141},
			expectedIsolationLevel: 1,
		},
	}

	for _, cc := range cases {
		var peers []*fitPeer
		for i := range cc.peerStoreID {
			role := Follower
			if i < len(cc.peerRole) {
				role = cc.peerRole[i]
			}
			peers = append(peers, &fitPeer{
				Peer:     &metapb.Peer{Id: cc.peerStoreID[i], StoreId: cc.peerStoreID[i], IsLearner: role == Learner},
				store:    stores[cc.peerStoreID[i]],
				isLeader: role == Leader,
			})
		}

		rule := &Rule{Count: len(cc.peerStoreID), Role: Voter}
		if len(cc.locationLabels) > 0 {
			rule.LocationLabels = strings.Split(cc.locationLabels, ",")
		}
		if cc.role != "" {
			rule.Role = cc.role
		}
		if cc.count > 0 {
			rule.Count = cc.count
		}
		c.Log("Peers:", peers)
		c.Log("rule:", rule)
		ruleFit := fitRule(peers, rule)
		selectedIDs := make([]uint64, 0)
		for _, p := range ruleFit.Peers {
			selectedIDs = append(selectedIDs, p.GetId())
		}
		sort.Slice(selectedIDs, func(i, j int) bool { return selectedIDs[i] < selectedIDs[j] })
		expectedPeers := cc.expectedPeers
		if len(expectedPeers) == 0 {
			expectedPeers = cc.peerStoreID
		}
		sort.Slice(expectedPeers, func(i, j int) bool { return expectedPeers[i] < expectedPeers[j] })
		c.Assert(selectedIDs, DeepEquals, expectedPeers)
		c.Assert(ruleFit.IsolationLevel, Equals, cc.expectedIsolationLevel)
	}
}
