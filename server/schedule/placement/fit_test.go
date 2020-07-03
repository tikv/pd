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

func (s *testFitSuite) makeStores() map[uint64]*core.StoreInfo {
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
	return stores
}

func (s *testFitSuite) TestFitByLocation(c *C) {
	stores := s.makeStores()

	type Case struct {
		// peers info
		peerStoreID []uint64
		peerRole    []PeerRoleType // default: all Followers
		// rule
		locationLabels string       // default: ""
		count          int          // default: len(peerStoreID)
		role           PeerRoleType // default: Voter
		// expect result:
		expectedPeers []uint64 // default: same as peerStoreID
	}

	cases := []Case{
		// test count
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 1, expectedPeers: []uint64{1111}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 2, expectedPeers: []uint64{1111, 1112}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 3, expectedPeers: []uint64{1111, 1112, 1113}},
		{peerStoreID: []uint64{1111, 1112, 1113}, count: 5, expectedPeers: []uint64{1111, 1112, 1113}},
		// test isolation level
		{peerStoreID: []uint64{1111}, locationLabels: "zone,rack,host"},
		{peerStoreID: []uint64{1111}, locationLabels: "zone,rack"},
		{peerStoreID: []uint64{1111}, locationLabels: "zone"},
		{peerStoreID: []uint64{1111}, locationLabels: ""},
		{peerStoreID: []uint64{1111, 2111}, locationLabels: "zone,rack,host"},
		{peerStoreID: []uint64{1111, 2222, 3333}, locationLabels: "zone,rack,host"},
		{peerStoreID: []uint64{1111, 1211, 3111}, locationLabels: "zone,rack,host"},
		{peerStoreID: []uint64{1111, 1121, 3111}, locationLabels: "zone,rack,host"},
		{peerStoreID: []uint64{1111, 1121, 1122}, locationLabels: "zone,rack,host"},
		// test best location
		{
			peerStoreID:    []uint64{1111, 1112, 1113, 2111, 2222, 3222, 3333},
			locationLabels: "zone,rack,host",
			count:          3,
			expectedPeers:  []uint64{1111, 2111, 3222},
		},
		{
			peerStoreID:    []uint64{1111, 1121, 1211, 2111, 2211},
			locationLabels: "zone,rack,host",
			count:          3,
			expectedPeers:  []uint64{1111, 1211, 2111},
		},
		{
			peerStoreID:    []uint64{1111, 1211, 1311, 1411, 2111, 2211, 2311, 3111},
			locationLabels: "zone,rack,host",
			count:          5,
			expectedPeers:  []uint64{1111, 1211, 2111, 2211, 3111},
		},
		// test role match
		{
			peerStoreID:   []uint64{1111, 1112, 1113},
			peerRole:      []PeerRoleType{Learner, Follower, Follower},
			count:         1,
			expectedPeers: []uint64{1112},
		},
		{
			peerStoreID:   []uint64{1111, 1112, 1113},
			peerRole:      []PeerRoleType{Learner, Follower, Follower},
			count:         2,
			expectedPeers: []uint64{1112, 1113},
		},
		{
			peerStoreID:   []uint64{1111, 1112, 1113},
			peerRole:      []PeerRoleType{Learner, Follower, Follower},
			count:         3,
			expectedPeers: []uint64{1112, 1113, 1111},
		},
		{
			peerStoreID:    []uint64{1111, 1112, 1121, 1122, 1131, 1132, 1141, 1142},
			peerRole:       []PeerRoleType{Follower, Learner, Learner, Learner, Learner, Follower, Follower, Follower},
			locationLabels: "zone,rack,host",
			count:          3,
			expectedPeers:  []uint64{1111, 1132, 1141},
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
	}
}

func (s *testFitSuite) TestIsolationScore(c *C) {
	stores := s.makeStores()
	testCases := []struct {
		peers1 []uint64
		Checker
		peers2 []uint64
	}{
		{[]uint64{1111, 1112}, Less, []uint64{1111, 1121}},
		{[]uint64{1111, 1211}, Less, []uint64{1111, 2111}},
		{[]uint64{1111, 1211, 1311, 2111, 3111}, Less, []uint64{1111, 1211, 2111, 2211, 3111}},
		{[]uint64{1111, 1211, 2111, 2211, 3111}, Equals, []uint64{1111, 2111, 2211, 3111, 3211}},
		{[]uint64{1111, 1211, 2111, 2211, 3111}, Greater, []uint64{1111, 1121, 2111, 2211, 3111}},
	}

	makePeers := func(ids []uint64) []*fitPeer {
		var peers []*fitPeer
		for _, id := range ids {
			peers = append(peers, &fitPeer{
				Peer:  &metapb.Peer{StoreId: id},
				store: stores[id],
			})
		}
		return peers
	}

	for _, tc := range testCases {
		peers1, peers2 := makePeers(tc.peers1), makePeers(tc.peers2)
		score1 := isolationScore(peers1, []string{"zone", "rack", "host"})
		score2 := isolationScore(peers2, []string{"zone", "rack", "host"})
		c.Assert(score1, tc.Checker, score2)
	}
}
