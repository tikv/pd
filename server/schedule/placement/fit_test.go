// Copyright 2019 TiKV Project Authors.
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

package placement

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/core"
)

func makeStores() StoreSet {
	stores := core.NewStoresInfo()
	for zone := 1; zone <= 5; zone++ {
		for rack := 1; rack <= 5; rack++ {
			for host := 1; host <= 5; host++ {
				for x := 1; x <= 5; x++ {
					id := uint64(zone*1000 + rack*100 + host*10 + x)
					labels := map[string]string{
						"zone": fmt.Sprintf("zone%d", zone),
						"rack": fmt.Sprintf("rack%d", rack),
						"host": fmt.Sprintf("host%d", host),
						"id":   fmt.Sprintf("id%d", x),
					}
					stores.SetStore(core.NewStoreInfoWithLabel(id, 0, labels))
				}
			}
		}
	}
	return stores
}

// example: "1111_leader,1234,2111_learner"
func makeRegion(def string) *core.RegionInfo {
	var regionMeta metapb.Region
	var leader *metapb.Peer
	for _, peerDef := range strings.Split(def, ",") {
		role, idStr := Follower, peerDef
		if strings.Contains(peerDef, "_") {
			splits := strings.Split(peerDef, "_")
			idStr, role = splits[0], PeerRoleType(splits[1])
		}
		id, _ := strconv.Atoi(idStr)
		peer := &metapb.Peer{Id: uint64(id), StoreId: uint64(id), Role: role.MetaPeerRole()}
		regionMeta.Peers = append(regionMeta.Peers, peer)
		if role == Leader {
			leader = peer
		}
	}
	return core.NewRegionInfo(&regionMeta, leader)
}

// example: "3/voter/zone=zone1+zone2,rack=rack2/zone,rack,host"
//       count role constraints location_labels
func makeRule(def string) *Rule {
	var rule Rule
	splits := strings.Split(def, "/")
	rule.Count, _ = strconv.Atoi(splits[0])
	rule.Role = PeerRoleType(splits[1])
	// only support k=v type constraint
	for _, c := range strings.Split(splits[2], ",") {
		if c == "" {
			break
		}
		kv := strings.Split(c, "=")
		rule.LabelConstraints = append(rule.LabelConstraints, LabelConstraint{
			Key:    kv[0],
			Op:     "in",
			Values: strings.Split(kv[1], "+"),
		})
	}
	rule.LocationLabels = strings.Split(splits[3], ",")
	return &rule
}

func checkPeerMatch(peers []*metapb.Peer, expect string) bool {
	if len(peers) == 0 && expect == "" {
		return true
	}

	m := make(map[string]struct{})
	for _, p := range peers {
		m[strconv.Itoa(int(p.Id))] = struct{}{}
	}
	expects := strings.Split(expect, ",")
	if len(expects) != len(m) {
		return false
	}
	for _, p := range expects {
		delete(m, p)
	}
	return len(m) == 0
}

func TestFitRegion(t *testing.T) {
	re := require.New(t)
	stores := makeStores()

	cases := []struct {
		region   string
		rules    []string
		fitPeers string
	}{
		// test count
		{"1111,1112,1113", []string{"1/voter//"}, "1111"},
		{"1111,1112,1113", []string{"2/voter//"}, "1111,1112"},
		{"1111,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112,1113", []string{"5/voter//"}, "1111,1112,1113"},
		// best location
		{"1111,1112,1113,2111,2222,3222,3333", []string{"3/voter//zone,rack,host"}, "1111,2111,3222"},
		{"1111,1121,1211,2111,2211", []string{"3/voter//zone,rack,host"}, "1111,1211,2111"},
		{"1111,1211,1311,1411,2111,2211,2311,3111", []string{"5/voter//zone,rack,host"}, "1111,1211,2111,2211,3111"},
		// test role match
		{"1111_learner,1112,1113", []string{"1/voter//"}, "1112"},
		{"1111_learner,1112,1113", []string{"2/voter//"}, "1112,1113"},
		{"1111_learner,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112_learner,1121_learner,1122_learner,1131_learner,1132,1141,1142", []string{"3/follower//zone,rack,host"}, "1111,1132,1141"},
		// test 2 rule
		{"1111,1112,1113,1114", []string{"3/voter//", "1/voter/id=id1/"}, "1112,1113,1114/1111"},
		{"1111,2211,3111,3112", []string{"3/voter//zone", "1/voter/rack=rack2/"}, "1111,2211,3111//3112"},
		{"1111,2211,3111,3112", []string{"1/voter/rack=rack2/", "3/voter//zone"}, "2211/1111,3111,3112"},
	}

	for _, cc := range cases {
		region := makeRegion(cc.region)
		var rules []*Rule
		for _, r := range cc.rules {
			rules = append(rules, makeRule(r))
		}
		rf := fitRegion(stores.GetStores(), region, rules)
		expects := strings.Split(cc.fitPeers, "/")
		for i, f := range rf.RuleFits {
			re.True(checkPeerMatch(f.Peers, expects[i]))
		}
		if len(rf.RuleFits) < len(expects) {
			re.True(checkPeerMatch(rf.OrphanPeers, expects[len(rf.RuleFits)]))
		}
	}
}
func TestIsolationScore(t *testing.T) {
	as := assert.New(t)
	stores := makeStores()
	testCases := []struct {
		checker func(interface{}, interface{}, ...interface{}) bool
		peers1  []uint64
		peers2  []uint64
	}{
		{as.Less, []uint64{1111, 1112}, []uint64{1111, 1121}},
		{as.Less, []uint64{1111, 1211}, []uint64{1111, 2111}},
		{as.Less, []uint64{1111, 1211, 1311, 2111, 3111}, []uint64{1111, 1211, 2111, 2211, 3111}},
		{as.Equal, []uint64{1111, 1211, 2111, 2211, 3111}, []uint64{1111, 2111, 2211, 3111, 3211}},
		{as.Greater, []uint64{1111, 1211, 2111, 2211, 3111}, []uint64{1111, 1121, 2111, 2211, 3111}},
	}

	makePeers := func(ids []uint64) []*fitPeer {
		var peers []*fitPeer
		for _, id := range ids {
			peers = append(peers, &fitPeer{
				Peer:  &metapb.Peer{StoreId: id},
				store: stores.GetStore(id),
			})
		}
		return peers
	}

	for _, testCase := range testCases {
		peers1, peers2 := makePeers(testCase.peers1), makePeers(testCase.peers2)
		score1 := isolationScore(peers1, []string{"zone", "rack", "host"})
		score2 := isolationScore(peers2, []string{"zone", "rack", "host"})
		testCase.checker(score1, score2)
	}
}
