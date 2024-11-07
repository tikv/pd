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
	"context"
	"encoding/hex"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

func newTestManager(t *testing.T, enableWitness bool) (endpoint.RuleStorage, *RuleManager) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	var err error
	manager := NewRuleManager(context.Background(), store, nil, mockconfig.NewTestOptions())
	manager.conf.SetEnableWitness(enableWitness)
	err = manager.Initialize(3, []string{"zone", "rack", "host"}, "")
	re.NoError(err)
	return store, manager
}

func TestDefault(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	rules := manager.GetAllRules()
	re.Len(rules, 1)
	re.Equal(DefaultGroupID, rules[0].GroupID)
	re.Equal(DefaultRuleID, rules[0].ID)
	re.Equal(0, rules[0].Index)
	re.Empty(rules[0].StartKey)
	re.Empty(rules[0].EndKey)
	re.Equal(Voter, rules[0].Role)
	re.Equal([]string{"zone", "rack", "host"}, rules[0].LocationLabels)
}

func TestDefault2(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, true)
	rules := manager.GetAllRules()
	re.Len(rules, 2)
	re.Equal(DefaultGroupID, rules[0].GroupID)
	re.Equal(DefaultRuleID, rules[0].ID)
	re.Equal(0, rules[0].Index)
	re.Empty(rules[0].StartKey)
	re.Empty(rules[0].EndKey)
	re.Equal(Voter, rules[0].Role)
	re.Equal([]string{"zone", "rack", "host"}, rules[0].LocationLabels)
	re.Equal(DefaultGroupID, rules[1].GroupID)
	re.Equal(defaultWitnessRuleID, rules[1].ID)
	re.Equal(0, rules[1].Index)
	re.Empty(rules[1].StartKey)
	re.Empty(rules[1].EndKey)
	re.Equal(Voter, rules[1].Role)
	re.True(rules[1].IsWitness)
	re.Equal([]string{"zone", "rack", "host"}, rules[1].LocationLabels)
}

func TestAdjustRule(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	rules := []Rule{
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3},
		{GroupID: "", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3},
		{GroupID: "group", ID: "", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123ab", EndKeyHex: "123abf", Role: Voter, Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "1123abf", Role: Voter, Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123aaa", Role: Voter, Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "master", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 0},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: -1},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3, LabelConstraints: []LabelConstraint{{Op: "foo"}}},
	}
	re.NoError(manager.AdjustRule(&rules[0], "group"))

	re.Equal([]byte{0x12, 0x3a, 0xbc}, rules[0].StartKey)
	re.Equal([]byte{0x12, 0x3a, 0xbf}, rules[0].EndKey)
	re.Error(manager.AdjustRule(&rules[1], ""))

	for i := 2; i < len(rules); i++ {
		re.Error(manager.AdjustRule(&rules[i], "group"))
	}

	manager.SetKeyType(constant.Table.String())
	re.Error(manager.AdjustRule(&Rule{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3}, "group"))

	manager.SetKeyType(constant.Txn.String())
	re.Error(manager.AdjustRule(&Rule{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3}, "group"))

	re.Error(manager.AdjustRule(&Rule{
		GroupID:     "group",
		ID:          "id",
		StartKeyHex: hex.EncodeToString(codec.EncodeBytes([]byte{0})),
		EndKeyHex:   "123abf",
		Role:        Voter,
		Count:       3,
	}, "group"))

	re.Error(manager.AdjustRule(&Rule{
		GroupID:          "tiflash",
		ID:               "id",
		StartKeyHex:      hex.EncodeToString(codec.EncodeBytes([]byte{0})),
		EndKeyHex:        hex.EncodeToString(codec.EncodeBytes([]byte{1})),
		Role:             Learner,
		Count:            1,
		IsWitness:        true,
		LabelConstraints: []LabelConstraint{{Key: "engine", Op: "in", Values: []string{"tiflash"}}},
	}, "tiflash"))
}

func TestLeaderCheck(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	re.Regexp(".*needs at least one leader or voter.*", manager.SetRule(&Rule{GroupID: DefaultGroupID, ID: DefaultRuleID, Role: Learner, Count: 3}).Error())
	re.Regexp(".*define multiple leaders by count 2.*", manager.SetRule(&Rule{GroupID: "g2", ID: "33", Role: Leader, Count: 2}).Error())
	re.Regexp(".*multiple leader replicas.*", manager.Batch([]RuleOp{
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo1", Role: Leader, Count: 1},
			Action: RuleOpAdd,
		},
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo2", Role: Leader, Count: 1},
			Action: RuleOpAdd,
		},
	}).Error())
}

func TestSaveLoad(t *testing.T) {
	re := require.New(t)
	store, manager := newTestManager(t, false)
	rules := []*Rule{
		{GroupID: DefaultGroupID, ID: DefaultRuleID, Role: Voter, Count: 5},
		{GroupID: "foo", ID: "baz", StartKeyHex: "", EndKeyHex: "abcd", Role: Voter, Count: 1},
		{GroupID: "foo", ID: "bar", Role: Learner, Count: 1},
	}
	for _, r := range rules {
		re.NoError(manager.SetRule(r.Clone()))
	}

	m2 := NewRuleManager(context.Background(), store, nil, nil)
	err := m2.Initialize(3, []string{"no", "labels"}, "")
	re.NoError(err)
	re.Len(m2.GetAllRules(), 3)
	re.Equal(m2.GetRule(DefaultGroupID, DefaultRuleID).String(), rules[0].String())
	re.Equal(m2.GetRule("foo", "baz").String(), rules[1].String())
	re.Equal(m2.GetRule("foo", "bar").String(), rules[2].String())
	re.Equal(3, manager.GetRulesCount())
	re.Equal(2, manager.GetGroupsCount())
}

func TestSetAfterGet(t *testing.T) {
	re := require.New(t)
	store, manager := newTestManager(t, false)
	rule := manager.GetRule(DefaultGroupID, DefaultRuleID)
	rule.Count = 1
	manager.SetRule(rule)

	m2 := NewRuleManager(context.Background(), store, nil, nil)
	err := m2.Initialize(100, []string{}, "")
	re.NoError(err)
	rule = m2.GetRule(DefaultGroupID, DefaultRuleID)
	re.Equal(1, rule.Count)
}

func checkRules(t *testing.T, rules []*Rule, expect [][2]string) {
	re := require.New(t)
	re.Len(rules, len(expect))
	for i := range rules {
		re.Equal(expect[i], rules[i].Key())
	}
}

func TestKeys(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	rules := []*Rule{
		{GroupID: "1", ID: "1", Role: Voter, Count: 1, StartKeyHex: "", EndKeyHex: ""},
		{GroupID: "2", ID: "2", Role: Voter, Count: 1, StartKeyHex: "11", EndKeyHex: "ff"},
		{GroupID: "2", ID: "3", Role: Voter, Count: 1, StartKeyHex: "22", EndKeyHex: "dd"},
	}

	toDelete := []RuleOp{}
	for _, r := range rules {
		manager.SetRule(r)
		toDelete = append(toDelete, RuleOp{
			Rule:             r,
			Action:           RuleOpDel,
			DeleteByIDPrefix: false,
		})
	}
	checkRules(t, manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {DefaultGroupID, DefaultRuleID}})
	manager.Batch(toDelete)
	checkRules(t, manager.GetAllRules(), [][2]string{{DefaultGroupID, DefaultRuleID}})

	rules = append(rules, &Rule{GroupID: "3", ID: "4", Role: Voter, Count: 1, StartKeyHex: "44", EndKeyHex: "ee"},
		&Rule{GroupID: "3", ID: "5", Role: Voter, Count: 1, StartKeyHex: "44", EndKeyHex: "dd"})
	manager.SetRules(rules)
	checkRules(t, manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}, {DefaultGroupID, DefaultRuleID}})

	manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	checkRules(t, manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}})

	splitKeys := [][]string{
		{"", "", "11", "22", "44", "dd", "ee", "ff"},
		{"44", "", "dd", "ee", "ff"},
		{"44", "dd"},
		{"22", "ef", "44", "dd", "ee"},
	}
	for _, keys := range splitKeys {
		splits := manager.GetSplitKeys(dhex(keys[0]), dhex(keys[1]))
		re.Len(splits, len(keys)-2)
		for i := range splits {
			re.Equal(dhex(keys[i+2]), splits[i])
		}
	}

	regionKeys := [][][2]string{
		{{"", ""}},
		{{"aa", "bb"}, {"", ""}, {"11", "ff"}, {"22", "dd"}, {"44", "ee"}, {"44", "dd"}},
		{{"11", "22"}, {"", ""}, {"11", "ff"}},
		{{"11", "33"}},
	}
	for _, keys := range regionKeys {
		region := core.NewRegionInfo(&metapb.Region{StartKey: dhex(keys[0][0]), EndKey: dhex(keys[0][1])}, nil)
		rules := manager.GetRulesForApplyRegion(region)
		re.Len(rules, len(keys)-1)
		for i := range rules {
			re.Equal(keys[i+1][0], rules[i].StartKeyHex)
			re.Equal(keys[i+1][1], rules[i].EndKeyHex)
		}
	}

	ruleByKeys := [][]string{ // first is query key, rests are rule keys.
		{"", "", ""},
		{"11", "", "", "11", "ff"},
		{"33", "", "", "11", "ff", "22", "dd"},
	}
	for _, keys := range ruleByKeys {
		rules := manager.GetRulesByKey(dhex(keys[0]))
		re.Len(rules, (len(keys)-1)/2)
		for i := range rules {
			re.Equal(keys[i*2+1], rules[i].StartKeyHex)
			re.Equal(keys[i*2+2], rules[i].EndKeyHex)
		}
	}

	rulesByGroup := [][]string{ // first is group, rests are rule keys.
		{"1", "", ""},
		{"2", "11", "ff", "22", "dd"},
		{"3", "44", "ee", "44", "dd"},
		{"4"},
	}
	for _, keys := range rulesByGroup {
		rules := manager.GetRulesByGroup(keys[0])
		re.Len(rules, (len(keys)-1)/2)
		for i := range rules {
			re.Equal(keys[i*2+1], rules[i].StartKeyHex)
			re.Equal(keys[i*2+2], rules[i].EndKeyHex)
		}
	}
}

func TestDeleteByIDPrefix(t *testing.T) {
	_, manager := newTestManager(t, false)
	manager.SetRules([]*Rule{
		{GroupID: "g1", ID: "foo1", Role: Voter, Count: 1},
		{GroupID: "g2", ID: "foo1", Role: Voter, Count: 1},
		{GroupID: "g2", ID: "foobar", Role: Voter, Count: 1},
		{GroupID: "g2", ID: "baz2", Role: Voter, Count: 1},
	})
	manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	checkRules(t, manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}, {"g2", "foo1"}, {"g2", "foobar"}})

	manager.Batch([]RuleOp{{
		Rule:             &Rule{GroupID: "g2", ID: "foo"},
		Action:           RuleOpDel,
		DeleteByIDPrefix: true,
	}})
	checkRules(t, manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}})
}

func TestRangeGap(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	err := manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	re.Error(err)

	err = manager.SetRule(&Rule{GroupID: DefaultGroupID, ID: "foo", StartKeyHex: "", EndKeyHex: "abcd", Role: Voter, Count: 1})
	re.NoError(err)
	// |-- default --|
	// |-- foo --|
	// still cannot delete default since it will cause ("abcd", "") has no rules inside.
	err = manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	re.Error(err)
	err = manager.SetRule(&Rule{GroupID: DefaultGroupID, ID: "bar", StartKeyHex: "abcd", EndKeyHex: "", Role: Voter, Count: 1})
	re.NoError(err)
	// now default can be deleted.
	err = manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	re.NoError(err)
	// cannot change range since it will cause ("abaa", "abcd") has no rules inside.
	err = manager.SetRule(&Rule{GroupID: DefaultGroupID, ID: "foo", StartKeyHex: "", EndKeyHex: "abaa", Role: Voter, Count: 1})
	re.Error(err)
}

func TestGroupConfig(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	pd1 := &RuleGroup{ID: DefaultGroupID}
	re.Equal(pd1, manager.GetRuleGroup(DefaultGroupID))

	// update group pd
	pd2 := &RuleGroup{ID: DefaultGroupID, Index: 100, Override: true}
	err := manager.SetRuleGroup(pd2)
	re.NoError(err)
	re.Equal(pd2, manager.GetRuleGroup(DefaultGroupID))

	// new group g without config
	err = manager.SetRule(&Rule{GroupID: "g", ID: "1", Role: Voter, Count: 1})
	re.NoError(err)
	g1 := &RuleGroup{ID: "g"}
	re.Equal(g1, manager.GetRuleGroup("g"))
	re.Equal([]*RuleGroup{g1, pd2}, manager.GetRuleGroups())

	// update group g
	g2 := &RuleGroup{ID: "g", Index: 2, Override: true}
	err = manager.SetRuleGroup(g2)
	re.NoError(err)
	re.Equal([]*RuleGroup{g2, pd2}, manager.GetRuleGroups())

	// delete pd group, restore to default config
	err = manager.DeleteRuleGroup(DefaultGroupID)
	re.NoError(err)
	re.Equal([]*RuleGroup{pd1, g2}, manager.GetRuleGroups())

	// delete rule, the group is removed too
	err = manager.DeleteRule(DefaultGroupID, DefaultRuleID)
	re.NoError(err)
	re.Equal([]*RuleGroup{g2}, manager.GetRuleGroups())
}

func TestRuleVersion(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	rule1 := manager.GetRule(DefaultGroupID, DefaultRuleID)
	re.Equal(uint64(0), rule1.Version)
	// create new rule
	newRule := &Rule{GroupID: "g1", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 3}
	err := manager.SetRule(newRule)
	re.NoError(err)
	newRule = manager.GetRule("g1", "id")
	re.Equal(uint64(0), newRule.Version)
	// update rule
	newRule = &Rule{GroupID: "g1", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: Voter, Count: 2}
	err = manager.SetRule(newRule)
	re.NoError(err)
	newRule = manager.GetRule("g1", "id")
	re.Equal(uint64(1), newRule.Version)
	// delete rule
	err = manager.DeleteRule("g1", "id")
	re.NoError(err)
	// recreate new rule
	err = manager.SetRule(newRule)
	re.NoError(err)
	// assert version should be 0 again
	newRule = manager.GetRule("g1", "id")
	re.Equal(uint64(0), newRule.Version)
}

func TestCheckApplyRules(t *testing.T) {
	re := require.New(t)
	err := checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
	})
	re.NoError(err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Voter,
			Count: 1,
		},
	})
	re.NoError(err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Voter,
			Count: 1,
		},
	})
	re.NoError(err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 3,
		},
	})
	re.Regexp("multiple leader replicas", err.Error())

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Leader,
			Count: 1,
		},
	})
	re.Regexp("multiple leader replicas", err.Error())

	err = checkApplyRules([]*Rule{
		{
			Role:  Learner,
			Count: 1,
		},
		{
			Role:  Follower,
			Count: 1,
		},
	})
	re.Regexp("needs at least one leader or voter", err.Error())
}

func TestCacheManager(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t, false)
	manager.conf.SetPlacementRulesCacheEnabled(true)
	rules := addExtraRules(0)
	re.NoError(manager.SetRules(rules))
	stores := makeStores()

	regionMeta := &metapb.Region{
		Id:          1,
		StartKey:    []byte(""),
		EndKey:      []byte(""),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 0, Version: 0},
		Peers: []*metapb.Peer{
			{Id: 11, StoreId: 1111, Role: metapb.PeerRole_Voter},
			{Id: 12, StoreId: 2111, Role: metapb.PeerRole_Voter},
			{Id: 13, StoreId: 3111, Role: metapb.PeerRole_Voter},
		},
	}
	region := core.NewRegionInfo(regionMeta, regionMeta.Peers[0])
	fit := manager.FitRegion(stores, region)
	manager.SetRegionFitCache(region, fit)
	// bestFit is not stored when the total number of hits is insufficient.
	for i := 1; i < minHitCountToCacheHit/2; i++ {
		manager.FitRegion(stores, region)
		re.True(manager.IsRegionFitCached(stores, region))
		cache := manager.cache.regionCaches[1]
		re.Equal(uint32(i), cache.hitCount)
		re.Nil(cache.bestFit)
	}
	// Store bestFit when the total number of hits is sufficient.
	for range minHitCountToCacheHit {
		manager.FitRegion(stores, region)
	}
	cache := manager.cache.regionCaches[1]
	re.Equal(uint32(minHitCountToCacheHit), cache.hitCount)
	re.NotNil(cache.bestFit)
	// Cache invalidation after change
	regionMeta.Peers[2] = &metapb.Peer{Id: 14, StoreId: 4111, Role: metapb.PeerRole_Voter}
	region = core.NewRegionInfo(regionMeta, regionMeta.Peers[0])
	re.False(manager.IsRegionFitCached(stores, region))
}

func dhex(hk string) []byte {
	k, err := hex.DecodeString(hk)
	if err != nil {
		panic("decode fail")
	}
	return k
}
