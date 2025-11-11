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

package rule

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type placementRuleWatcherTestSuite struct {
	suite.Suite
	ctx     context.Context
	client  *clientv3.Client
	cleanup func()

	manager     *placement.RuleManager
	coordinator *schedule.Coordinator
	rw          *PlacementRuleWatcher
}

func TestPlacementRuleWatcherSuite(t *testing.T) {
	suite.Run(t, new(placementRuleWatcherTestSuite))
}

func (s *placementRuleWatcherTestSuite) SetupSuite() {
	re := s.Require()
	s.ctx, s.client, s.cleanup = setupEtcd(re)
}

func (s *placementRuleWatcherTestSuite) TearDownSuite() {
	s.cleanup()
}

func (s *placementRuleWatcherTestSuite) SetupTest() {
	re := s.Require()
	// Start RuleManager and Coordinator
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	s.manager = placement.NewRuleManager(s.ctx, store, nil, nil)
	err := s.manager.Initialize(3, []string{"zone", "rack", "host"}, "", false)
	re.NoError(err)
	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(s.ctx, opts)
	s.coordinator = schedule.NewCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster, true))
	s.coordinator.Run()
}

func (s *placementRuleWatcherTestSuite) startWatcher() {
	re := s.Require()
	rw, err := NewPlacementRuleWatcher(s.ctx, s.client, s.coordinator.GetCheckerController(), s.manager)
	re.NoError(err)
	s.rw = rw
}

func (s *placementRuleWatcherTestSuite) TearDownTest() {
	if s.rw != nil {
		s.rw.Close()
	}
	s.coordinator.Stop()

	re := s.Require()
	_, err := s.client.Delete(s.ctx, keypath.RulesPathPrefix(), clientv3.WithPrefix())
	re.NoError(err)
	_, err = s.client.Delete(s.ctx, keypath.RuleGroupPathPrefix(), clientv3.WithPrefix())
	re.NoError(err)
}

func (s *placementRuleWatcherTestSuite) TestEvents() {
	re := s.Require()
	s.startWatcher()

	// Test PUT event
	rule1 := &placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "test", StartKey: []byte("a"), EndKey: []byte("b"), Role: placement.Voter, Count: 3,
	}
	rule1JSON, err := json.Marshal(rule1)
	re.NoError(err)
	rule1Key := keypath.RuleKeyPath(rule1.GroupID + "-" + rule1.ID)
	_, err = s.client.Put(s.ctx, rule1Key, string(rule1JSON))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.manager.GetRule(rule1.GroupID, rule1.ID) != nil
	})
	ruleFromManager := s.manager.GetRule(rule1.GroupID, rule1.ID)
	re.NotNil(ruleFromManager)
	re.Equal(rule1.Count, ruleFromManager.Count)

	// Test DELETE event
	_, err = s.client.Delete(s.ctx, rule1Key)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.manager.GetRule(rule1.GroupID, rule1.ID) == nil
	})
	re.Nil(s.manager.GetRule(rule1.GroupID, rule1.ID))
}

func (s *placementRuleWatcherTestSuite) TestInvalidDeleteKey() {
	re := s.Require()
	s.startWatcher()

	// Put and Delete an invalid key
	invalidKey := keypath.RulesPathPrefix() + "invalidkey-no-dash"
	_, err := s.client.Put(s.ctx, invalidKey, "bogus-value")
	re.NoError(err)
	_, err = s.client.Delete(s.ctx, invalidKey)
	re.NoError(err)

	// Check if the watcher loop is still alive
	ruleCanary := &placement.Rule{
		GroupID: "pd", ID: "canary", StartKey: []byte("c"), EndKey: []byte("d"), Role: placement.Voter, Count: 1,
	}
	canaryJSON, err := json.Marshal(ruleCanary)
	re.NoError(err)
	canaryKey := keypath.RuleKeyPath(ruleCanary.GroupID + "-" + ruleCanary.ID)

	// Put the canary rule
	_, err = s.client.Put(s.ctx, canaryKey, string(canaryJSON))
	re.NoError(err)

	// Wait for the watcher to update the RuleManager with the canary rule
	testutil.Eventually(re, func() bool {
		return s.manager.GetRule(ruleCanary.GroupID, ruleCanary.ID) != nil
	})
	re.NotNil(s.manager.GetRule(ruleCanary.GroupID, ruleCanary.ID), "watcher loop should be alive and process new event")
}

func (s *placementRuleWatcherTestSuite) TestRuleGroupEvents() {
	re := s.Require()
	s.startWatcher()

	group := &placement.RuleGroup{
		ID:       "test-group",
		Index:    1,
		Override: true,
	}
	groupJSON, err := json.Marshal(group)
	re.NoError(err)
	groupKey := keypath.RuleGroupIDPath(group.ID)

	// 1. Test PUT RuleGroup event
	_, err = s.client.Put(s.ctx, groupKey, string(groupJSON))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		g := s.manager.GetRuleGroup(group.ID)
		return g != nil && g.Override == group.Override && g.Index == group.Index
	})

	// 2. Test DELETE RuleGroup event
	_, err = s.client.Delete(s.ctx, groupKey)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.manager.GetRuleGroup(group.ID) == nil
	})
}

func (s *placementRuleWatcherTestSuite) TestInitialLoad() {
	re := s.Require()

	rule1 := &placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "pre-exist-rule", StartKey: []byte("a"), EndKey: []byte("b"), Role: placement.Voter, Count: 3,
	}
	rule1JSON, err := json.Marshal(rule1)
	re.NoError(err)
	rule1Key := keypath.RuleKeyPath(rule1.GroupID + "-" + rule1.ID)
	_, err = s.client.Put(s.ctx, rule1Key, string(rule1JSON))
	re.NoError(err)

	group1 := &placement.RuleGroup{
		ID: "pre-exist-group", Index: 1, Override: true,
	}
	group1JSON, err := json.Marshal(group1)
	re.NoError(err)
	group1Key := keypath.RuleGroupIDPath(group1.ID)
	_, err = s.client.Put(s.ctx, group1Key, string(group1JSON))
	re.NoError(err)

	s.startWatcher()

	ruleFromManager := s.manager.GetRule(rule1.GroupID, rule1.ID)
	re.NotNil(ruleFromManager)
	re.Equal(rule1.Count, ruleFromManager.Count)

	groupFromManager := s.manager.GetRuleGroup(group1.ID)
	re.NotNil(groupFromManager)
	re.Equal(group1.Override, groupFromManager.Override)
}

func (s *placementRuleWatcherTestSuite) TestInvalidJSONValue() {
	re := s.Require()
	s.startWatcher()

	// 1. Put invalid rule
	invalidRuleKey := keypath.RuleKeyPath("pd-invalid-rule")
	_, err := s.client.Put(s.ctx, invalidRuleKey, "{invalid-json")
	re.NoError(err)

	// 2. Put invalid group
	invalidGroupKey := keypath.RuleGroupIDPath("invalid-group")
	_, err = s.client.Put(s.ctx, invalidGroupKey, "{invalid-json")
	re.NoError(err)

	// 3. Check that invalid entries are ignored
	ruleCanary := &placement.Rule{
		GroupID: "pd", ID: "canary", StartKey: []byte("c"), EndKey: []byte("d"), Role: placement.Voter, Count: 1,
	}
	canaryJSON, err := json.Marshal(ruleCanary)
	re.NoError(err)
	canaryKey := keypath.RuleKeyPath(ruleCanary.GroupID + "-" + ruleCanary.ID)
	_, err = s.client.Put(s.ctx, canaryKey, string(canaryJSON))
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		return s.manager.GetRule(ruleCanary.GroupID, ruleCanary.ID) != nil
	})
	re.Nil(s.manager.GetRule("pd", "invalid-rule"))
	re.Nil(s.manager.GetRuleGroup("invalid-group"))
}
