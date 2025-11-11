// Copyright 2023 TiKV Project Authors.
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
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const rulesNum = 16384

func TestLoadLargeRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := setupEtcd(re)
	defer clean()
	prepare(ctx, re, client)
	runWatcherLoadLabelRule(ctx, re, client)
}

func BenchmarkLoadLargeRules(b *testing.B) {
	re := require.New(b)
	ctx, client, clean := setupEtcd(re)
	defer clean()
	prepare(ctx, re, client)

	b.ResetTimer() // Resets the timer to ignore initialization time in the benchmark

	for range b.N {
		runWatcherLoadLabelRule(ctx, re, client)
	}
}

func runWatcherLoadLabelRule(ctx context.Context, re *require.Assertions, client *clientv3.Client) {
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)
	ctx, cancel := context.WithCancel(ctx)
	rw := &RegionLabelRuleWatcher{
		ctx:                   ctx,
		cancel:                cancel,
		regionLabelPathPrefix: keypath.RegionLabelPathPrefix(),
		etcdClient:            client,
		regionLabeler:         labelerManager,
	}
	err = rw.initializeWatcher()
	re.NoError(err)
	re.Len(labelerManager.GetAllLabelRules(), rulesNum)
	cancel()
}

func setupEtcd(re *require.Assertions) (context.Context, *clientv3.Client, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestSingleConfig()
	var err error
	cfg.Dir, err = os.MkdirTemp("", "pd_tests")
	re.NoError(err)
	os.RemoveAll(cfg.Dir)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	return ctx, client, func() {
		cancel()
		client.Close()
		etcd.Close()
		os.RemoveAll(cfg.Dir)
	}
}

func prepare(ctx context.Context, re *require.Assertions, client *clientv3.Client) {
	for i := 1; i < rulesNum+1; i++ {
		rule := &labeler.LabelRule{
			ID:       "test_" + strconv.Itoa(i),
			Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
			RuleType: labeler.KeyRange,
			Data:     keyspace.MakeKeyRanges(uint32(i)),
		}
		value, err := json.Marshal(rule)
		re.NoError(err)
		key := keypath.RegionLabelKeyPath(rule.ID)
		_, err = clientv3.NewKV(client).Put(ctx, key, string(value))
		re.NoError(err)
	}
}

type regionLabelRuleWatcherTestSuite struct {
	suite.Suite
	ctx     context.Context
	client  *clientv3.Client
	cleanup func()

	labeler *labeler.RegionLabeler
	rw      *RegionLabelRuleWatcher
}

func TestRegionLabelRuleWatcherSuite(t *testing.T) {
	suite.Run(t, new(regionLabelRuleWatcherTestSuite))
}

func (s *regionLabelRuleWatcherTestSuite) SetupSuite() {
	re := s.Require()
	s.ctx, s.client, s.cleanup = setupEtcd(re)
}

func (s *regionLabelRuleWatcherTestSuite) TearDownSuite() {
	s.cleanup()
}

func (s *regionLabelRuleWatcherTestSuite) SetupTest() {
	re := s.Require()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(s.ctx, storage, time.Hour)
	re.NoError(err)
	s.labeler = labelerManager
	rw, err := NewRegionLabelRuleWatcher(s.ctx, s.client, s.labeler)
	re.NoError(err)
	s.rw = rw
}

func (s *regionLabelRuleWatcherTestSuite) TearDownTest() {
	if s.rw != nil {
		s.rw.Close()
	}
	re := s.Require()
	_, err := s.client.Delete(s.ctx, keypath.RegionLabelPathPrefix(), clientv3.WithPrefix())
	re.NoError(err)
}

func (s *regionLabelRuleWatcherTestSuite) TestEvents() {
	re := s.Require()

	// 1. Test PUT event
	rule1 := &labeler.LabelRule{
		ID:       "test-event-rule",
		Labels:   []labeler.RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: labeler.KeyRange,
		Data:     json.RawMessage(fmt.Sprintf(`[{"start_key": "%x", "end_key": "%x"}]`, "a", "b")),
	}
	rule1JSON, err := json.Marshal(rule1)
	re.NoError(err)
	rule1Key := keypath.RegionLabelKeyPath(rule1.ID)

	_, err = s.client.Put(s.ctx, rule1Key, string(rule1JSON))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.labeler.GetLabelRule(rule1.ID) != nil
	})
	ruleFromManager := s.labeler.GetLabelRule(rule1.ID)
	re.NotNil(ruleFromManager)
	re.Equal(rule1.Labels[0].Key, ruleFromManager.Labels[0].Key)

	// 2. Test DELETE event
	_, err = s.client.Delete(s.ctx, rule1Key)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.labeler.GetLabelRule(rule1.ID) == nil
	})
	ruleFromManager = s.labeler.GetLabelRule(rule1.ID)
	re.Nil(ruleFromManager)
}

func (s *regionLabelRuleWatcherTestSuite) TestInvalidJSONValue() {
	re := s.Require()

	invalidRuleKey := keypath.RegionLabelKeyPath("invalid-rule")
	_, err := s.client.Put(s.ctx, invalidRuleKey, "{invalid-json")
	re.NoError(err)

	ruleCanary := &labeler.LabelRule{
		ID:       "canary",
		Labels:   []labeler.RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: labeler.KeyRange,
		Data:     json.RawMessage(fmt.Sprintf(`[{"start_key": "%x", "end_key": "%x"}]`, "c", "d")),
	}
	canaryJSON, err := json.Marshal(ruleCanary)
	re.NoError(err)
	canaryKey := keypath.RegionLabelKeyPath(ruleCanary.ID)
	_, err = s.client.Put(s.ctx, canaryKey, string(canaryJSON))
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		return s.labeler.GetLabelRule(ruleCanary.ID) != nil
	})

	re.Nil(s.labeler.GetLabelRule("invalid-rule"))
}
