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

package keyspace

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const (
	testConfig  = "test config"
	testConfig1 = "config_entry_1"
	testConfig2 = "config_entry_2"
)

type keyspaceTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	manager *Manager
}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

type mockConfig struct {
	PreAlloc                 []string
	WaitRegionSplit          bool
	WaitRegionSplitTimeout   typeutil.Duration
	CheckRegionSplitInterval typeutil.Duration
}

func (m *mockConfig) GetPreAlloc() []string {
	return m.PreAlloc
}

func (m *mockConfig) ToWaitRegionSplit() bool {
	return m.WaitRegionSplit
}

func (m *mockConfig) GetWaitRegionSplitTimeout() time.Duration {
	return m.WaitRegionSplitTimeout.Duration
}

func (m *mockConfig) GetCheckRegionSplitInterval() time.Duration {
	return m.CheckRegionSplitInterval.Duration
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	allocator := mockid.NewIDAllocator()
	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	suite.manager = NewKeyspaceManager(suite.ctx, store, nil, allocator, &mockConfig{}, kgm)
	re.NoError(kgm.Bootstrap(suite.ctx))
	re.NoError(suite.manager.Bootstrap())
}

func (suite *keyspaceTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *keyspaceTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
}

func (suite *keyspaceTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func makeCreateKeyspaceRequests(count int) []*CreateKeyspaceRequest {
	now := time.Now().Unix()
	requests := make([]*CreateKeyspaceRequest, count)
	for i := range count {
		requests[i] = &CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace_%d", i),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			CreateTime: now,
		}
	}
	return requests
}

func (suite *keyspaceTestSuite) TestCreateKeyspace() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(10)

	for i, request := range requests {
		created, err := manager.CreateKeyspace(request)
		re.NoError(err)
		re.Equal(uint32(i+1), created.Id)
		checkCreateRequest(re, request, created)

		name, err := manager.GetKeyspaceNameByID(created.Id)
		re.NoError(err)
		re.Equal(created.Name, name)

		name, err = manager.GetEnabledKeyspaceNameByID(created.Id)
		re.NoError(err)
		re.Equal(created.Name, name)

		loaded, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(uint32(i+1), loaded.Id)
		checkCreateRequest(re, request, loaded)

		loaded, err = manager.LoadKeyspaceByID(created.Id)
		re.NoError(err)
		re.Equal(loaded.Name, request.Name)
		checkCreateRequest(re, request, loaded)
	}

	// Create a keyspace with existing name must return error.
	_, err := manager.CreateKeyspace(requests[0])
	re.Error(err)

	// Create a keyspace with empty name must return error.
	_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{Name: ""})
	re.Error(err)
}

// getCreateKeyspaceStepCounts gathers metrics and returns the observation count per step
// for the create_keyspace_step_duration_seconds histogram.
func getCreateKeyspaceStepCounts(re *require.Assertions) map[string]uint64 {
	metrics, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)
	const countMetricName = "pd_keyspace_create_keyspace_step_duration_seconds"
	counts := make(map[string]uint64)
	for _, mf := range metrics {
		if mf.GetName() != countMetricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			var step string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "step" {
					step = lp.GetValue()
					break
				}
			}
			if step != "" && m.GetHistogram() != nil {
				counts[step] = uint64(m.GetHistogram().GetSampleCount())
			}
		}
		break
	}
	return counts
}

// expectedCreateKeyspaceSteps returns the ordered list of step names that recordStep should record
// for one successful CreateKeyspace. Must match metrics.go and keyspace.go CreateKeyspace flow.
func expectedCreateKeyspaceSteps() []string {
	return []string{
		stepValidateName,
		stepAllocateID,
		stepGetConfig,
		stepSaveKeyspaceMeta,
		stepSplitRegion,
		stepEnableKeyspace,
		stepUpdateKeyspaceGroup,
	}
}

func (suite *keyspaceTestSuite) TestCreateKeyspaceMetrics() {
	re := suite.Require()
	manager := suite.manager
	expectedSteps := expectedCreateKeyspaceSteps()
	re.Len(expectedSteps, 7, "expected 7 steps in create keyspace")

	before := getCreateKeyspaceStepCounts(re)

	req := &CreateKeyspaceRequest{
		Name:       "record_step_metrics_test_keyspace",
		CreateTime: time.Now().Unix(),
		Config: map[string]string{
			testConfig1: "100",
			testConfig2: "200",
		},
	}
	_, err := manager.CreateKeyspace(req)
	re.NoError(err)

	after := getCreateKeyspaceStepCounts(re)
	for _, step := range expectedSteps {
		re.Contains(after, step, "metric should have step %q", step)
		re.GreaterOrEqual(after[step], before[step]+1,
			"step %q: after (%d) should be >= before (%d) + 1", step, after[step], before[step])
	}
}

func (suite *keyspaceTestSuite) TestGCManagementTypeDefaultValue() {
	re := suite.Require()
	manager := suite.manager

	now := time.Now().Unix()
	const classic = `return(false)`
	const nextGen = `return(true)`

	type testCase struct {
		nextGenFlag      string
		gcManagementType string
		expect           string
	}

	cases := []testCase{
		{classic, "", ""},
		{classic, UnifiedGC, UnifiedGC},
		{classic, KeyspaceLevelGC, KeyspaceLevelGC},
		{nextGen, "", KeyspaceLevelGC},
		{nextGen, UnifiedGC, UnifiedGC},
		{classic, KeyspaceLevelGC, KeyspaceLevelGC},
	}
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"))
	}()
	for idx, tc := range cases {
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", tc.nextGenFlag))
		cfg := make(map[string]string)
		if tc.gcManagementType != "" {
			cfg[GCManagementType] = tc.gcManagementType
		}
		req := &CreateKeyspaceRequest{
			Name:       fmt.Sprintf("gc_mgmt_type_%d", idx),
			CreateTime: now,
			Config:     cfg,
		}
		created, err := manager.CreateKeyspace(req)
		re.NoError(err)
		loaded, err := manager.LoadKeyspaceByID(created.Id)
		re.NoError(err)
		re.Equal(tc.expect, loaded.Config[GCManagementType])
	}
}

func makeCreateKeyspaceByIDRequests(count int) []*CreateKeyspaceByIDRequest {
	now := time.Now().Unix()
	requests := make([]*CreateKeyspaceByIDRequest, count)
	for i := range count {
		id := uint32(i + 1)
		requests[i] = &CreateKeyspaceByIDRequest{
			ID:   &id,
			Name: strconv.FormatUint(uint64(id), 10),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			CreateTime: now,
		}
	}
	return requests
}

func (suite *keyspaceTestSuite) TestCreateKeyspaceByID() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceByIDRequests(10)

	for i, request := range requests {
		created, err := manager.CreateKeyspaceByID(request)
		re.NoError(err)
		id := i + 1
		re.Equal(uint32(id), created.Id)
		re.Equal(strconv.Itoa(id), created.Name)
		checkCreateByIDRequest(re, request, created)

		loaded, err := manager.LoadKeyspaceByID(*request.ID)
		re.NoError(err)
		checkCreateByIDRequest(re, request, loaded)

		loaded, err = manager.LoadKeyspaceByID(created.Id)
		re.NoError(err)
		checkCreateByIDRequest(re, request, loaded)
	}

	// Create a keyspace with existing ID must return error.
	_, err := manager.CreateKeyspaceByID(requests[0])
	re.Error(err)

	// Create a keyspace with existing name must return error.
	*requests[0].ID = 100
	_, err = manager.CreateKeyspaceByID(requests[0])
	re.Error(err)

	// Create a keyspace with empty id must return error.
	_, err = manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{})
	re.Error(err)

	// Create a keyspace with empty name must return error.
	id := uint32(100)
	_, err = manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: ""})
	re.Error(err)
}

// TestCreateKeyspaceNoIDLeak tests that repeated failed creation attempts
// do not waste IDs for both CreateKeyspace and CreateKeyspaceByID.
func (suite *keyspaceTestSuite) TestCreateKeyspaceNoIDLeak() {
	re := suite.Require()
	manager := suite.manager
	now := time.Now().Unix()

	// Test CreateKeyspace: repeated attempts with same name should not waste IDs.
	req := &CreateKeyspaceRequest{
		Name:       "test_no_leak",
		CreateTime: now,
		Config:     map[string]string{testConfig1: "100"},
	}
	first, err := manager.CreateKeyspace(req)
	re.NoError(err)
	re.Equal(uint32(1), first.Id)

	// Attempt to create the same keyspace 5 times - should all fail without allocating IDs.
	for range 5 {
		_, err := manager.CreateKeyspace(req)
		re.ErrorIs(err, errs.ErrKeyspaceExists)
	}

	// Next successful creation should get ID 2, not 7 (proving no ID leak).
	second, err := manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       "test_no_leak_2",
		CreateTime: now,
		Config:     map[string]string{testConfig1: "100"},
	})
	re.NoError(err)
	re.Equal(uint32(2), second.Id)

	// Test CreateKeyspaceByID: should reject duplicate name or ID early.
	id10 := uint32(10)
	_, err = manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{
		ID:         &id10,
		Name:       "test_by_id",
		CreateTime: now,
		Config:     map[string]string{testConfig1: "100"},
	})
	re.NoError(err)

	// Duplicate name with different ID should fail.
	id11 := uint32(11)
	for range 3 {
		_, err := manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{
			ID:         &id11,
			Name:       "test_by_id", // Duplicate name
			CreateTime: now,
		})
		re.ErrorIs(err, errs.ErrKeyspaceExists)
	}

	// Duplicate ID with different name should also fail.
	for range 3 {
		_, err := manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{
			ID:         &id10, // Duplicate ID
			Name:       "test_different",
			CreateTime: now,
		})
		re.ErrorIs(err, errs.ErrKeyspaceExists)
	}

	// Next successful creation should get ID 3, not 14 (proving no ID leak).
	third, err := manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       "test_no_leak_3",
		CreateTime: now,
		Config:     map[string]string{testConfig1: "100"},
	})
	re.NoError(err)
	re.Equal(uint32(3), third.Id)
}

func makeMutations() []*Mutation {
	return []*Mutation{
		{
			Op:    OpPut,
			Key:   testConfig1,
			Value: "new val",
		},
		{
			Op:    OpPut,
			Key:   "new config",
			Value: "new val",
		},
		{
			Op:  OpDel,
			Key: testConfig2,
		},
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(5)
	mutations := makeMutations()
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		updated, err := manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.NoError(err)
		checkMutations(re, createRequest.Config, updated.Config, mutations)
		// Changing config of a ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, time.Now().Unix())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.Error(err)
	}
	// Changing config of bootstrap keyspace is allowed.
	bootstrapKeyspaceName := GetBootstrapKeyspaceName()
	updated, err := manager.UpdateKeyspaceConfig(bootstrapKeyspaceName, mutations)
	re.NoError(err)
	// remove auto filled fields
	delete(updated.Config, TSOKeyspaceGroupIDKey)
	delete(updated.Config, UserKindKey)
	delete(updated.Config, GCManagementType)
	checkMutations(re, nil, updated.Config, mutations)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(5)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		meta, err := manager.LoadKeyspace(createRequest.Name)
		re.NoError(err)
		re.Equal(createRequest.Name, meta.GetName())
		re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.GetState())
		id := meta.GetId()

		oldTime := time.Now().Unix()
		// Archiving an ENABLED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, oldTime)
		re.Error(err)
		state, err := manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_ENABLED, state)
		name, err := manager.GetEnabledKeyspaceNameByID(id)
		re.NoError(err)
		re.Equal(createRequest.Name, name)
		// Disabling an ENABLED keyspace is allowed. Should update StateChangedAt.
		updated, err := manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, oldTime)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, updated.State)
		re.Equal(oldTime, updated.StateChangedAt)
		state, err = manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, state)
		name, err = manager.GetEnabledKeyspaceNameByID(id)
		re.Error(err)
		re.Empty(name)

		newTime := time.Now().Unix()
		// Disabling an DISABLED keyspace is allowed. Should NOT update StateChangedAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, newTime)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, updated.State)
		re.Equal(oldTime, updated.StateChangedAt)
		state, err = manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, state)
		name, err = manager.GetEnabledKeyspaceNameByID(id)
		re.Error(err)
		re.Empty(name)
		// Archiving a DISABLED keyspace is allowed. Should update StateChangeAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, newTime)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, updated.State)
		re.Equal(newTime, updated.StateChangedAt)
		state, err = manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, state)
		name, err = manager.GetEnabledKeyspaceNameByID(id)
		re.Error(err)
		re.Empty(name)
		// Changing state of an ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ENABLED, newTime)
		re.Error(err)
		state, err = manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, state)
		name, err = manager.GetEnabledKeyspaceNameByID(id)
		re.Error(err)
		re.Empty(name)
		// Changing state of bootstrap keyspace is not allowed.
		bootstrapKeyspaceName := GetBootstrapKeyspaceName()
		_, err = manager.UpdateKeyspaceState(bootstrapKeyspaceName, keyspacepb.KeyspaceState_DISABLED, newTime)
		re.Error(err)
		id = GetBootstrapKeyspaceID()
		state, err = manager.GetKeyspaceStateByID(id)
		re.NoError(err)
		re.Equal(keyspacepb.KeyspaceState_ENABLED, state)
		name, err = manager.GetEnabledKeyspaceNameByID(id)
		re.NoError(err)
		re.Equal(bootstrapKeyspaceName, name)
	}
}

func (suite *keyspaceTestSuite) TestLoadRangeKeyspace() {
	re := suite.Require()
	manager := suite.manager
	// Test with 100 keyspaces.
	// Created keyspace ids are 1 - 100.
	total := 100
	requests := makeCreateKeyspaceRequests(total)

	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// Load all keyspaces including the bootstrap keyspace.
	keyspaces, err := manager.LoadRangeKeyspace(0, 0)
	re.NoError(err)
	re.Len(keyspaces, total+1)

	// In next-gen mode, the bootstrap keyspace has SystemKeyspaceID instead of DefaultKeyspaceID (0).
	// So the keyspaces will be ordered as: [1, 2, 3, ..., 100, SystemKeyspaceID]
	// In legacy mode, they will be ordered as: [0, 1, 2, 3, ..., 100]
	if kerneltype.IsNextGen() {
		// For next-gen: expect keyspaces [1, 2, ..., 100, SystemKeyspaceID]
		for i := range keyspaces {
			if i < total {
				// User-created keyspaces with IDs 1-100
				re.Equal(uint32(i+1), keyspaces[i].Id)
				checkCreateRequest(re, requests[i], keyspaces[i])
			} else {
				// Bootstrap keyspace with SystemKeyspaceID
				re.Equal(constant.SystemKeyspaceID, keyspaces[i].Id)
			}
		}
	} else {
		// For classic: expect keyspaces [0, 1, 2, ..., 100]
		for i := range keyspaces {
			re.Equal(uint32(i), keyspaces[i].Id)
			if i != 0 {
				checkCreateRequest(re, requests[i-1], keyspaces[i])
			}
		}
	}

	// Load first 50 keyspaces.
	keyspaces, err = manager.LoadRangeKeyspace(0, 50)
	re.NoError(err)

	if kerneltype.IsNextGen() {
		// In next-gen mode, result should be keyspaces with id 1 - 50.
		re.Len(keyspaces, 50)
		for i := range keyspaces {
			re.Equal(uint32(i+1), keyspaces[i].Id)
			checkCreateRequest(re, requests[i], keyspaces[i])
		}
	} else {
		// In legacy mode, result should be keyspaces with id 0 - 49.
		re.Len(keyspaces, 50)
		for i := range keyspaces {
			re.Equal(uint32(i), keyspaces[i].Id)
			if i != 0 {
				checkCreateRequest(re, requests[i-1], keyspaces[i])
			}
		}
	}

	// Load 20 keyspaces starting from keyspace with id 33.
	// Result should be keyspaces with id 33 - 52.
	loadStart := 33
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 20)
	re.NoError(err)
	re.Len(keyspaces, 20)
	for i := range keyspaces {
		re.Equal(uint32(loadStart+i), keyspaces[i].Id)
		checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
	}

	// Attempts to load 30 keyspaces starting from keyspace with id 90.
	loadStart = 90
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 30)
	re.NoError(err)

	if kerneltype.IsNextGen() {
		// In next-gen mode, scan result should be keyspaces with id 90-100 plus SystemKeyspaceID.
		re.Len(keyspaces, 12)
		for i := range keyspaces {
			if i < 11 {
				// User-created keyspaces with IDs 90-100
				re.Equal(uint32(loadStart+i), keyspaces[i].Id)
				checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
			} else {
				// System keyspace with SystemKeyspaceID
				re.Equal(constant.SystemKeyspaceID, keyspaces[i].Id)
			}
		}
	} else {
		// In legacy mode, scan result should be keyspaces with id 90-100.
		re.Len(keyspaces, 11)
		for i := range keyspaces {
			re.Equal(uint32(loadStart+i), keyspaces[i].Id)
			checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
		}
	}

	// Loading starting from non-existing keyspace ID should result in empty result.
	loadStart = 900
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 0)
	re.NoError(err)
	if kerneltype.IsNextGen() {
		// In next-gen mode, only SystemKeyspaceID is greater than 900.
		re.Len(keyspaces, 1)
		re.Equal(constant.SystemKeyspaceID, keyspaces[0].Id)
	} else {
		re.Empty(keyspaces)
	}

	// Scanning starting from a non-zero illegal index should result in error.
	loadStart = math.MaxUint32
	_, err = manager.LoadRangeKeyspace(uint32(loadStart), 0)
	re.Error(err)
}

// TestUpdateMultipleKeyspace checks that updating multiple keyspace's config simultaneously
// will be successful.
func (suite *keyspaceTestSuite) TestUpdateMultipleKeyspace() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(50)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// Concurrently update all keyspaces' testConfig sequentially.
	end := 100
	wg := sync.WaitGroup{}
	for _, request := range requests {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			updateKeyspaceConfig(re, manager, name, end)
		}(request.Name)
	}
	wg.Wait()

	// Check that eventually all test keyspaces' test config reaches end
	for _, request := range requests {
		keyspace, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(keyspace.Config[testConfig], strconv.Itoa(end))
	}
}

// checkCreateRequest verifies a keyspace meta matches a create request.
func checkCreateRequest(re *require.Assertions, request *CreateKeyspaceRequest, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.GetName())
	re.Equal(request.CreateTime, meta.GetCreatedAt())
	re.Equal(request.CreateTime, meta.GetStateChangedAt())
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.GetState())
	re.Equal(request.Config, meta.GetConfig())
}

// checkCreateByIDRequest verifies a keyspace meta matches a create request.
func checkCreateByIDRequest(re *require.Assertions, request *CreateKeyspaceByIDRequest, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(*request.ID, meta.GetId())
	re.Equal(request.CreateTime, meta.GetCreatedAt())
	re.Equal(request.CreateTime, meta.GetStateChangedAt())
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.GetState())
	re.Equal(request.Config, meta.GetConfig())
}

// checkMutations verifies that performing mutations on old config would result in new config.
func checkMutations(re *require.Assertions, oldConfig, newConfig map[string]string, mutations []*Mutation) {
	// Copy oldConfig to expected to avoid modifying its content.
	expected := map[string]string{}
	for k, v := range oldConfig {
		expected[k] = v
	}
	for _, mutation := range mutations {
		switch mutation.Op {
		case OpPut:
			expected[mutation.Key] = mutation.Value
		case OpDel:
			delete(expected, mutation.Key)
		}
	}
	re.Equal(expected, newConfig)
}

// updateKeyspaceConfig sequentially updates given keyspace's entry.
func updateKeyspaceConfig(re *require.Assertions, manager *Manager, name string, end int) {
	oldMeta, err := manager.LoadKeyspace(name)
	re.NoError(err)
	for i := 0; i <= end; i++ {
		mutations := []*Mutation{
			{
				Op:    OpPut,
				Key:   testConfig,
				Value: strconv.Itoa(i),
			},
		}
		updatedMeta, err := manager.UpdateKeyspaceConfig(name, mutations)
		re.NoError(err)
		checkMutations(re, oldMeta.GetConfig(), updatedMeta.GetConfig(), mutations)
		oldMeta = updatedMeta
	}
}

func (suite *keyspaceTestSuite) TestPatrolKeyspaceAssignment() {
	re := suite.Require()
	// Create a keyspace without any keyspace group.
	now := time.Now().Unix()
	err := suite.manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
		Id:             111,
		Name:           "111",
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      now,
		StateChangedAt: now,
	})
	re.NoError(err)
	// Check if the keyspace is not attached to the default group.
	defaultKeyspaceGroup, err := suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	re.NotContains(defaultKeyspaceGroup.Keyspaces, uint32(111))
	// Patrol the keyspace assignment.
	err = suite.manager.PatrolKeyspaceAssignment(0, 0)
	re.NoError(err)
	// Check if the keyspace is attached to the default group.
	defaultKeyspaceGroup, err = suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	re.Contains(defaultKeyspaceGroup.Keyspaces, uint32(111))
}

func (suite *keyspaceTestSuite) TestPatrolKeyspaceAssignmentInBatch() {
	re := suite.Require()
	// Create some keyspaces without any keyspace group.
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		now := time.Now().Unix()
		err := suite.manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
			Id:             uint32(i),
			Name:           strconv.Itoa(i),
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      now,
			StateChangedAt: now,
		})
		re.NoError(err)
	}
	// Check if all the keyspaces are not attached to the default group.
	defaultKeyspaceGroup, err := suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		re.NotContains(defaultKeyspaceGroup.Keyspaces, uint32(i))
	}
	// Patrol the keyspace assignment.
	err = suite.manager.PatrolKeyspaceAssignment(0, 0)
	re.NoError(err)
	// Check if all the keyspaces are attached to the default group.
	defaultKeyspaceGroup, err = suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		re.Contains(defaultKeyspaceGroup.Keyspaces, uint32(i))
	}
}

func (suite *keyspaceTestSuite) TestPatrolKeyspaceAssignmentWithRange() {
	re := suite.Require()
	// Create some keyspaces without any keyspace group.
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		now := time.Now().Unix()
		err := suite.manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
			Id:             uint32(i),
			Name:           strconv.Itoa(i),
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      now,
			StateChangedAt: now,
		})
		re.NoError(err)
	}
	// Check if all the keyspaces are not attached to the default group.
	defaultKeyspaceGroup, err := suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		re.NotContains(defaultKeyspaceGroup.Keyspaces, uint32(i))
	}
	// Patrol the keyspace assignment with range [ etcdutil.MaxEtcdTxnOps/2,  etcdutil.MaxEtcdTxnOps/2+ etcdutil.MaxEtcdTxnOps+1]
	// to make sure the range crossing the boundary of etcd transaction operation limit.
	var (
		startKeyspaceID = uint32(etcdutil.MaxEtcdTxnOps / 2)
		endKeyspaceID   = startKeyspaceID + etcdutil.MaxEtcdTxnOps + 1
	)
	err = suite.manager.PatrolKeyspaceAssignment(startKeyspaceID, endKeyspaceID)
	re.NoError(err)
	// Check if only the keyspaces within the range are attached to the default group.
	defaultKeyspaceGroup, err = suite.manager.kgm.GetKeyspaceGroupByID(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.NotNil(defaultKeyspaceGroup)
	for i := 1; i < etcdutil.MaxEtcdTxnOps*2+1; i++ {
		keyspaceID := uint32(i)
		if keyspaceID >= startKeyspaceID && keyspaceID <= endKeyspaceID {
			re.Contains(defaultKeyspaceGroup.Keyspaces, keyspaceID)
		} else {
			re.NotContains(defaultKeyspaceGroup.Keyspaces, keyspaceID)
		}
	}
}

func TestIterateKeyspaces(t *testing.T) {
	re := require.New(t)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	testWithKeyspaces := func(keyspaceIDs []uint32, keyspaceNames []string, expectedLoadRangeCount int) {
		re.Len(keyspaceNames, len(keyspaceIDs))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
		allocator := mockid.NewIDAllocator()
		kgm := NewKeyspaceGroupManager(ctx, store, nil)
		manager := NewKeyspaceManager(ctx, store, nil, allocator, &mockConfig{}, kgm)

		re.NoError(kgm.Bootstrap(ctx))
		re.NoError(manager.Bootstrap())

		now := time.Now().Unix()
		for i, id := range keyspaceIDs {
			name := keyspaceNames[i]
			_, err := manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{
				ID:   &id,
				Name: name,
				Config: map[string]string{
					"test_cfg": strconv.FormatUint(uint64(id), 10),
				},
				CreateTime: now,
			})
			re.NoError(err)
		}

		// Add the reserved keyspace to the keyspace list for later check.
		if kerneltype.IsNextGen() {
			keyspaceIDs = append(keyspaceIDs, constant.SystemKeyspaceID)
			keyspaceNames = append(keyspaceNames, constant.SystemKeyspaceName)
		} else {
			keyspaceIDs = append([]uint32{constant.DefaultKeyspaceID}, keyspaceIDs...)
			keyspaceNames = append([]string{constant.DefaultKeyspaceName}, keyspaceNames...)
		}

		loadRangeCounter := 0
		re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/keyspace/keyspaceIteratorOnLoadRange", func() {
			loadRangeCounter++
		}))
		defer func() {
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/keyspaceIteratorOnLoadRange"))
		}()

		it := manager.IterateKeyspaces()
		i := 0
		for ; ; i++ {
			meta, ok, err := it.Next()
			re.NoError(err)
			if !ok {
				break
			}
			re.Equal(keyspaceIDs[i], meta.Id)
			re.Equal(keyspaceNames[i], meta.Name)
			if meta.Id != constant.DefaultKeyspaceID && meta.Id != constant.SystemKeyspaceID {
				re.Equal(strconv.FormatUint(uint64(meta.Id), 10), meta.Config["test_cfg"])
			}
		}
		re.Equal(len(keyspaceIDs), i)
		re.Equal(expectedLoadRangeCount, loadRangeCounter)
	}

	testWithNKeyspaces := func(idStart int, n int, idStep int, expectedLoadRangeCount int) {
		keyspaceIDs := make([]uint32, n)
		keyspaceNames := make([]string, n)
		for i := range n {
			id := uint32(idStart + i*idStep)
			keyspaceIDs[i] = id
			keyspaceNames[i] = fmt.Sprintf("ks%d", id)
		}
		testWithKeyspaces(keyspaceIDs, keyspaceNames, expectedLoadRangeCount)
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/keyspaceIteratorLoadingBatchSize", "return(5)"))

	testWithKeyspaces([]uint32{}, []string{}, 2)
	testWithKeyspaces([]uint32{1, 2}, []string{"ks1", "ks2"}, 2)
	testWithKeyspaces([]uint32{1, 2, 3, 4, 5, 6}, []string{"ks1", "ks2", "ks3", "ks4", "ks5", "ks6"}, 3)
	testWithKeyspaces([]uint32{10, 20, 30, 40, 50, 60}, []string{"ks10", "ks20", "ks30", "ks40", "ks50", "ks60"}, 3)
	testWithNKeyspaces(1, 18, 1, 5)
	testWithNKeyspaces(1, 19, 2, 5)
	testWithNKeyspaces(100, 20, 2, 6)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/keyspaceIteratorLoadingBatchSize"))

	testWithNKeyspaces(1, 999, 1, 11)
	testWithNKeyspaces(1, 1000, 10, 12)
}

// Benchmark the keyspace assignment patrol.
func BenchmarkPatrolKeyspaceAssignment1000(b *testing.B) {
	benchmarkPatrolKeyspaceAssignmentN(1000, b)
}

func BenchmarkPatrolKeyspaceAssignment10000(b *testing.B) {
	benchmarkPatrolKeyspaceAssignmentN(10000, b)
}

func BenchmarkPatrolKeyspaceAssignment100000(b *testing.B) {
	benchmarkPatrolKeyspaceAssignmentN(100000, b)
}

func benchmarkPatrolKeyspaceAssignmentN(
	n int, b *testing.B,
) {
	suite := new(keyspaceTestSuite)
	suite.SetT(&testing.T{})
	suite.SetupSuite()
	suite.SetupTest()
	re := suite.Require()
	// Create some keyspaces without any keyspace group.
	for i := 1; i <= n; i++ {
		now := time.Now().Unix()
		err := suite.manager.saveNewKeyspace(&keyspacepb.KeyspaceMeta{
			Id:             uint32(i),
			Name:           strconv.Itoa(i),
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      now,
			StateChangedAt: now,
		})
		re.NoError(err)
	}
	// Benchmark the keyspace assignment patrol.
	b.ResetTimer()
	for range b.N {
		err := suite.manager.PatrolKeyspaceAssignment(0, 0)
		re.NoError(err)
	}
	b.StopTimer()
	suite.TearDownTest()
	suite.TearDownSuite()
}
