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
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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
	// MetaServiceGroups is used to mock the meta-service groups for keyspace assignment.
	MetaServiceGroups map[string]string
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

func (m *mockConfig) SetMetaServiceGroups(metaServiceGroups map[string]string) {
	m.MetaServiceGroups = metaServiceGroups
}

func (m *mockConfig) GetMetaServiceGroups() map[string]string {
	return m.MetaServiceGroups
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	allocator := mockid.NewIDAllocator()
	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil)
	suite.manager = NewKeyspaceManager(suite.ctx, store, nil, allocator, &mockConfig{}, kgm, nil)
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
		re.Equal(uint32(i+1), created.GetId())
		checkCreateRequest(re, request, created)

		name, err := manager.GetKeyspaceNameByID(created.GetId())
		re.NoError(err)
		re.Equal(created.Name, name)

		name, err = manager.GetEnabledKeyspaceNameByID(created.GetId())
		re.NoError(err)
		re.Equal(created.Name, name)

		loaded, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(uint32(i+1), loaded.GetId())
		checkCreateRequest(re, request, loaded)

		loaded, err = manager.LoadKeyspaceByID(created.GetId())
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
				counts[step] = m.GetHistogram().GetSampleCount()
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
		StepAllocateID,
		StepGetConfig,
		StepSaveKeyspaceMeta,
		StepSplitRegion,
		StepEnableKeyspace,
		StepUpdateKeyspaceGroup,
		StepTotal,
	}
}

func (suite *keyspaceTestSuite) TestCreateKeyspaceMetrics() {
	re := suite.Require()
	manager := suite.manager
	expectedSteps := expectedCreateKeyspaceSteps()
	re.Len(expectedSteps, 7, "expected 7 steps in create keyspace")

	before := getCreateKeyspaceStepCounts(re)

	req := &CreateKeyspaceRequest{
		Name:       "test_ks_metrics",
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
		expectErr        error
	}

	cases := []testCase{
		{classic, "", UnifiedGC, nil},
		{classic, UnifiedGC, UnifiedGC, nil},
		{classic, KeyspaceLevelGC, "", errs.ErrKeyspaceGCModeMismatch},
		{nextGen, "", KeyspaceLevelGC, nil},
		{nextGen, UnifiedGC, "", errs.ErrKeyspaceGCModeMismatch},
		{nextGen, KeyspaceLevelGC, KeyspaceLevelGC, nil},
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
		if tc.expectErr != nil {
			re.ErrorIs(err, tc.expectErr)
			continue
		}
		re.NoError(err)
		loaded, err := manager.LoadKeyspaceByID(created.GetId())
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
		re.Equal(uint32(id), created.GetId())
		re.Equal(strconv.Itoa(id), created.Name)
		checkCreateByIDRequest(re, request, created)

		loaded, err := manager.LoadKeyspaceByID(*request.ID)
		re.NoError(err)
		checkCreateByIDRequest(re, request, loaded)

		loaded, err = manager.LoadKeyspaceByID(created.GetId())
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
	re.Equal(uint32(1), first.GetId())

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
	re.Equal(uint32(2), second.GetId())

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
	re.Equal(uint32(3), third.GetId())
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
	delete(updated.Config, RegionBoundType)
	checkMutations(re, nil, updated.Config, mutations)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfigWithPreconditions() {
	re := suite.Require()
	manager := suite.manager

	ksName := "precond_test"
	_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       ksName,
		Config:     map[string]string{},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)

	currentKey := "current_file_id"
	nextKey := "next_file_id"

	// Expecting a missing key to exist should fail.
	expectedMissing := "1"
	_, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpPut, Key: currentKey, Value: expectedMissing},
	}, map[string]*string{
		currentKey: &expectedMissing,
	})
	re.Error(err)
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)

	// 1) only set next_file_id if it doesn't exist already.
	nextV1 := "1000"
	meta, err := manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpPut, Key: nextKey, Value: nextV1},
	}, map[string]*string{
		nextKey: nil,
	})
	re.NoError(err)
	re.Equal(nextV1, meta.Config[nextKey])

	nextV2 := "2000"
	_, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpPut, Key: nextKey, Value: nextV2},
	}, map[string]*string{
		nextKey: nil,
	})
	re.Error(err)
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)

	// 2) Update current_file_id to the next_file_id (guarded by next_file_id == expected).
	meta, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpPut, Key: currentKey, Value: nextV1},
	}, map[string]*string{
		nextKey: &nextV1,
	})
	re.NoError(err)
	re.Equal(nextV1, meta.Config[currentKey])

	// 3) Delete next_file_id if it matches my expected value.
	wrongExpected := "999"
	_, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpDel, Key: nextKey},
	}, map[string]*string{
		nextKey: &wrongExpected,
	})
	re.Error(err)
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)

	meta, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpDel, Key: nextKey},
	}, map[string]*string{
		nextKey: &nextV1,
	})
	re.NoError(err)
	re.NotContains(meta.GetConfig(), nextKey)

	// Empty preconditions should behave like a normal update.
	meta, err = manager.UpdateKeyspaceConfigWithPreconditions(ksName, []*Mutation{
		{Op: OpPut, Key: currentKey, Value: "3000"},
	}, map[string]*string{})
	re.NoError(err)
	re.Equal("3000", meta.Config[currentKey])
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
				re.Equal(uint32(i+1), keyspaces[i].GetId())
				checkCreateRequest(re, requests[i], keyspaces[i])
			} else {
				// Bootstrap keyspace with SystemKeyspaceID
				re.Equal(constant.SystemKeyspaceID, keyspaces[i].GetId())
			}
		}
	} else {
		// For classic: expect keyspaces [0, 1, 2, ..., 100]
		for i := range keyspaces {
			re.Equal(uint32(i), keyspaces[i].GetId())
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
			re.Equal(uint32(i+1), keyspaces[i].GetId())
			checkCreateRequest(re, requests[i], keyspaces[i])
		}
	} else {
		// In legacy mode, result should be keyspaces with id 0 - 49.
		re.Len(keyspaces, 50)
		for i := range keyspaces {
			re.Equal(uint32(i), keyspaces[i].GetId())
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
		re.Equal(uint32(loadStart+i), keyspaces[i].GetId())
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
				re.Equal(uint32(loadStart+i), keyspaces[i].GetId())
				checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
			} else {
				// System keyspace with SystemKeyspaceID
				re.Equal(constant.SystemKeyspaceID, keyspaces[i].GetId())
			}
		}
	} else {
		// In legacy mode, scan result should be keyspaces with id 90-100.
		re.Len(keyspaces, 11)
		for i := range keyspaces {
			re.Equal(uint32(loadStart+i), keyspaces[i].GetId())
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
		re.Equal(constant.SystemKeyspaceID, keyspaces[0].GetId())
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
	maps.Copy(expected, oldConfig)
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
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: 111},
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
			Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: uint32(i)},
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
			Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: uint32(i)},
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
		manager := NewKeyspaceManager(ctx, store, nil, allocator, &mockConfig{}, kgm, nil)

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
			re.Equal(keyspaceIDs[i], meta.GetId())
			re.Equal(keyspaceNames[i], meta.Name)
			if meta.GetId() != constant.DefaultKeyspaceID && meta.GetId() != constant.SystemKeyspaceID {
				re.Equal(strconv.FormatUint(uint64(meta.GetId()), 10), meta.Config["test_cfg"])
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
			Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: uint32(i)},
			Name:           strconv.Itoa(i),
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      now,
			StateChangedAt: now,
		})
		re.NoError(err)
	}
	// Benchmark the keyspace assignment patrol.

	for b.Loop() {
		err := suite.manager.PatrolKeyspaceAssignment(0, 0)
		re.NoError(err)
	}
	b.StopTimer()
	suite.TearDownTest()
	suite.TearDownSuite()
}

func (suite *keyspaceTestSuite) TestChecker() {
	re := suite.Require()
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: 10000},
		Name:           "1",
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      time.Now().Unix(),
		StateChangedAt: time.Now().Unix(),
	}
	re.NoError(suite.manager.saveNewKeyspace(meta))

	meta = &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: 10001},
		Name:           "2",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		CreatedAt:      time.Now().Unix(),
		StateChangedAt: time.Now().Unix(),
	}
	re.NoError(suite.manager.saveNewKeyspace(meta))

	// keyspace exist check.
	re.True(suite.manager.KeyspaceExist(10000))
	re.False(suite.manager.KeyspaceExist(10001))
	re.False(suite.manager.KeyspaceExist(10002))

	// keyspace id in range check.
	arr, exist := suite.manager.GetKeyspaceIDInRange(10000, 10010, 1)
	re.True(exist)
	re.Equal([]uint32{10000}, arr)
	arr, exist = suite.manager.GetKeyspaceIDInRange(10005, 10010, 1)
	re.False(exist)
	re.Empty(arr)
}

func (suite *keyspaceTestSuite) TestGCBarrierRemovalInvalidationAfterCommit() {
	re := suite.Require()
	m := suite.manager
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 20000}, Name: "barrier-remove", State: keyspacepb.KeyspaceState_TOMBSTONE}
	re.NoError(m.saveNewKeyspace(meta))
	re.NoError(m.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 101, UserKind: endpoint.Standard.String(), Keyspaces: []uint32{20000}}}))
	calls := 0
	m.SetGCBarrierInvalidator(func(id uint32) {
		calls++
		m.metaLock.Lock(id)
		defer m.metaLock.Unlock(id)
		_, err := m.LoadKeyspaceByID(id)
		re.Error(err)
		// A group operation takes the group lock, detecting lock-order regression.
		_, err = m.kgm.GetKeyspaceGroups(101, 1)
		re.NoError(err)
	})
	base := m.kgm.store
	m.kgm.store = &errorKeyspaceGroupStorage{StorageEndpoint: m.store.(*endpoint.StorageEndpoint), failOnSaveID: 101}
	_, err := m.kgm.removeKeyspacesFromGroupWithConditions(suite.ctx, 101, m, []uint32{20000}, nil)
	re.Error(err)
	re.Zero(calls)
	m.kgm.store = base
	_, err = m.kgm.removeKeyspacesFromGroupWithConditions(suite.ctx, 101, m, []uint32{20000}, nil)
	re.NoError(err)
	re.Equal(1, calls)
}

// TestRemoveKeyspaceCleansCache verifies that RemoveKeyspace deletes the
// keyspace's entry from the in-memory cache, not just from the legacy
// keyspaceNameLookup/keyspaceStateLookup maps, so a fully removed keyspace
// does not leak a stale cache entry forever.
func (suite *keyspaceTestSuite) TestRemoveKeyspaceCleansCache() {
	re := suite.Require()
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace:       &keyspacepb.KeyspaceMeta_Id{Id: 20000},
		Name:           "to-be-removed",
		State:          keyspacepb.KeyspaceState_TOMBSTONE,
		CreatedAt:      time.Now().Unix(),
		StateChangedAt: time.Now().Unix(),
	}
	re.NoError(suite.manager.saveNewKeyspace(meta))
	_, found := suite.manager.cache.getKeyspaceByID(meta.GetId())
	re.True(found)

	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.RemoveKeyspace(txn, meta.GetId())
	}))
	_, found = suite.manager.cache.getKeyspaceByID(meta.GetId())
	re.False(found)
}

// TestAssignGroupAndSaveKeyspace verifies that keyspace creation tolerates a
// stale pre-lock group check: if all meta-service groups are gone by the time
// the lock is held, the keyspace is created without an assignment instead of
// failing, while a present group is still assigned normally.
func TestAssignGroupAndSaveKeyspace(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	kgm := NewKeyspaceGroupManager(ctx, store, nil)

	// No groups available: assign=true (stale pre-check) must not fail creation.
	emptyMgm := NewMetaServiceGroupManager(store, map[string]string{}, nil)
	managerNoGroup := NewKeyspaceManager(ctx, store, nil, mockid.NewIDAllocator(), &mockConfig{}, kgm, emptyMgm)
	cfg := map[string]string{}
	ks := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 100}, Name: "ks-stale-precheck", Config: cfg}
	re.NoError(managerNoGroup.assignGroupAndSaveKeyspace(true, &cfg, ks))
	re.NotContains(ks.GetConfig(), MetaServiceGroupIDKey)
	loaded, err := managerNoGroup.LoadKeyspace("ks-stale-precheck")
	re.NoError(err)
	re.Empty(loaded.GetConfig()[MetaServiceGroupIDKey])

	// A present, enabled group is still assigned. Groups are disabled by
	// default, so it must be enabled before it is eligible for assignment.
	mgm := NewMetaServiceGroupManager(store, map[string]string{"g1": "addr1"}, nil)
	enabled := true
	re.NoError(mgm.PatchStatus(ctx, "g1", &MetaServiceGroupStatusPatch{Enabled: &enabled}))
	managerWithGroup := NewKeyspaceManager(ctx, store, nil, mockid.NewIDAllocator(), &mockConfig{}, kgm, mgm)
	cfg2 := map[string]string{}
	ks2 := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 101}, Name: "ks-with-group", Config: cfg2}
	re.NoError(managerWithGroup.assignGroupAndSaveKeyspace(true, &cfg2, ks2))
	re.Equal("g1", ks2.GetConfig()[MetaServiceGroupIDKey])

	// A group that exists but is disabled must not fail creation: the keyspace is
	// created without a meta-service group assignment instead.
	disabledMgm := NewMetaServiceGroupManager(store, map[string]string{"g2": "addr2"}, nil)
	managerDisabled := NewKeyspaceManager(ctx, store, nil, mockid.NewIDAllocator(), &mockConfig{}, kgm, disabledMgm)
	cfg3 := map[string]string{}
	ks3 := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 102}, Name: "ks-disabled-group", Config: cfg3}
	re.NoError(managerDisabled.assignGroupAndSaveKeyspace(true, &cfg3, ks3))
	re.NotContains(ks3.GetConfig(), MetaServiceGroupIDKey)
}

func (suite *keyspaceTestSuite) TestTombstoneKeyspaceUnassignsMetaServiceGroup() {
	re := suite.Require()
	manager := suite.manager
	groupID := "etcd-group-0"
	groupEndpoint := "etcd-group-0.tidb-serverless.cluster.svc.local"
	metaServiceGroupStore, ok := manager.store.(endpoint.MetaServiceGroupStorage)
	re.True(ok)
	// Start without any group so creation never auto-assigns: meta-service groups
	// are disabled by default, and this keeps the test independent of that.
	manager.mgm = NewMetaServiceGroupManager(metaServiceGroupStore, map[string]string{}, nil)
	manager.mgm.SetKeyspaceAssignmentCounter(manager.CountKeyspacesByMetaServiceGroup)

	created, err := manager.CreateKeyspace(&CreateKeyspaceRequest{
		Name:       "test_ks_msg",
		Config:     map[string]string{},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	re.NotContains(created.GetConfig(), MetaServiceGroupIDKey)

	// Make the group available and assign the keyspace to it explicitly, mirroring
	// what a meta-service-group-enabled cluster persists.
	assignKeyspaceToGroup := func(id uint32) {
		re.NoError(manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
			meta, err := manager.store.LoadKeyspaceMeta(txn, id)
			if err != nil {
				return err
			}
			if meta.Config == nil {
				meta.Config = map[string]string{}
			}
			meta.Config[MetaServiceGroupIDKey] = groupID
			if err := manager.store.SaveKeyspaceMeta(txn, meta); err != nil {
				return err
			}
			return manager.mgm.updateAssignmentTxn(txn, "", groupID)
		}))
	}
	manager.mgm.updateGroups(map[string]string{groupID: groupEndpoint})
	assignKeyspaceToGroup(created.GetId())

	counts, err := manager.mgm.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(1, counts[groupID])

	_, err = manager.UpdateKeyspaceState(created.GetName(), keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
	re.NoError(err)
	_, err = manager.UpdateKeyspaceState(created.GetName(), keyspacepb.KeyspaceState_ARCHIVED, time.Now().Unix())
	re.NoError(err)
	updated, err := manager.UpdateKeyspaceState(created.GetName(), keyspacepb.KeyspaceState_TOMBSTONE, time.Now().Unix())
	re.NoError(err)
	re.NotContains(updated.GetConfig(), MetaServiceGroupIDKey)

	loaded, err := manager.LoadKeyspace(created.GetName())
	re.NoError(err)
	re.NotContains(loaded.GetConfig(), MetaServiceGroupIDKey)
	counts, err = manager.mgm.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(0, counts[groupID])

	// A keyspace tombstoned before this cleanup existed still carries the group
	// binding and an inflated counter. Re-applying the TOMBSTONE state (a
	// same-state update) must repair it rather than being skipped.
	assignKeyspaceToGroup(updated.GetId())
	counts, err = manager.mgm.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(1, counts[groupID])
	repaired, err := manager.UpdateKeyspaceState(created.GetName(), keyspacepb.KeyspaceState_TOMBSTONE, time.Now().Unix())
	re.NoError(err)
	re.NotContains(repaired.GetConfig(), MetaServiceGroupIDKey)
	counts, err = manager.mgm.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(0, counts[groupID])

	// Removing the already-tombstoned keyspace must not decrement the counter
	// again: the group binding was cleared and persisted during the tombstone
	// transition, so unassignment is a no-op and the count stays at zero.
	re.NoError(manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return manager.RemoveKeyspace(txn, updated.GetId())
	}))
	counts, err = manager.mgm.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(0, counts[groupID])
}

func TestLoadKeyspaceByIDWithRevision(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	groups := NewMetaServiceGroupManager(store, map[string]string{"group1": "http://127.0.0.1:2379"}, nil)
	defer func() { re.NoError(groups.Close()) }()
	manager := &Manager{ctx: ctx, store: store, mgm: groups}
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Name: "revision-keyspace", Config: map[string]string{MetaServiceGroupIDKey: "group1"}}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error { return store.SaveKeyspaceMeta(txn, meta) }))
	stored, err := client.Get(ctx, keypath.KeyspaceMetaPath(1))
	re.NoError(err)
	loaded, revision, err := manager.LoadKeyspaceByIDWithRevision(ctx, 1)
	re.NoError(err)
	re.Equal(meta.GetId(), loaded.GetId())
	re.Equal(meta.Name, loaded.Name)
	re.Equal("group1", loaded.Config[MetaServiceGroupIDKey])
	re.Equal("http://127.0.0.1:2379", loaded.Config[MetaServiceGroupAddressesKey])
	re.Equal(stored.Kvs[0].ModRevision, revision)
	loaded, _, err = manager.LoadKeyspaceByIDWithRevision(ctx, 2)
	re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	re.Nil(loaded)
}

type revisionedKeyspaceStorage struct {
	endpoint.KeyspaceStorage
	load func(context.Context, uint32) (*keyspacepb.KeyspaceMeta, int64, error)
}

func (s *revisionedKeyspaceStorage) LoadKeyspaceMetaWithRevision(ctx context.Context, id uint32) (*keyspacepb.KeyspaceMeta, int64, error) {
	return s.load(ctx, id)
}

func TestLoadKeyspaceByIDWithRevisionCancellation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cancelManager bool
		cancelBefore  bool
	}{
		{name: "caller-before", cancelBefore: true},
		{name: "manager-before", cancelManager: true, cancelBefore: true},
		{name: "caller-in-flight"},
		{name: "manager-in-flight", cancelManager: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			callerCtx, cancelCaller := context.WithCancel(context.Background())
			managerCtx, cancelManager := context.WithCancel(context.Background())
			cancel := cancelCaller
			if tc.cancelManager {
				cancel = cancelManager
			}
			if tc.cancelBefore {
				cancel()
			}
			started := make(chan error, 1)
			abort := make(chan struct{})
			manager := &Manager{
				ctx: managerCtx,
				store: &revisionedKeyspaceStorage{load: func(ctx context.Context, _ uint32) (*keyspacepb.KeyspaceMeta, int64, error) {
					started <- ctx.Err()
					select {
					case <-ctx.Done():
						return nil, 0, ctx.Err()
					case <-abort:
						return nil, 0, context.DeadlineExceeded
					}
				}},
			}
			var (
				loaded   *keyspacepb.KeyspaceMeta
				revision int64
				err      error
			)
			done := make(chan struct{})
			go func() {
				loaded, revision, err = manager.LoadKeyspaceByIDWithRevision(callerCtx, 1)
				close(done)
			}()
			t.Cleanup(func() {
				cancelCaller()
				cancelManager()
				close(abort)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("metadata lookup did not stop during cleanup")
				}
			})
			select {
			case errAtEntry := <-started:
				if tc.cancelBefore {
					re.ErrorIs(errAtEntry, context.Canceled)
				} else {
					re.NoError(errAtEntry)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("metadata lookup did not reach storage")
			}
			cancel()
			select {
			case <-done:
				re.ErrorIs(err, context.Canceled)
				re.Nil(loaded)
				re.Zero(revision)
			case <-time.After(5 * time.Second):
				t.Fatal("metadata lookup did not stop after cancellation")
			}
		})
	}
}

func TestLoadKeyspaceByIDWithRevisionContextCleanup(t *testing.T) {
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Name: "revision-keyspace"}
	for _, tc := range []struct {
		name       string
		meta       *keyspacepb.KeyspaceMeta
		storageErr error
		wantErr    error
	}{
		{name: "success", meta: meta},
		{name: "storage-error", storageErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded},
		{name: "not-found", wantErr: errs.ErrKeyspaceNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			type valueKey struct{}
			deadline := time.Now().Add(time.Minute)
			callerCtx, cancelCaller := context.WithDeadline(context.WithValue(context.Background(), valueKey{}, "caller-value"), deadline)
			defer cancelCaller()
			managerCtx, cancelManager := context.WithCancel(context.Background())
			defer cancelManager()
			var loadCtx context.Context
			manager := &Manager{
				ctx: managerCtx,
				store: &revisionedKeyspaceStorage{load: func(ctx context.Context, _ uint32) (*keyspacepb.KeyspaceMeta, int64, error) {
					loadCtx = ctx
					return tc.meta, 42, tc.storageErr
				}},
			}
			loaded, revision, err := manager.LoadKeyspaceByIDWithRevision(callerCtx, 1)
			if tc.wantErr != nil {
				re.ErrorIs(err, tc.wantErr)
				re.Nil(loaded)
				re.Zero(revision)
			} else {
				re.NoError(err)
				re.Equal(meta, loaded)
				re.Equal(int64(42), revision)
			}
			re.NotNil(loadCtx)
			re.Equal("caller-value", loadCtx.Value(valueKey{}))
			loadDeadline, ok := loadCtx.Deadline()
			re.True(ok)
			re.Equal(deadline, loadDeadline)
			re.ErrorIs(loadCtx.Err(), context.Canceled)
			re.NoError(callerCtx.Err())
			re.NoError(managerCtx.Err())
		})
	}
}

func TestEffectiveGCManagementType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config map[string]string
		want   string
	}{
		{"nil", nil, UnifiedGC},
		{"empty", map[string]string{}, UnifiedGC},
		{"legacy-v1", map[string]string{"safe_point_version": "v1"}, UnifiedGC},
		{"legacy-v2", map[string]string{"safe_point_version": "v2"}, KeyspaceLevelGC},
		{"legacy-unknown", map[string]string{"safe_point_version": "unknown"}, UnifiedGC},
		{"new-unified", map[string]string{GCManagementType: UnifiedGC}, UnifiedGC},
		{"new-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, KeyspaceLevelGC},
		{"unified-overrides-legacy", map[string]string{GCManagementType: UnifiedGC, "safe_point_version": "v2"}, UnifiedGC},
		{"keyspace-overrides-legacy", map[string]string{GCManagementType: KeyspaceLevelGC, "safe_point_version": "v1"}, KeyspaceLevelGC},
		{"empty-suppresses-legacy", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, UnifiedGC},
		{"unknown-suppresses-legacy", map[string]string{GCManagementType: "unknown", "safe_point_version": "v2"}, UnifiedGC},
		{"empty-new", map[string]string{GCManagementType: ""}, UnifiedGC},
		{"unknown-new", map[string]string{GCManagementType: "unknown"}, UnifiedGC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := maps.Clone(tc.config)
			require.Equal(t, tc.want, EffectiveGCManagementType(tc.config))
			require.Equal(t, original, tc.config)
		})
	}
}

func TestGCConfigCreationRejectsBeforeSideEffects(t *testing.T) {
	m := &Manager{} // Every external dependency is absent: rejection must precede IO.
	opposite := KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		opposite = UnifiedGC
	}
	for _, tc := range []struct {
		config map[string]string
		want   error
	}{
		{map[string]string{GCManagementType: ""}, errs.ErrKeyspaceGCConfigInvalid},
		{map[string]string{GCManagementType: "unknown"}, errs.ErrKeyspaceGCConfigInvalid},
		{map[string]string{GCManagementType: opposite}, errs.ErrKeyspaceGCModeMismatch},
		{map[string]string{"safe_point_version": ""}, errs.ErrKeyspaceLegacyGCConfigUnsupported},
	} {
		_, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "gc_rejected", Config: maps.Clone(tc.config)})
		require.ErrorIs(t, err, tc.want)
		id := uint32(123)
		_, err = m.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: "gc_rejected", Config: maps.Clone(tc.config)})
		require.ErrorIs(t, err, tc.want)
	}
}

func TestObserveGCConfigBoundAndLegacy(t *testing.T) {
	re := require.New(t)
	core, logs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	m := &Manager{} // Observation works without storage or any other dependency.
	m.ObserveGCConfig(nil)
	m.ObserveGCConfig(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: constant.NullKeyspaceID}, Config: map[string]string{GCManagementType: "invalid"}})
	legacy := "v1"
	if kerneltype.IsNextGen() {
		legacy = "v2"
	}
	m.ObserveGCConfig(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Config: map[string]string{"safe_point_version": legacy}})
	re.Empty(logs.All(), "matching legacy metadata and NullKeyspace are not violations")
	for id := uint32(1); id <= 4097; id++ {
		meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Config: map[string]string{GCManagementType: "invalid"}}
		m.ObserveGCConfig(meta)
		re.Equal(map[string]string{GCManagementType: "invalid"}, meta.Config)
	}
	re.Len(logs.All(), 4097)
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 4097}, Config: map[string]string{GCManagementType: "invalid"}}
	m.ObserveGCConfig(meta)
	re.Len(logs.All(), 4097, "the most recent violation remains suppressed")
	meta.Keyspace = &keyspacepb.KeyspaceMeta_Id{Id: 1}
	m.ObserveGCConfig(meta)
	re.Len(logs.All(), 4098, "an evicted violation may be reported again")
}

// saveHistoricalGCConfig constructs persisted history without using a creation API.
func (suite *keyspaceTestSuite) saveHistoricalGCConfig(id uint32, config map[string]string) *keyspacepb.KeyspaceMeta {
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
		Name:     fmt.Sprintf("gc-history-%d", id),
		State:    keyspacepb.KeyspaceState_ENABLED,
		Config:   maps.Clone(config),
	}
	suite.Require().NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		if err := suite.manager.store.SaveKeyspaceID(txn, id, meta.Name); err != nil {
			return err
		}
		return suite.manager.store.SaveKeyspaceMeta(txn, meta)
	}))
	return meta
}

func (suite *keyspaceTestSuite) TestGCConfigCreationPolicy() {
	const kernelFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	suite.T().Cleanup(func() { suite.Require().NoError(failpoint.Disable(kernelFlag)) })
	allocated := uint32(0)
	for _, nextGen := range []bool{false, true} {
		suite.Require().NoError(failpoint.Enable(kernelFlag, fmt.Sprintf("return(%t)", nextGen)))
		mode, incompatible := UnifiedGC, KeyspaceLevelGC
		if nextGen {
			mode, incompatible = KeyspaceLevelGC, UnifiedGC
		}
		for _, byID := range []bool{false, true} {
			cases := []struct {
				name   string
				config map[string]string
				valid  bool
			}{
				{"nil", nil, true},
				{"absent", map[string]string{"unrelated": "retained"}, true},
				{"matching", map[string]string{GCManagementType: mode}, true},
				{"mismatched", map[string]string{GCManagementType: incompatible}, false},
				{"empty", map[string]string{GCManagementType: ""}, false},
				{"unknown", map[string]string{GCManagementType: "other"}, false},
				{"legacy-v2", map[string]string{"safe_point_version": "v2"}, false},
				{"legacy-empty", map[string]string{"safe_point_version": ""}, false},
				{"legacy-v1", map[string]string{GCManagementType: mode, "safe_point_version": "v1"}, false},
			}
			for i, tc := range cases {
				suite.Run(fmt.Sprintf("nextgen=%t/by-id=%t/%s", nextGen, byID, tc.name), func() {
					re := suite.Require()
					name := fmt.Sprintf("gc_%t_%t_%d", nextGen, byID, i)
					var meta *keyspacepb.KeyspaceMeta
					var err error
					if byID {
						id := uint32(1000 + i)
						if nextGen {
							id += 100
						}
						meta, err = suite.manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: name, Config: maps.Clone(tc.config)})
					} else {
						meta, err = suite.manager.CreateKeyspace(&CreateKeyspaceRequest{Name: name, Config: maps.Clone(tc.config)})
					}
					if !tc.valid {
						re.Error(err)
						_, err = suite.manager.LoadKeyspace(name)
						re.ErrorIs(err, errs.ErrKeyspaceNotFound)
						return
					}
					re.NoError(err)
					re.Equal(mode, meta.Config[GCManagementType])
					if !byID {
						allocated++
						re.Equal(allocated, meta.GetId(), "rejected requests must not allocate IDs")
					}
				})
			}
		}
	}
}

func (suite *keyspaceTestSuite) TestGCConfigUpdates() {
	put := func(value string) []*Mutation { return []*Mutation{{Op: OpPut, Key: GCManagementType, Value: value}} }
	del := []*Mutation{{Op: OpDel, Key: GCManagementType}}
	cases := []struct {
		name      string
		stored    map[string]string
		mutations []*Mutation
		want      string
		wantErr   error
	}{
		{"nil", nil, nil, UnifiedGC, nil},
		{"missing", map[string]string{}, nil, UnifiedGC, nil},
		{"legacy-v2", map[string]string{"safe_point_version": "v2"}, nil, KeyspaceLevelGC, nil},
		{"legacy-v1", map[string]string{"safe_point_version": "v1"}, nil, UnifiedGC, nil},
		{"legacy-explicit", map[string]string{"safe_point_version": "v2"}, put(KeyspaceLevelGC), KeyspaceLevelGC, nil},
		{"legacy-conversion", map[string]string{"safe_point_version": "v2"}, put(UnifiedGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"repeat-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, put(KeyspaceLevelGC), KeyspaceLevelGC, nil},
		{"repeat-unified", map[string]string{GCManagementType: UnifiedGC}, put(UnifiedGC), UnifiedGC, nil},
		{"unified-to-keyspace", map[string]string{GCManagementType: UnifiedGC}, put(KeyspaceLevelGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"keyspace-to-unified", map[string]string{GCManagementType: KeyspaceLevelGC}, put(UnifiedGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-both-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC, "safe_point_version": "v2"}, del, KeyspaceLevelGC, nil},
		{"delete-activates-legacy", map[string]string{GCManagementType: UnifiedGC, "safe_point_version": "v2"}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-unified", map[string]string{GCManagementType: UnifiedGC}, del, UnifiedGC, nil},
		{"delete-missing-legacy", map[string]string{"safe_point_version": "v2"}, del, KeyspaceLevelGC, nil},
		{"empty-history", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, nil, "", nil},
		{"unknown-history", map[string]string{GCManagementType: "unknown", "safe_point_version": "v2"}, nil, "unknown", nil},
		{"repair-empty", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, put(UnifiedGC), UnifiedGC, nil},
		{"repair-unknown", map[string]string{GCManagementType: "unknown"}, put(UnifiedGC), UnifiedGC, nil},
		{"delete-empty-activates-legacy", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-unknown", map[string]string{GCManagementType: "unknown"}, del, UnifiedGC, nil},
		{"put-empty", nil, put(""), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-unknown", nil, put("unknown"), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-invalid-then-valid", nil, append(put("unknown"), put(UnifiedGC)...), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-legacy-same", map[string]string{"safe_point_version": "v2"}, []*Mutation{{Op: OpPut, Key: "safe_point_version", Value: "v2"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"put-legacy-empty", nil, []*Mutation{{Op: OpPut, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"delete-legacy", map[string]string{"safe_point_version": "v2"}, []*Mutation{{Op: OpDel, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"delete-legacy-absent", nil, []*Mutation{{Op: OpDel, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
	}
	for i, tc := range cases {
		suite.Run(tc.name, func() {
			re := suite.Require()
			original := suite.saveHistoricalGCConfig(uint32(2000+i), tc.stored)
			mutations := append([]*Mutation{{Op: OpPut, Key: "unrelated", Value: "updated"}}, tc.mutations...)
			updated, err := suite.manager.UpdateKeyspaceConfig(original.Name, mutations)
			if tc.wantErr != nil {
				re.ErrorIs(err, tc.wantErr)
				stored, err := suite.manager.LoadKeyspace(original.Name)
				re.NoError(err)
				re.Equal(original.Config, stored.Config, "rejection must not persist any mutation or normalization")
				return
			}
			re.NoError(err)
			want := maps.Clone(tc.stored)
			if want == nil {
				want = map[string]string{}
			}
			want[GCManagementType] = tc.want
			want["unrelated"] = "updated"
			re.Equal(want, updated.Config)
			stored, err := suite.manager.LoadKeyspace(original.Name)
			re.NoError(err)
			re.Equal(want, stored.Config)
		})
	}
}

func (suite *keyspaceTestSuite) TestGCConfigOriginalPreconditionsAndAtomicity() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3000, map[string]string{"safe_point_version": "v2"})
	unified := UnifiedGC
	_, err := suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: &unified})
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)
	stored, err := suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, stored.Config)
	_, err = suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "partial", Value: "no"}, {Op: OpType(99)}})
	re.ErrorIs(err, errs.ErrIllegalOperation)
	stored, err = suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, stored.Config)
	updated, err := suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: nil})
	re.NoError(err)
	re.Equal(KeyspaceLevelGC, updated.Config[GCManagementType])
	_, err = suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: nil})
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)
}

func (suite *keyspaceTestSuite) TestGCConfigEmptyUpdateNormalizes() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3001, nil)
	updated, err := suite.manager.UpdateKeyspaceConfig(meta.Name, nil)
	re.NoError(err)
	re.Equal(map[string]string{GCManagementType: UnifiedGC}, updated.Config)
}

func (suite *keyspaceTestSuite) TestGCConfigReadsAndStateChangesPreserveHistory() {
	re := suite.Require()
	for i, config := range []map[string]string{nil, {"safe_point_version": "v2"}, {GCManagementType: "unknown", "safe_point_version": "v2"}} {
		meta := suite.saveHistoricalGCConfig(uint32(3100+i), config)
		byName, err := suite.manager.LoadKeyspace(meta.Name)
		re.NoError(err)
		re.Equal(meta.Config, byName.Config)
		byID, err := suite.manager.LoadKeyspaceByID(meta.GetId())
		re.NoError(err)
		re.Equal(meta.Config, byID.Config)
		_, err = suite.manager.UpdateKeyspaceState(meta.Name, keyspacepb.KeyspaceState_DISABLED, 1)
		re.NoError(err)
		updated, err := suite.manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 2)
		re.NoError(err)
		re.Equal(meta.Config, updated.Config)
	}
}

func (suite *keyspaceTestSuite) TestGCConfigDiagnostics() {
	re := suite.Require()
	core, logs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	matching, mismatched := UnifiedGC, KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		matching, mismatched = KeyspaceLevelGC, UnifiedGC
	}
	meta := suite.saveHistoricalGCConfig(3200, map[string]string{GCManagementType: mismatched})
	// Concurrent observations must emit one error, including for disabled history.
	meta.State = keyspacepb.KeyspaceState_DISABLED
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	wg := &sync.WaitGroup{}
	start := make(chan struct{})
	results := make(chan error, 50)
	for range 50 {
		wg.Go(func() {
			<-start
			_, err := suite.manager.LoadKeyspaceByID(meta.GetId())
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		re.NoError(err)
	}
	re.Len(logs.All(), 1)
	fields := logs.All()[0].ContextMap()
	re.Equal(meta.GetId(), fields["keyspace-id"])
	re.Equal(meta.Name, fields["keyspace-name"])
	re.Equal(mismatched, fields["effective-gc-mode"])
	re.Equal(true, fields["gc-management-type-present"])
	re.Equal(mismatched, fields["gc-management-type"])
	re.Equal(false, fields["safe-point-version-present"])
	re.Contains(fields, "kernel")
	re.Contains(fields, "reason")
	// Unrelated changes leave the diagnostic fingerprint unchanged.
	_, err := suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "unrelated", Value: "changed"}})
	re.NoError(err)
	re.Len(logs.All(), 1)
	// A changed malformed representation logs again even if its effective mode is unchanged.
	for _, value := range []string{"", "unknown"} {
		meta.Config[GCManagementType] = value
		re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
		_, err = suite.manager.LoadKeyspace(meta.Name)
		re.NoError(err)
	}
	re.Len(logs.All(), 3)
	// A valid observation clears the entry, so recurrence can be reported.
	meta.Config[GCManagementType] = matching
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	_, err = suite.manager.LoadRangeKeyspace(meta.GetId(), 1)
	re.NoError(err)
	meta.Config[GCManagementType] = "unknown"
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	_, err = suite.manager.LoadKeyspaceByID(meta.GetId())
	re.NoError(err)
	re.Len(logs.All(), 4)
}

func (suite *keyspaceTestSuite) TestGCConfigDiagnosticsDuringMaintenance() {
	m := suite.manager
	for i, path := range []string{"revision", "group-scan", "assignment-patrol", "removal"} {
		suite.Run(path, func() {
			re := suite.Require()
			core, logs := observer.New(zap.ErrorLevel)
			restore := log.ReplaceGlobals(zap.New(core), nil)
			defer restore()
			meta := suite.saveHistoricalGCConfig(uint32(3400+i), map[string]string{GCManagementType: "malformed", "safe_point_version": "v2"})
			meta.State = keyspacepb.KeyspaceState_ARCHIVED
			re.NoError(m.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return m.store.SaveKeyspaceMeta(txn, meta) }))
			switch path {
			case "revision":
				base := m.store
				m.store = &revisionedKeyspaceStorage{KeyspaceStorage: base, load: func(context.Context, uint32) (*keyspacepb.KeyspaceMeta, int64, error) { return meta, 123, nil }}
				loaded, revision, err := m.LoadKeyspaceByIDWithRevision(suite.ctx, meta.GetId())
				m.store = base
				re.NoError(err)
				re.Equal(int64(123), revision)
				re.Equal(meta, loaded)
			case "group-scan":
				_, err := m.CountKeyspacesByMetaServiceGroup(map[string]struct{}{"group": {}})
				re.NoError(err)
			case "assignment-patrol":
				re.NoError(m.PatrolKeyspaceAssignment(meta.GetId(), meta.GetId()))
				loaded, err := m.LoadKeyspaceByID(meta.GetId())
				re.NoError(err)
				re.Equal("malformed", loaded.Config[GCManagementType])
				re.Equal("v2", loaded.Config["safe_point_version"])
			case "removal":
				re.NoError(m.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return m.RemoveKeyspace(txn, meta.GetId()) }))
			}
			re.Len(logs.All(), 1, "each existing metadata read must observe archived history")
		})
	}
}

func (suite *keyspaceTestSuite) TestGCConfigReservedAndPreallocatedDefaults() {
	re := suite.Require()
	mode := UnifiedGC
	if kerneltype.IsNextGen() {
		mode = KeyspaceLevelGC
	}
	reserved, err := suite.manager.LoadKeyspaceByID(GetBootstrapKeyspaceID())
	re.NoError(err)
	re.Equal(mode, reserved.Config[GCManagementType])
	delete(reserved.Config, GCManagementType)
	reserved.Config["safe_point_version"] = "v2"
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, reserved) }))
	suite.manager.config.(*mockConfig).PreAlloc = []string{"gc_preallocated"}
	re.NoError(suite.manager.Bootstrap())
	loaded, err := suite.manager.LoadKeyspaceByID(reserved.GetId())
	re.NoError(err)
	re.Equal(reserved.Config, loaded.Config, "restart must retain existing reserved metadata")
	re.Eventually(func() bool {
		meta, err := suite.manager.LoadKeyspace("gc_preallocated")
		return err == nil && meta.Config[GCManagementType] == mode && meta.State == keyspacepb.KeyspaceState_ENABLED
	}, time.Second, time.Millisecond)
}

func (suite *keyspaceTestSuite) TestGCConfigPolicyBeforeGroupChanges() {
	re := suite.Require()
	m := suite.manager
	created, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "gc_group_policy"})
	re.NoError(err)
	re.NoError(m.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 123, UserKind: endpoint.Basic.String()}}))
	oldGroups, err := m.kgm.GetKeyspaceGroups(0, 0)
	re.NoError(err)
	opposite := KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		opposite = UnifiedGC
	}
	_, err = m.UpdateKeyspaceConfig(created.Name, []*Mutation{
		{Op: OpPut, Key: TSOKeyspaceGroupIDKey, Value: "123"},
		{Op: OpPut, Key: MetaServiceGroupIDKey, Value: "missing"},
		{Op: OpPut, Key: GCManagementType, Value: opposite},
	})
	re.ErrorIs(err, errs.ErrKeyspaceGCModeImmutable)
	groups, err := m.kgm.GetKeyspaceGroups(0, 0)
	re.NoError(err)
	re.Equal(oldGroups, groups)
	loaded, err := m.LoadKeyspace(created.Name)
	re.NoError(err)
	re.Equal(created.Config, loaded.Config)
	// A group validation failure must not persist successful normalization.
	history := suite.saveHistoricalGCConfig(3300, map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: endpoint.Basic.String()})
	_, err = m.UpdateKeyspaceConfig(history.Name, []*Mutation{{Op: OpPut, Key: TSOKeyspaceGroupIDKey, Value: "missing"}})
	re.Error(err)
	loaded, err = m.LoadKeyspace(history.Name)
	re.NoError(err)
	re.Equal(history.Config, loaded.Config)
}

type rejectedGCConfigTransaction struct{ endpoint.KeyspaceStorage }

func (s *rejectedGCConfigTransaction) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.KeyspaceStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		return errors.New("injected transaction rejection")
	})
}

func (suite *keyspaceTestSuite) TestGCConfigTransactionFailureDoesNotNormalize() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3301, map[string]string{"safe_point_version": "v2"})
	base := suite.manager.store
	suite.manager.store = &rejectedGCConfigTransaction{base}
	_, err := suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "partial", Value: "no"}})
	suite.manager.store = base
	re.ErrorContains(err, "injected transaction rejection")
	loaded, err := suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, loaded.Config)
}
