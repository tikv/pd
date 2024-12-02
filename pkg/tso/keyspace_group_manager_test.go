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

package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type keyspaceGroupManagerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	ClusterID        uint64
	backendEndpoints string
	etcdClient       *clientv3.Client
	clean            func()
	cfg              *TestServiceConfig
}

func TestKeyspaceGroupManagerTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupManagerTestSuite))
}

func (suite *keyspaceGroupManagerTestSuite) SetupSuite() {
	t := suite.T()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.ClusterID = rand.Uint64()
	servers, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	suite.backendEndpoints, suite.etcdClient, suite.clean = servers[0].Config().ListenClientUrls[0].String(), client, clean
	suite.cfg = suite.createConfig()
}

func (suite *keyspaceGroupManagerTestSuite) TearDownSuite() {
	suite.clean()
	suite.cancel()
}

func (suite *keyspaceGroupManagerTestSuite) createConfig() *TestServiceConfig {
	addr := tempurl.Alloc()
	return &TestServiceConfig{
		Name:                      "tso-test-name-default",
		BackendEndpoints:          suite.backendEndpoints,
		ListenAddr:                addr,
		AdvertiseListenAddr:       addr,
		LeaderLease:               constant.DefaultLeaderLease,
		LocalTSOEnabled:           false,
		TSOUpdatePhysicalInterval: 50 * time.Millisecond,
		TSOSaveInterval:           time.Duration(constant.DefaultLeaderLease) * time.Second,
		MaxResetTSGap:             time.Hour * 24,
		TLSConfig:                 nil,
	}
}

func (suite *keyspaceGroupManagerTestSuite) TestDeletedGroupCleanup() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastDeletedGroupCleaner", "return(true)"))

	// Start with the empty keyspace group assignment.
	mgr := suite.newUniqueKeyspaceGroupManager(0)
	re.NotNil(mgr)
	defer mgr.Close()
	err := mgr.Initialize()
	re.NoError(err)

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	// Add keyspace group 1.
	suite.applyEtcdEvents(re, rootPath, []*etcdEvent{generateKeyspaceGroupPutEvent(1, []uint32{1}, []string{svcAddr})})
	// Check if the TSO key is created.
	testutil.Eventually(re, func() bool {
		ts, err := mgr.tsoSvcStorage.LoadTimestamp(keypath.KeyspaceGroupGlobalTSPath(1))
		re.NoError(err)
		return ts != typeutil.ZeroTime
	})
	// Delete keyspace group 1.
	suite.applyEtcdEvents(re, rootPath, []*etcdEvent{generateKeyspaceGroupDeleteEvent(1)})
	// Check if the TSO key is deleted.
	testutil.Eventually(re, func() bool {
		ts, err := mgr.tsoSvcStorage.LoadTimestamp(keypath.KeyspaceGroupGlobalTSPath(1))
		re.NoError(err)
		return ts == typeutil.ZeroTime
	})
	// Check if the keyspace group is deleted completely.
	mgr.RLock()
	re.Nil(mgr.ams[1])
	re.Nil(mgr.kgs[1])
	re.NotContains(mgr.deletedGroups, 1)
	mgr.RUnlock()
	// Try to delete the default keyspace group.
	suite.applyEtcdEvents(re, rootPath, []*etcdEvent{generateKeyspaceGroupDeleteEvent(constant.DefaultKeyspaceGroupID)})
	// Default keyspace group should NOT be deleted.
	mgr.RLock()
	re.NotNil(mgr.ams[constant.DefaultKeyspaceGroupID])
	re.NotNil(mgr.kgs[constant.DefaultKeyspaceGroupID])
	re.NotContains(mgr.deletedGroups, constant.DefaultKeyspaceGroupID)
	mgr.RUnlock()
	// Default keyspace group TSO key should NOT be deleted.
	ts, err := mgr.legacySvcStorage.LoadTimestamp(keypath.KeyspaceGroupGlobalTSPath(constant.DefaultKeyspaceGroupID))
	re.NoError(err)
	re.NotEmpty(ts)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastDeletedGroupCleaner"))
}

// TestNewKeyspaceGroupManager tests the initialization of KeyspaceGroupManager.
// It should initialize the allocator manager with the desired configurations and parameters.
func (suite *keyspaceGroupManagerTestSuite) TestNewKeyspaceGroupManager() {
	re := suite.Require()

	tsoServiceID := &discovery.ServiceRegistryEntry{ServiceAddr: suite.cfg.AdvertiseListenAddr}
	clusterID, err := endpoint.InitClusterID(suite.etcdClient)
	re.NoError(err)
	clusterIDStr := strconv.FormatUint(clusterID, 10)
	keypath.SetClusterID(clusterID)

	legacySvcRootPath := path.Join("/pd", clusterIDStr)
	tsoSvcRootPath := path.Join(constant.MicroserviceRootPath, clusterIDStr, "tso")
	electionNamePrefix := "tso-server-" + clusterIDStr

	kgm := NewKeyspaceGroupManager(
		suite.ctx, tsoServiceID, suite.etcdClient, nil, electionNamePrefix,
		legacySvcRootPath, tsoSvcRootPath, suite.cfg)
	defer kgm.Close()
	re.NoError(kgm.Initialize())

	re.Equal(tsoServiceID, kgm.tsoServiceID)
	re.Equal(suite.etcdClient, kgm.etcdClient)
	re.Equal(electionNamePrefix, kgm.electionNamePrefix)
	re.Equal(legacySvcRootPath, kgm.legacySvcRootPath)
	re.Equal(tsoSvcRootPath, kgm.tsoSvcRootPath)
	re.Equal(suite.cfg, kgm.cfg)

	am, err := kgm.GetAllocatorManager(constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, am.kgID)
	re.Equal(constant.DefaultLeaderLease, am.leaderLease)
	re.Equal(time.Hour*24, am.maxResetTSGap())
	re.Equal(legacySvcRootPath, am.rootPath)
	re.Equal(time.Duration(constant.DefaultLeaderLease)*time.Second, am.saveInterval)
	re.Equal(time.Duration(50)*time.Millisecond, am.updatePhysicalInterval)
}

// TestLoadKeyspaceGroupsAssignment tests the loading of the keyspace group assignment.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsAssignment() {
	re := suite.Require()
	maxCountInUse := 512
	// Test loading of empty keyspace group assignment.
	suite.runTestLoadKeyspaceGroupsAssignment(re, 0, 0, 100)
	// Test loading of single keyspace group assignment.
	suite.runTestLoadKeyspaceGroupsAssignment(re, 1, 0, 100)
	// Test loading of multiple keyspace group assignment.
	suite.runTestLoadKeyspaceGroupsAssignment(re, 3, 0, 100)
	suite.runTestLoadKeyspaceGroupsAssignment(re, maxCountInUse-1, 0, 10)
	suite.runTestLoadKeyspaceGroupsAssignment(re, maxCountInUse, 0, 10)
	// Test loading of the keyspace group assignment which exceeds the maximum keyspace group count.
	// In this case, the manager should only load/serve the first MaxKeyspaceGroupCountInUse keyspace
	// groups and ignore the rest.
	suite.runTestLoadKeyspaceGroupsAssignment(re, maxCountInUse+1, 0, 10)
}

// TestLoadWithDifferentBatchSize tests the loading of the keyspace group assignment with the different batch size.
func (suite *keyspaceGroupManagerTestSuite) TestLoadWithDifferentBatchSize() {
	re := suite.Require()

	batchSize := int64(17)
	maxCount := uint32(1024)
	params := []struct {
		batchSize             int64
		count                 int
		probabilityAssignToMe int // percentage of assigning keyspace groups to this host/pod
	}{
		{batchSize: 1, count: 1, probabilityAssignToMe: 100},
		{batchSize: 2, count: int(maxCount / 10), probabilityAssignToMe: 100},
		{batchSize: 7, count: int(maxCount / 10), probabilityAssignToMe: 100},
		{batchSize: batchSize, count: int(batchSize), probabilityAssignToMe: 50},
		{batchSize: int64(maxCount / 13), count: int(maxCount / 13), probabilityAssignToMe: 50},
		{batchSize: int64(maxCount), count: int(maxCount / 13), probabilityAssignToMe: 10},
	}

	for _, param := range params {
		suite.runTestLoadKeyspaceGroupsAssignment(re, param.count-1, param.batchSize, param.probabilityAssignToMe)
		suite.runTestLoadKeyspaceGroupsAssignment(re, param.count, param.batchSize, param.probabilityAssignToMe)
		suite.runTestLoadKeyspaceGroupsAssignment(re, param.count+1, param.batchSize, param.probabilityAssignToMe)
	}
}

// TestLoadKeyspaceGroupsSucceedWithTempFailures tests the initialization should succeed when there are temporary
// failures during loading the initial keyspace group assignment from etcd.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsSucceedWithTempFailures() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(0), mgr.legacySvcRootPath,
		[]string{mgr.tsoServiceID.ServiceAddr}, []int{0}, []uint32{0})

	// Set the max retry times to 3 and inject the loadTemporaryFail to return 2 to let
	// loading from etcd fail 2 times but the whole initialization still succeeds.
	mgr.loadFromEtcdMaxRetryTimes = 3
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(2)"))
	err := mgr.Initialize()
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
}

// TestLoadKeyspaceGroupsFailed tests the initialization should fail when there are too many failures
// during loading the initial keyspace group assignment from etcd.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsFailed() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(0), mgr.legacySvcRootPath,
		[]string{mgr.tsoServiceID.ServiceAddr}, []int{0}, []uint32{0})

	// Set the max retry times to 3 and inject the loadTemporaryFail to return 3 to let
	// loading from etcd fail 3 times which should cause the whole initialization to fail.
	mgr.loadFromEtcdMaxRetryTimes = 3
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(3)"))
	err := mgr.Initialize()
	re.Error(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
}

// TestWatchAndDynamicallyApplyChanges tests the keyspace group manager watch and dynamically apply
// keyspace groups' membership/distribution meta changes.
func (suite *keyspaceGroupManagerTestSuite) TestWatchAndDynamicallyApplyChanges() {
	re := suite.Require()

	// Start with the empty keyspace group assignment.
	mgr := suite.newUniqueKeyspaceGroupManager(0)
	re.NotNil(mgr)
	defer mgr.Close()
	err := mgr.Initialize()
	re.NoError(err)

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	// Initialize PUT/DELETE events
	events := []*etcdEvent{}
	// Assign keyspace group 0 to this host/pod/keyspace-group-manager.
	// final result: assigned [0], loaded [0]
	events = append(events, generateKeyspaceGroupPutEvent(0, []uint32{0}, []string{svcAddr}))
	// Assign keyspace group 1 to this host/pod/keyspace-group-manager.
	// final result: assigned [0,1], loaded [0,1]
	events = append(events, generateKeyspaceGroupPutEvent(1, []uint32{1}, []string{"unknown", svcAddr}))
	// Assign keyspace group 2 to other host/pod/keyspace-group-manager.
	// final result: assigned [0,1], loaded [0,1,2]
	events = append(events, generateKeyspaceGroupPutEvent(2, []uint32{2}, []string{"unknown"}))
	// Assign keyspace group 3 to this host/pod/keyspace-group-manager.
	// final result: assigned [0,1,3], loaded [0,1,2,3]
	events = append(events, generateKeyspaceGroupPutEvent(3, []uint32{3}, []string{svcAddr}))
	// Delete keyspace group 0. Every tso node/pod now should initialize keyspace group 0.
	// final result: assigned [0,1,3], loaded [0,1,2,3]
	events = append(events, generateKeyspaceGroupDeleteEvent(0))
	// Put keyspace group 4 which doesn't belong to anyone.
	// final result: assigned [0,1,3], loaded [0,1,2,3,4]
	events = append(events, generateKeyspaceGroupPutEvent(4, []uint32{4}, []string{}))
	// Put keyspace group 5 which doesn't belong to anyone.
	// final result: assigned [0,1,3], loaded [0,1,2,3,4,5]
	events = append(events, generateKeyspaceGroupPutEvent(5, []uint32{5}, []string{}))
	// Assign keyspace group 2 to this host/pod/keyspace-group-manager.
	// final result: assigned [0,1,2,3], loaded [0,1,2,3,4,5]
	events = append(events, generateKeyspaceGroupPutEvent(2, []uint32{2}, []string{svcAddr}))
	// Reassign keyspace group 3 to no one.
	// final result: assigned [0,1,2], loaded [0,1,2,3,4,5]
	events = append(events, generateKeyspaceGroupPutEvent(3, []uint32{3}, []string{}))
	// Reassign keyspace group 4 to this host/pod/keyspace-group-manager.
	// final result: assigned [0,1,2,4], loaded [0,1,2,3,4,5]
	events = append(events, generateKeyspaceGroupPutEvent(4, []uint32{4}, []string{svcAddr}))
	// Delete keyspace group 2.
	// final result: assigned [0,1,4], loaded [0,1,3,4,5]
	events = append(events, generateKeyspaceGroupDeleteEvent(2))

	// Apply the keyspace group assignment change events to etcd.
	suite.applyEtcdEvents(re, rootPath, events)

	// Verify the keyspace groups assigned.
	// Eventually, this keyspace groups manager is expected to serve the following keyspace groups.
	expectedAssignedGroups := []uint32{0, 1, 4}
	testutil.Eventually(re, func() bool {
		assignedGroups := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedAssignedGroups, assignedGroups)
	})

	// Verify the keyspace groups loaded.
	// Eventually, this keyspace groups manager is expected to load the following keyspace groups
	// in which keyspace group 3, 5 aren't served by this tso node/pod.
	expectedLoadedGroups := []uint32{0, 1, 3, 4, 5}
	testutil.Eventually(re, func() bool {
		loadedGroups := collectAllLoadedKeyspaceGroupIDs(mgr)
		return reflect.DeepEqual(expectedLoadedGroups, loadedGroups)
	})
}

// TestDefaultKeyspaceGroup tests the initialization logic of the default keyspace group.
// If the default keyspace group isn't configured in the etcd, every tso node/pod should initialize
// it and join the election for the primary of this group.
// If the default keyspace group is configured in the etcd, the tso nodes/pods which are assigned with
// this group will initialize it and join the election for the primary of this group.
func (suite *keyspaceGroupManagerTestSuite) TestInitDefaultKeyspaceGroup() {
	re := suite.Require()

	var (
		expectedGroupIDs []uint32
		event            *etcdEvent
	)

	// Start with the empty keyspace group assignment.
	mgr := suite.newUniqueKeyspaceGroupManager(0)
	defer mgr.Close()
	err := mgr.Initialize()
	re.NoError(err)

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	expectedGroupIDs = []uint32{0}
	assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
	re.Equal(expectedGroupIDs, assignedGroupIDs)

	// Config keyspace group 0 in the storage but assigned to no one.
	// final result: []
	expectedGroupIDs = []uint32{}
	event = generateKeyspaceGroupPutEvent(0, []uint32{0}, []string{"unknown"})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedGroupIDs, assignedGroupIDs)
	})
	// Config keyspace group 0 in the storage and assigned to this host/pod/keyspace-group-manager.
	// final result: [0]
	expectedGroupIDs = []uint32{0}
	event = generateKeyspaceGroupPutEvent(0, []uint32{0}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedGroupIDs, assignedGroupIDs)
	})
	// Delete keyspace group 0. Every tso node/pod now should initialize keyspace group 0.
	// final result: [0]
	expectedGroupIDs = []uint32{0}
	event = generateKeyspaceGroupDeleteEvent(0)
	err = deleteKeyspaceGroupInEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg.ID)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedGroupIDs, assignedGroupIDs)
	})
	// Config keyspace group 0 in the storage and assigned to this host/pod/keyspace-group-manager.
	// final result: [0]
	expectedGroupIDs = []uint32{0}
	event = generateKeyspaceGroupPutEvent(0, []uint32{0}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedGroupIDs, assignedGroupIDs)
	})
}

// TestGetKeyspaceGroupMetaWithCheck tests GetKeyspaceGroupMetaWithCheck.
func (suite *keyspaceGroupManagerTestSuite) TestGetKeyspaceGroupMetaWithCheck() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	var (
		am   *AllocatorManager
		kg   *endpoint.KeyspaceGroup
		kgid uint32
		err  error
	)

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(0), mgr.legacySvcRootPath,
		[]string{mgr.tsoServiceID.ServiceAddr}, []int{0}, []uint32{0, 1, 2})

	err = mgr.Initialize()
	re.NoError(err)

	// Should be able to get AM for the default/null keyspace and keyspace 1, 2 in keyspace group 0.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(constant.DefaultKeyspaceID, 0)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(constant.NullKeyspaceID, 0)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(1, 0)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(2, 0)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	// Should still succeed even keyspace 3 isn't explicitly assigned to any
	// keyspace group. It will be assigned to the default keyspace group.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(3, 0)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	// Should succeed and get the meta of keyspace group 0, because keyspace 0
	// belongs to group 0, though the specified group 1 doesn't exist.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(constant.DefaultKeyspaceID, 1)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	// Should fail because keyspace 3 isn't explicitly assigned to any keyspace
	// group, and the specified group isn't the default keyspace group.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(3, 100)
	re.Error(err)
	re.Equal(uint32(100), kgid)
	re.Nil(am)
	re.Nil(kg)
}

// TestDefaultMembershipRestriction tests the restriction of default keyspace always
// belongs to default keyspace group.
func (suite *keyspaceGroupManagerTestSuite) TestDefaultMembershipRestriction() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	var (
		am    *AllocatorManager
		kg    *endpoint.KeyspaceGroup
		kgid  uint32
		err   error
		event *etcdEvent
	)

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, constant.DefaultKeyspaceGroupID, rootPath,
		[]string{svcAddr}, []int{0}, []uint32{constant.DefaultKeyspaceID, 1, 2})
	// Create keyspace group 3 which contains keyspace 3, 4.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(3), mgr.legacySvcRootPath,
		[]string{mgr.tsoServiceID.ServiceAddr}, []int{0}, []uint32{3, 4})

	err = mgr.Initialize()
	re.NoError(err)

	// Should be able to get AM for keyspace 0 in keyspace group 0.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(
		constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)

	event = generateKeyspaceGroupPutEvent(
		constant.DefaultKeyspaceGroupID, []uint32{1, 2}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)
	event = generateKeyspaceGroupPutEvent(
		3, []uint32{constant.DefaultKeyspaceID, 3, 4}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)

	// Sleep for a while to wait for the events to propagate. If the logic doesn't work
	// as expected, it will cause random failure.
	time.Sleep(1 * time.Second)
	// Should still be able to get AM for keyspace 0 in keyspace group 0.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(
		constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
	// Should succeed and return the keyspace group meta from the default keyspace group
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(constant.DefaultKeyspaceID, 3)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)
}

// TestKeyspaceMovementConsistency tests the consistency of keyspace movement.
// When a keyspace is moved from one keyspace group to another, the allocator manager
// update source group and target group state in etcd atomically. The TSO keyspace group
// manager watches the state change in persistent store but hard to apply the movement state
// change across two groups atomically. This test case is to test the movement state is
// eventually consistent, for example, if a keyspace "move to group B" event is applied
// before "move away from group A" event, the second event shouldn't overwrite the global
// state, such as the global keyspace group lookup table.
func (suite *keyspaceGroupManagerTestSuite) TestKeyspaceMovementConsistency() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	var (
		am    *AllocatorManager
		kg    *endpoint.KeyspaceGroup
		kgid  uint32
		err   error
		event *etcdEvent
	)

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, constant.DefaultKeyspaceGroupID,
		rootPath, []string{svcAddr}, []int{0}, []uint32{constant.DefaultKeyspaceID, 10, 20})
	// Create keyspace group 1 which contains keyspace 3, 4.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(1), rootPath,
		[]string{svcAddr}, []int{0}, []uint32{11, 21})

	err = mgr.Initialize()
	re.NoError(err)

	// Should be able to get AM for keyspace 10 in keyspace group 0.
	am, kg, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(10, constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, kgid)
	re.NotNil(am)
	re.NotNil(kg)

	// Move keyspace 10 from keyspace group 0 to keyspace group 1 and apply this state change
	// to TSO first.
	event = generateKeyspaceGroupPutEvent(1, []uint32{10, 11, 21}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)
	// Wait until the keyspace 10 is served by keyspace group 1.
	testutil.Eventually(re, func() bool {
		_, _, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(10, 1)
		return err == nil && kgid == 1
	}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	event = generateKeyspaceGroupPutEvent(
		constant.DefaultKeyspaceGroupID, []uint32{constant.DefaultKeyspaceID, 20}, []string{svcAddr})
	err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
	re.NoError(err)

	// Sleep for a while to wait for the events to propagate. If the restriction is not working,
	// it will cause random failure.
	time.Sleep(1 * time.Second)
	// Should still be able to get AM for keyspace 10 in keyspace group 1.
	_, _, kgid, err = mgr.getKeyspaceGroupMetaWithCheck(10, 1)
	re.NoError(err)
	re.Equal(uint32(1), kgid)
}

// TestHandleTSORequestWithWrongMembership tests the case that HandleTSORequest receives
// a tso request with mismatched keyspace and keyspace group.
func (suite *keyspaceGroupManagerTestSuite) TestHandleTSORequestWithWrongMembership() {
	re := suite.Require()

	mgr := suite.newUniqueKeyspaceGroupManager(1)
	re.NotNil(mgr)
	defer mgr.Close()

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, uint32(0), mgr.legacySvcRootPath,
		[]string{mgr.tsoServiceID.ServiceAddr}, []int{0}, []uint32{0, 1, 2})

	err := mgr.Initialize()
	re.NoError(err)

	// Wait until the keyspace group 0 is ready for serving tso requests.
	testutil.Eventually(re, func() bool {
		member, err := mgr.GetElectionMember(0, 0)
		if err != nil {
			return false
		}
		return member.IsLeader()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Should succeed because keyspace 0 is actually in keyspace group 0, which is served
	// by the current keyspace group manager, instead of keyspace group 1 in ask, and
	// keyspace group 0 is returned in the response.
	_, keyspaceGroupBelongTo, err := mgr.HandleTSORequest(suite.ctx, 0, 1, 1)
	re.NoError(err)
	re.Equal(uint32(0), keyspaceGroupBelongTo)

	// Should succeed because keyspace 100 doesn't belong to any keyspace group, so it will
	// be served by the default keyspace group 0, and keyspace group 0 is returned in the response.
	_, keyspaceGroupBelongTo, err = mgr.HandleTSORequest(suite.ctx, 100, 0, 1)
	re.NoError(err)
	re.Equal(uint32(0), keyspaceGroupBelongTo)

	// Should fail because keyspace 100 doesn't belong to any keyspace group, and the keyspace group
	// 1 in ask doesn't exist.
	_, keyspaceGroupBelongTo, err = mgr.HandleTSORequest(suite.ctx, 100, 1, 1)
	re.Error(err)
	re.Equal(uint32(1), keyspaceGroupBelongTo)
}

type etcdEvent struct {
	eventType mvccpb.Event_EventType
	ksg       *endpoint.KeyspaceGroup
}

func generateKeyspaceGroupPutEvent(
	groupID uint32, keyspaces []uint32, addrs []string, splitState ...*endpoint.SplitState,
) *etcdEvent {
	members := []endpoint.KeyspaceGroupMember{}
	for _, addr := range addrs {
		members = append(members, endpoint.KeyspaceGroupMember{Address: addr})
	}
	var ss *endpoint.SplitState
	if len(splitState) > 0 {
		ss = splitState[0]
	}

	return &etcdEvent{
		eventType: mvccpb.PUT,
		ksg: &endpoint.KeyspaceGroup{
			ID:         groupID,
			Members:    members,
			Keyspaces:  keyspaces,
			SplitState: ss,
		},
	}
}

func generateKeyspaceGroupDeleteEvent(groupID uint32) *etcdEvent {
	return &etcdEvent{
		eventType: mvccpb.DELETE,
		ksg: &endpoint.KeyspaceGroup{
			ID: groupID,
		},
	}
}

func (suite *keyspaceGroupManagerTestSuite) applyEtcdEvents(
	re *require.Assertions,
	rootPath string,
	events []*etcdEvent,
) {
	var err error
	for _, event := range events {
		switch event.eventType {
		case mvccpb.PUT:
			err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
		case mvccpb.DELETE:
			err = deleteKeyspaceGroupInEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg.ID)
		}
		re.NoError(err)
	}
}

// runTestLoadMultipleKeyspaceGroupsAssignment tests the loading of multiple keyspace group assignment.
func (suite *keyspaceGroupManagerTestSuite) runTestLoadKeyspaceGroupsAssignment(
	re *require.Assertions,
	numberOfKeyspaceGroupsToAdd int,
	loadKeyspaceGroupsBatchSize int64, // set to 0 to use the default value
	probabilityAssignToMe int, // percentage of assigning keyspace groups to this host/pod
) {
	expectedGroupIDs := []uint32{}
	mgr := suite.newUniqueKeyspaceGroupManager(loadKeyspaceGroupsBatchSize)
	re.NotNil(mgr)
	defer mgr.Close()

	step := 30
	mux := syncutil.Mutex{}
	wg := sync.WaitGroup{}
	for i := 0; i < numberOfKeyspaceGroupsToAdd; i += step {
		wg.Add(1)
		go func(startID int) {
			defer wg.Done()

			endID := startID + step
			if endID > numberOfKeyspaceGroupsToAdd {
				endID = numberOfKeyspaceGroupsToAdd
			}

			randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
			for j := startID; j < endID; j++ {
				assignToMe := false
				// Assign the keyspace group to this host/pod with the given probability,
				// and the keyspace group manager only loads the keyspace groups with id
				// less than len(mgr.ams).
				if j < len(mgr.ams) && randomGen.Intn(100) < probabilityAssignToMe {
					assignToMe = true
					mux.Lock()
					expectedGroupIDs = append(expectedGroupIDs, uint32(j))
					mux.Unlock()
				}

				svcAddrs := make([]string, 0)
				if assignToMe {
					svcAddrs = append(svcAddrs, mgr.tsoServiceID.ServiceAddr)
				} else {
					svcAddrs = append(svcAddrs, fmt.Sprintf("test-%d", rand.Uint64()))
				}
				addKeyspaceGroupAssignment(
					suite.ctx, suite.etcdClient, uint32(j), mgr.legacySvcRootPath,
					svcAddrs, []int{0}, []uint32{uint32(j)})
			}
		}(i)
	}
	wg.Wait()

	err := mgr.Initialize()
	re.NoError(err)

	// If no keyspace group is assigned to this host/pod, the default keyspace group should be initialized.
	if numberOfKeyspaceGroupsToAdd <= 0 {
		expectedGroupIDs = append(expectedGroupIDs, constant.DefaultKeyspaceGroupID)
	}

	// Verify the keyspace group assignment.
	// Sort the keyspaces in ascending order
	sort.Slice(expectedGroupIDs, func(i, j int) bool {
		return expectedGroupIDs[i] < expectedGroupIDs[j]
	})
	assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
	re.Equal(expectedGroupIDs, assignedGroupIDs)
}

func (suite *keyspaceGroupManagerTestSuite) newUniqueKeyspaceGroupManager(
	loadKeyspaceGroupsBatchSize int64, // set to 0 to use the default value
) *KeyspaceGroupManager {
	return suite.newKeyspaceGroupManager(loadKeyspaceGroupsBatchSize, rand.Uint64(), suite.cfg)
}

func (suite *keyspaceGroupManagerTestSuite) newKeyspaceGroupManager(
	loadKeyspaceGroupsBatchSize int64, // set to 0 to use the default value
	clusterID uint64,
	cfg *TestServiceConfig,
) *KeyspaceGroupManager {
	tsoServiceID := &discovery.ServiceRegistryEntry{ServiceAddr: cfg.GetAdvertiseListenAddr()}
	clusterIDStr := strconv.FormatUint(clusterID, 10)
	legacySvcRootPath := path.Join("/pd", clusterIDStr)
	tsoSvcRootPath := path.Join(constant.MicroserviceRootPath, clusterIDStr, "tso")
	electionNamePrefix := "kgm-test-" + cfg.GetAdvertiseListenAddr()

	kgm := NewKeyspaceGroupManager(
		suite.ctx, tsoServiceID, suite.etcdClient, nil, electionNamePrefix,
		legacySvcRootPath, tsoSvcRootPath, cfg)
	if loadKeyspaceGroupsBatchSize != 0 {
		kgm.loadKeyspaceGroupsBatchSize = loadKeyspaceGroupsBatchSize
	}
	return kgm
}

// putKeyspaceGroupToEtcd puts a keyspace group to etcd.
func putKeyspaceGroupToEtcd(
	ctx context.Context, etcdClient *clientv3.Client,
	rootPath string, group *endpoint.KeyspaceGroup,
) error {
	key := strings.Join([]string{rootPath, keypath.KeyspaceGroupIDPath(group.ID)}, "/")
	value, err := json.Marshal(group)
	if err != nil {
		return err
	}

	if _, err := etcdClient.Put(ctx, key, string(value)); err != nil {
		return err
	}

	return nil
}

// deleteKeyspaceGroupInEtcd deletes a keyspace group in etcd.
func deleteKeyspaceGroupInEtcd(
	ctx context.Context, etcdClient *clientv3.Client,
	rootPath string, id uint32,
) error {
	key := strings.Join([]string{rootPath, keypath.KeyspaceGroupIDPath(id)}, "/")

	if _, err := etcdClient.Delete(ctx, key); err != nil {
		return err
	}

	return nil
}

// addKeyspaceGroupAssignment adds a keyspace group assignment to etcd.
func addKeyspaceGroupAssignment(
	ctx context.Context,
	etcdClient *clientv3.Client,
	groupID uint32,
	rootPath string,
	svcAddrs []string,
	priorities []int,
	keyspaces []uint32,
) error {
	members := make([]endpoint.KeyspaceGroupMember, len(svcAddrs))
	for i, svcAddr := range svcAddrs {
		members[i] = endpoint.KeyspaceGroupMember{Address: svcAddr, Priority: priorities[i]}
	}
	group := &endpoint.KeyspaceGroup{
		ID:        groupID,
		Members:   members,
		Keyspaces: keyspaces,
	}

	key := strings.Join([]string{rootPath, keypath.KeyspaceGroupIDPath(groupID)}, "/")
	value, err := json.Marshal(group)
	if err != nil {
		return err
	}

	if _, err := etcdClient.Put(ctx, key, string(value)); err != nil {
		return err
	}

	return nil
}

func collectAssignedKeyspaceGroupIDs(re *require.Assertions, kgm *KeyspaceGroupManager) []uint32 {
	kgm.RLock()
	defer kgm.RUnlock()

	ids := []uint32{}
	for i := range kgm.kgs {
		kg := kgm.kgs[i]
		if kg == nil {
			re.Nil(kgm.ams[i], fmt.Sprintf("ksg is nil but am is not nil for id %d", i))
		} else {
			am := kgm.ams[i]
			if am != nil {
				re.Equal(i, int(am.kgID))
				re.Equal(i, int(kg.ID))
				for _, m := range kg.Members {
					if m.IsAddressEquivalent(kgm.tsoServiceID.ServiceAddr) {
						ids = append(ids, uint32(i))
						break
					}
				}
			}
		}
	}

	return ids
}

func collectAllLoadedKeyspaceGroupIDs(kgm *KeyspaceGroupManager) []uint32 {
	kgm.RLock()
	defer kgm.RUnlock()

	ids := []uint32{}
	for i := range kgm.kgs {
		kg := kgm.kgs[i]
		if kg != nil {
			ids = append(ids, uint32(i))
		}
	}

	return ids
}

func (suite *keyspaceGroupManagerTestSuite) TestUpdateKeyspaceGroupMembership() {
	re := suite.Require()

	// Start from an empty keyspace group.
	// Use non-default keyspace group ID.
	// The default keyspace group always contains the default keyspace.
	// We have dedicated tests for the default keyspace group.
	groupID := uint32(1)
	oldGroup := &endpoint.KeyspaceGroup{ID: groupID, Keyspaces: []uint32{}}
	newGroup := &endpoint.KeyspaceGroup{ID: groupID, Keyspaces: []uint32{}}
	kgm := &KeyspaceGroupManager{
		state: state{
			keyspaceLookupTable: make(map[uint32]uint32),
		}}

	kgm.updateKeyspaceGroupMembership(oldGroup, newGroup, true)
	verifyLocalKeyspaceLookupTable(re, newGroup.KeyspaceLookupTable, newGroup.Keyspaces)
	verifyGlobalKeyspaceLookupTable(re, kgm.keyspaceLookupTable, newGroup.KeyspaceLookupTable)

	targetKeyspacesList := [][]uint32{
		{1},                         // Add keyspace 1 to the keyspace group.
		{1, 2},                      // Add keyspace 2 to the keyspace group.
		{1, 2},                      // No change.
		{1, 2, 3, 4},                // Add keyspace 3 and 4 to the keyspace group.
		{5, 6, 7},                   // Remove keyspace 1, 2, 3, 4 from the keyspace group and add 5, 6, 7
		{7, 8, 9},                   // Partially update the keyspace group.
		{1, 2, 3, 4, 5, 6, 7, 8, 9}, // Add more keyspace to the keyspace group.
		{9, 8, 4, 5, 6},             // Out of order.
		{9, 8, 4, 5, 6},             // No change. Out of order.
		{8, 9},                      // Remove
		{10},                        // Remove
		{},                          // End with the empty keyspace group.
	}

	for _, keyspaces := range targetKeyspacesList {
		oldGroup = newGroup
		keyspacesCopy := make([]uint32, len(keyspaces))
		copy(keyspacesCopy, keyspaces)
		newGroup = &endpoint.KeyspaceGroup{ID: groupID, Keyspaces: keyspacesCopy}
		kgm.updateKeyspaceGroupMembership(oldGroup, newGroup, true)
		verifyLocalKeyspaceLookupTable(re, newGroup.KeyspaceLookupTable, newGroup.Keyspaces)
		verifyGlobalKeyspaceLookupTable(re, kgm.keyspaceLookupTable, newGroup.KeyspaceLookupTable)

		// Verify the keyspaces loaded is sorted.
		re.Equal(len(keyspaces), len(newGroup.Keyspaces))
		for i := range newGroup.Keyspaces {
			if i > 0 {
				re.Less(newGroup.Keyspaces[i-1], newGroup.Keyspaces[i])
			}
		}
	}
}

func verifyLocalKeyspaceLookupTable(
	re *require.Assertions, keyspaceLookupTable map[uint32]struct{}, newKeyspaces []uint32,
) {
	re.Equal(len(newKeyspaces), len(keyspaceLookupTable),
		fmt.Sprintf("%v %v", newKeyspaces, keyspaceLookupTable))
	for _, keyspace := range newKeyspaces {
		_, ok := keyspaceLookupTable[keyspace]
		re.True(ok)
	}
}

func verifyGlobalKeyspaceLookupTable(
	re *require.Assertions,
	gKeyspaceLookupTable map[uint32]uint32,
	lKeyspaceLookupTable map[uint32]struct{},
) {
	for keyspace := range gKeyspaceLookupTable {
		_, ok := lKeyspaceLookupTable[keyspace]
		re.True(ok)
	}
	for keyspace := range lKeyspaceLookupTable {
		_, ok := gKeyspaceLookupTable[keyspace]
		re.True(ok)
	}
}

func (suite *keyspaceGroupManagerTestSuite) TestGroupSplitUpdateRetry() {
	re := suite.Require()

	// Start with the empty keyspace group assignment.
	mgr := suite.newUniqueKeyspaceGroupManager(0)
	re.NotNil(mgr)
	defer mgr.Close()
	err := mgr.Initialize()
	re.NoError(err)

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	events := []*etcdEvent{}
	// Split target keyspace group event arrives first.
	events = append(events, generateKeyspaceGroupPutEvent(2, []uint32{2} /* Mock 2 replicas */, []string{svcAddr, svcAddr}, &endpoint.SplitState{
		SplitSource: 1,
	}))
	// Split source keyspace group event arrives later.
	events = append(events, generateKeyspaceGroupPutEvent(1, []uint32{1}, []string{svcAddr, svcAddr}, &endpoint.SplitState{
		SplitSource: 1,
	}))

	// Eventually, this keyspace groups manager is expected to serve the following keyspace groups.
	expectedGroupIDs := []uint32{0, 1, 2}

	// Apply the keyspace group assignment change events to etcd.
	suite.applyEtcdEvents(re, rootPath, events)

	// Verify the keyspace group assignment.
	testutil.Eventually(re, func() bool {
		assignedGroupIDs := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(expectedGroupIDs, assignedGroupIDs)
	})
}

// TestPrimaryPriorityChange tests the case that the primary priority of a keyspace group changes
// and the locations of the primaries should be updated accordingly.
func (suite *keyspaceGroupManagerTestSuite) TestPrimaryPriorityChange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastPrimaryPriorityCheck", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastPrimaryPriorityCheck"))
	}()

	var err error
	defaultPriority := constant.DefaultKeyspaceGroupReplicaPriority
	clusterID, err := endpoint.InitClusterID(suite.etcdClient)
	re.NoError(err)
	clusterIDStr := strconv.FormatUint(clusterID, 10)
	rootPath := path.Join("/pd", clusterIDStr)
	cfg1 := suite.createConfig()
	cfg2 := suite.createConfig()
	svcAddr1 := cfg1.GetAdvertiseListenAddr()
	svcAddr2 := cfg2.GetAdvertiseListenAddr()

	// Register TSO server 1
	cfg1.Name = "tso1"
	err = suite.registerTSOServer(re, svcAddr1, cfg1)
	re.NoError(err)
	defer func() {
		re.NoError(suite.deregisterTSOServer(svcAddr1))
	}()

	// Create three keyspace groups on two TSO servers with default replica priority.
	ids := []uint32{0, constant.MaxKeyspaceGroupCountInUse / 2, constant.MaxKeyspaceGroupCountInUse - 1}
	for _, id := range ids {
		addKeyspaceGroupAssignment(
			suite.ctx, suite.etcdClient, id, rootPath,
			[]string{svcAddr1, svcAddr2}, []int{defaultPriority, defaultPriority}, []uint32{id})
	}

	// Create the first TSO server which loads all three keyspace groups created above.
	// All primaries should be on the first TSO server.
	mgr1 := suite.newKeyspaceGroupManager(1, clusterID, cfg1)
	re.NotNil(mgr1)
	defer mgr1.Close()
	err = mgr1.Initialize()
	re.NoError(err)
	// Wait until all keyspace groups are ready for serving tso requests.
	waitForPrimariesServing(re, []*KeyspaceGroupManager{mgr1, mgr1, mgr1}, ids)

	// We increase the priority of the TSO server 2 which hasn't started yet. The primaries
	// on the TSO server 1 shouldn't move.
	for _, id := range ids {
		addKeyspaceGroupAssignment(
			suite.ctx, suite.etcdClient, id, rootPath,
			[]string{svcAddr1, svcAddr2}, []int{defaultPriority, defaultPriority + 1}, []uint32{id})
	}

	// And the primaries on TSO Server 1 should continue to serve TSO requests without any failures.
	for range 100 {
		for _, id := range ids {
			_, keyspaceGroupBelongTo, err := mgr1.HandleTSORequest(suite.ctx, id, id, 1)
			re.NoError(err)
			re.Equal(id, keyspaceGroupBelongTo)
		}
	}

	// Continually sending TSO requests to the TSO server 1 to make sure the primaries will move back
	// to it at the end of test
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	checkTSO(ctx, re, &wg, mgr1, ids)

	// Create the Second TSO server.
	cfg2.Name = "tso2"
	err = suite.registerTSOServer(re, svcAddr2, cfg2)
	re.NoError(err)
	mgr2 := suite.newKeyspaceGroupManager(1, clusterID, cfg2)
	re.NotNil(mgr2)
	err = mgr2.Initialize()
	re.NoError(err)
	// All primaries should eventually move to the second TSO server because of the higher priority.
	waitForPrimariesServing(re, []*KeyspaceGroupManager{mgr2, mgr2, mgr2}, ids)

	// Shutdown the second TSO server.
	mgr2.Close()
	re.NoError(suite.deregisterTSOServer(svcAddr2))
	// The primaries should move back to the first TSO server.
	waitForPrimariesServing(re, []*KeyspaceGroupManager{mgr1, mgr1, mgr1}, ids)

	// Restart the Second TSO server.
	err = suite.registerTSOServer(re, svcAddr2, cfg2)
	re.NoError(err)
	defer func() {
		re.NoError(suite.deregisterTSOServer(svcAddr2))
	}()
	mgr2 = suite.newKeyspaceGroupManager(1, clusterID, cfg2)
	re.NotNil(mgr2)
	defer mgr2.Close()
	err = mgr2.Initialize()
	re.NoError(err)
	// All primaries should eventually move to the second TSO server because of the higher priority.
	waitForPrimariesServing(re, []*KeyspaceGroupManager{mgr2, mgr2, mgr2}, ids)

	mgrs := []*KeyspaceGroupManager{mgr2, mgr2, mgr2}
	for i, id := range ids {
		// Set the keyspace group replica on the first TSO server to have higher priority.
		addKeyspaceGroupAssignment(
			suite.ctx, suite.etcdClient, id, rootPath,
			[]string{svcAddr1, svcAddr2}, []int{defaultPriority - 1, defaultPriority - 2}, []uint32{id})
		// The primary of this keyspace group should move back to the first TSO server.
		mgrs[i] = mgr1
		waitForPrimariesServing(re, mgrs, ids)
	}

	cancel()
	wg.Wait()
}

// Register TSO server.
func (suite *keyspaceGroupManagerTestSuite) registerTSOServer(
	re *require.Assertions, svcAddr string, cfg *TestServiceConfig,
) error {
	serviceID := &discovery.ServiceRegistryEntry{ServiceAddr: cfg.GetAdvertiseListenAddr(), Name: cfg.Name}
	serializedEntry, err := serviceID.Serialize()
	re.NoError(err)
	serviceKey := keypath.RegistryPath(constant.TSOServiceName, svcAddr)
	_, err = suite.etcdClient.Put(suite.ctx, serviceKey, serializedEntry)
	return err
}

// Deregister TSO server.
func (suite *keyspaceGroupManagerTestSuite) deregisterTSOServer(svcAddr string) error {
	serviceKey := keypath.RegistryPath(constant.TSOServiceName, svcAddr)
	if _, err := suite.etcdClient.Delete(suite.ctx, serviceKey); err != nil {
		return err
	}
	return nil
}

func checkTSO(
	ctx context.Context, re *require.Assertions, wg *sync.WaitGroup,
	mgr *KeyspaceGroupManager, ids []uint32,
) {
	wg.Add(len(ids))
	for _, id := range ids {
		go func(id uint32) {
			defer wg.Done()
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				respTS, respGroupID, err := mgr.HandleTSORequest(ctx, id, id, 1)
				// omit the error check since there are many kinds of errors during primaries movement
				if err != nil {
					continue
				}
				re.Equal(id, respGroupID)
				ts = tsoutil.ComposeTS(respTS.Physical, respTS.Logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}(id)
	}
}

func waitForPrimariesServing(
	re *require.Assertions, mgrs []*KeyspaceGroupManager, ids []uint32,
) {
	testutil.Eventually(re, func() bool {
		for j, id := range ids {
			if member, err := mgrs[j].GetElectionMember(id, id); err != nil || member == nil || !member.IsLeader() {
				return false
			}
			if _, _, err := mgrs[j].HandleTSORequest(mgrs[j].ctx, id, id, 1); err != nil {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(50*time.Millisecond))
}
