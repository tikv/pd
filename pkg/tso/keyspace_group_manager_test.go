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

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type keyspaceGroupManagerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
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
	suite.backendEndpoints, suite.etcdClient, suite.clean = startEmbeddedEtcd(t)

	suite.cfg = &TestServiceConfig{
		Name:                      "tso-test-name",
		BackendEndpoints:          suite.backendEndpoints,
		ListenAddr:                "http://127.0.0.1:3379",
		AdvertiseListenAddr:       "http://127.0.0.1:3379",
		LeaderLease:               mcsutils.DefaultLeaderLease,
		LocalTSOEnabled:           false,
		TSOUpdatePhysicalInterval: 50 * time.Millisecond,
		TSOSaveInterval:           time.Duration(mcsutils.DefaultLeaderLease) * time.Second,
		MaxResetTSGap:             time.Hour * 24,
		TLSConfig:                 nil,
	}
}

func (suite *keyspaceGroupManagerTestSuite) TearDownSuite() {
	suite.clean()
	suite.cancel()
}

// TestNewKeyspaceGroupManager tests the initialization of KeyspaceGroupManager.
// It should initialize the allocator manager with the desired configurations and parameters.
func (suite *keyspaceGroupManagerTestSuite) TestNewKeyspaceGroupManager() {
	re := suite.Require()

	tsoServiceID := &discovery.ServiceRegistryEntry{ServiceAddr: suite.cfg.AdvertiseListenAddr}
	guid := uuid.New().String()
	legacySvcRootPath := path.Join("/pd", guid)
	tsoSvcRootPath := path.Join("/ms", guid, "tso")
	electionNamePrefix := "tso-server-" + guid

	ksgMgr := NewKeyspaceGroupManager(
		suite.ctx, tsoServiceID, suite.etcdClient, electionNamePrefix, legacySvcRootPath, tsoSvcRootPath, suite.cfg)
	err := ksgMgr.Initialize(false)
	re.NoError(err)

	re.Equal(tsoServiceID, ksgMgr.tsoServiceID)
	re.Equal(suite.etcdClient, ksgMgr.etcdClient)
	re.Equal(electionNamePrefix, ksgMgr.electionNamePrefix)
	re.Equal(legacySvcRootPath, ksgMgr.legacySvcRootPath)
	re.Equal(tsoSvcRootPath, ksgMgr.tsoSvcRootPath)
	re.Equal(suite.cfg, ksgMgr.cfg)
	re.Equal(defaultLoadKeyspaceGroupsBatchSize, ksgMgr.loadKeyspaceGroupsBatchSize)
	re.Equal(defaultLoadKeyspaceGroupsTimeout, ksgMgr.loadKeyspaceGroupsTimeout)

	am, err := ksgMgr.GetAllocatorManager(mcsutils.DefaultKeySpaceGroupID)
	re.NoError(err)
	re.False(am.enableLocalTSO)
	re.Equal(mcsutils.DefaultKeySpaceGroupID, am.ksgID)
	re.Equal(mcsutils.DefaultLeaderLease, am.leaderLease)
	re.Equal(time.Hour*24, am.maxResetTSGap())
	re.Equal(legacySvcRootPath, am.rootPath)
	re.Equal(time.Duration(mcsutils.DefaultLeaderLease)*time.Second, am.saveInterval)
	re.Equal(time.Duration(50)*time.Millisecond, am.updatePhysicalInterval)

	ksgMgr.Close()
}

// TestLoadKeyspaceGroupsAssignment tests the loading of the keyspace group assignment.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsAssignment() {
	re := suite.Require()
	maxCountInUse := int(mcsutils.MaxKeyspaceGroupCountInUse)
	// Test loading of empty keyspace group assignment.
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, 0, 0, 100)
	// Test loading of single keyspace group assignment.
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, 1, 0, 100)
	// Test loading of multiple keyspace group assignment.
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, 3, 0, 100)
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, maxCountInUse-1, 0, 10)
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, maxCountInUse, 0, 10)
	// Test loading of the keyspace group assignment which exceeds the maximum keyspace group count.
	// In this case, the manager should only load/serve the first MaxKeyspaceGroupCountInUse keyspace
	// groups and ignore the rest.
	runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg, maxCountInUse+1, 0, 10)
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
		runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg,
			param.count-1, param.batchSize, param.probabilityAssignToMe)
		runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg,
			param.count, param.batchSize, param.probabilityAssignToMe)
		runTestLoadKeyspaceGroupsAssignment(suite.ctx, re, suite.etcdClient, suite.cfg,
			param.count+1, param.batchSize, param.probabilityAssignToMe)
	}
}

// TestLoadKeyspaceGroupsTimeout tests there is timeout when loading the initial keyspace group assignment
// from etcd. The initialization of the keyspace group manager should fail.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsTimeout() {
	re := suite.Require()

	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 1)
	re.NotNil(mgr)
	defer mgr.Close()

	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, true,
		mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr, uint32(0), []uint32{0})

	// Set the timeout to 1 second and inject the delayLoadKeyspaceGroups to return 3 seconds to let
	// the loading sleep 3 seconds.
	mgr.loadKeyspaceGroupsTimeout = time.Second
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delayLoadKeyspaceGroups", "return(3)"))
	err := mgr.Initialize(true)
	// If loading keyspace groups timeout, the initialization should fail with ErrLoadKeyspaceGroupsTerminated.
	re.Equal(errs.ErrLoadKeyspaceGroupsTerminated, err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delayLoadKeyspaceGroups"))
}

// TestLoadKeyspaceGroupsSucceedWithTempFailures tests the initialization should succeed when there are temporary
// failures during loading the initial keyspace group assignment from etcd.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsSucceedWithTempFailures() {
	re := suite.Require()

	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 1)
	re.NotNil(mgr)
	defer mgr.Close()

	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, true,
		mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr, uint32(0), []uint32{0})

	// Set the max retry times to 3 and inject the loadKeyspaceGroupsTemporaryFail to return 2 to let
	// loading from etcd fail 2 times but the whole initialization still succeeds.
	mgr.loadFromEtcdMaxRetryTimes = 3
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/loadKeyspaceGroupsTemporaryFail", "return(2)"))
	err := mgr.Initialize(true)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/loadKeyspaceGroupsTemporaryFail"))
}

// TestLoadKeyspaceGroupsFailed tests the initialization should fail when there are too many failures
// during loading the initial keyspace group assignment from etcd.
func (suite *keyspaceGroupManagerTestSuite) TestLoadKeyspaceGroupsFailed() {
	re := suite.Require()

	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 1)
	re.NotNil(mgr)
	defer mgr.Close()

	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, true,
		mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr, uint32(0), []uint32{0})

	// Set the max retry times to 3 and inject the loadKeyspaceGroupsTemporaryFail to return 3 to let
	// loading from etcd fail 3 times which should cause the whole initialization to fail.
	mgr.loadFromEtcdMaxRetryTimes = 3
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/loadKeyspaceGroupsTemporaryFail", "return(3)"))
	err := mgr.Initialize(true)
	re.Error(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/loadKeyspaceGroupsTemporaryFail"))
}

// TestWatchAndDynamicallyApplyChanges tests the keyspace group manager watch and dynamically apply
// keyspace groups' membership/distribution meta changes.
func (suite *keyspaceGroupManagerTestSuite) TestWatchAndDynamicallyApplyChanges() {
	re := suite.Require()

	// Start with the empty keyspace group assignment.
	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 0)
	re.NotNil(mgr)
	defer mgr.Close()
	err := mgr.Initialize(true)
	re.NoError(err)

	rootPath := mgr.legacySvcRootPath
	svcAddr := mgr.tsoServiceID.ServiceAddr

	// Initialize PUT/DELETE events
	events := []*etcdEvent{}
	// Assign keyspace group 0 to this host/pod/keyspace-group-manager.
	// final result: [0]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 0, []string{svcAddr})
	// Assign keyspace group 1 to this host/pod/keyspace-group-manager.
	// final result: [0,1]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 1, []string{"unknown", svcAddr})
	// Assign keyspace group 2 to other host/pod/keyspace-group-manager.
	// final result: [0,1]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 2, []string{"unknown"})
	// Assign keyspace group 3 to this host/pod/keyspace-group-manager.
	// final result: [0,1,3]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 3, []string{svcAddr})
	// Delete keyspace group 0
	// final result: [1,3]
	events = generateKeyspaceGroupEvent(events, mvccpb.DELETE, 0, []string{})
	// Put keyspace group 4 which doesn't belong to anyone.
	// final result: [1,3]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 4, []string{})
	// Put keyspace group 5 which doesn't belong to anyone.
	// final result: [1,3]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 5, []string{})
	// Assign keyspace group 2 to this host/pod/keyspace-group-manager.
	// final result: [1,2,3]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 2, []string{svcAddr})
	// Reassign keyspace group 3 to no one.
	// final result: [1,2]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 3, []string{})
	// Reassign keyspace group 4 to this host/pod/keyspace-group-manager.
	// final result: [1,2,4]
	events = generateKeyspaceGroupEvent(events, mvccpb.PUT, 4, []string{svcAddr})

	// Eventually, this keyspace groups manager is expected to serve the following keyspace groups.
	idsExpected := []int{1, 2, 4}

	// Apply the keyspace group assignment change events to etcd.
	for _, event := range events {
		switch event.eventType {
		case mvccpb.PUT:
			err = putKeyspaceGroupToEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg)
			re.NoError(err)
		case mvccpb.DELETE:
			err = deleteKeyspaceGroupInEtcd(suite.ctx, suite.etcdClient, rootPath, event.ksg.ID)
			re.NoError(err)
		}
	}

	// Verify the keyspace group assignment.
	testutil.Eventually(re, func() bool {
		idsAssigned := collectAssignedKeyspaceGroupIDs(re, mgr)
		return reflect.DeepEqual(idsExpected, idsAssigned)
	})
}

// TestGetAMWithMembershipCheck tests GetAMWithMembershipCheck.
func (suite *keyspaceGroupManagerTestSuite) TestGetAMWithMembershipCheck() {
	re := suite.Require()

	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 1)
	re.NotNil(mgr)
	defer mgr.Close()

	var (
		am  *AllocatorManager
		err error
	)

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, true,
		mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr,
		uint32(0), []uint32{0, 1, 2})

	err = mgr.Initialize(true)
	re.NoError(err)

	// Should be able to get AM for keyspace 0, 1, 2 in keyspace group 0.
	am, err = mgr.GetAMWithMembershipCheck(0, 0)
	re.NoError(err)
	re.NotNil(am)
	am, err = mgr.GetAMWithMembershipCheck(1, 0)
	re.NoError(err)
	re.NotNil(am)
	am, err = mgr.GetAMWithMembershipCheck(2, 0)
	re.NoError(err)
	re.NotNil(am)
	// Should fail because keyspace 3 is not in keyspace group 0.
	am, err = mgr.GetAMWithMembershipCheck(3, 0)
	re.Error(err)
	re.Nil(am)
	// Should fail because keyspace group 1 doesn't exist.
	am, err = mgr.GetAMWithMembershipCheck(0, 1)
	re.Error(err)
	re.Nil(am)
}

// TestHandleTSORequestWithWrongMembership tests the case that HandleTSORequest receives
// a tso request with mismatched keyspace and keyspace group.
func (suite *keyspaceGroupManagerTestSuite) TestHandleTSORequestWithWrongMembership() {
	re := suite.Require()

	mgr := newUniqueKeyspaceGroupManager(suite.ctx, suite.etcdClient, suite.cfg, 1)
	re.NotNil(mgr)
	defer mgr.Close()

	// Create keyspace group 0 which contains keyspace 0, 1, 2.
	addKeyspaceGroupAssignment(
		suite.ctx, suite.etcdClient, true,
		mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr,
		uint32(0), []uint32{0, 1, 2})

	err := mgr.Initialize(true)
	re.NoError(err)

	// Should fail because keyspace 0 is not in keyspace group 1 and the API returns
	// the keyspace group 0 to which the keyspace 0 belongs.
	_, keyspaceGroupBelongTo, err := mgr.HandleTSORequest(0, 1, GlobalDCLocation, 1)
	re.Error(err)
	re.Equal(uint32(0), keyspaceGroupBelongTo)
}

type etcdEvent struct {
	eventType mvccpb.Event_EventType
	ksg       *endpoint.KeyspaceGroup
}

func generateKeyspaceGroupEvent(
	events []*etcdEvent, eventType mvccpb.Event_EventType, id uint32, addrs []string,
) []*etcdEvent {
	members := []endpoint.KeyspaceGroupMember{}
	for _, addr := range addrs {
		members = append(members, endpoint.KeyspaceGroupMember{Address: addr})
	}

	return append(events,
		&etcdEvent{
			eventType: eventType,
			ksg: &endpoint.KeyspaceGroup{
				ID:        id,
				Members:   members,
				Keyspaces: []uint32{id},
			},
		},
	)
}

// runTestLoadMultipleKeyspaceGroupsAssignment tests the loading of multiple keyspace group assignment.
func runTestLoadKeyspaceGroupsAssignment(
	ctx context.Context,
	re *require.Assertions,
	etcdClient *clientv3.Client,
	cfg *TestServiceConfig,
	numberOfKeypaceGroupsToAdd int,
	loadKeyspaceGroupsBatchSize int64, // set to 0 to use the default value
	probabilityAssignToMe int, // percentage of assigning keyspace groups to this host/pod
) {
	idsExpected := []int{}
	mgr := newUniqueKeyspaceGroupManager(ctx, etcdClient, cfg, loadKeyspaceGroupsBatchSize)
	re.NotNil(mgr)
	defer mgr.Close()

	step := 30
	mux := sync.Mutex{}
	wg := sync.WaitGroup{}
	for i := 0; i < numberOfKeypaceGroupsToAdd; i += step {
		wg.Add(1)
		go func(startID int) {
			defer wg.Done()

			endID := startID + step
			if endID > numberOfKeypaceGroupsToAdd {
				endID = numberOfKeypaceGroupsToAdd
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
					idsExpected = append(idsExpected, j)
					mux.Unlock()
				}
				addKeyspaceGroupAssignment(
					ctx, etcdClient, assignToMe,
					mgr.legacySvcRootPath, mgr.tsoServiceID.ServiceAddr, uint32(j), []uint32{uint32(j)})
			}
		}(i)
	}
	wg.Wait()

	err := mgr.Initialize(true)
	re.NoError(err)

	// Verify the keyspace group assignment.
	sort.Ints(idsExpected)
	idsAssigned := collectAssignedKeyspaceGroupIDs(re, mgr)
	re.Equal(idsExpected, idsAssigned)
}

func newUniqueKeyspaceGroupManager(
	ctx context.Context,
	etcdClient *clientv3.Client,
	cfg *TestServiceConfig,
	loadKeyspaceGroupsBatchSize int64, // set to 0 to use the default value
) *KeyspaceGroupManager {
	tsoServiceID := &discovery.ServiceRegistryEntry{ServiceAddr: cfg.AdvertiseListenAddr}
	uniqueID := memberutil.GenerateUniqueID(uuid.New().String())
	uniqueStr := strconv.FormatUint(uniqueID, 10)
	legacySvcRootPath := path.Join("/pd", uniqueStr)
	tsoSvcRootPath := path.Join("/ms", uniqueStr, "tso")
	electionNamePrefix := "kgm-test-" + uniqueStr

	keyspaceGroupManager := NewKeyspaceGroupManager(
		ctx, tsoServiceID, etcdClient, electionNamePrefix, legacySvcRootPath, tsoSvcRootPath, cfg)

	if loadKeyspaceGroupsBatchSize != 0 {
		keyspaceGroupManager.loadKeyspaceGroupsBatchSize = loadKeyspaceGroupsBatchSize
	}
	return keyspaceGroupManager
}

// putKeyspaceGroupToEtcd puts a keyspace group to etcd.
func putKeyspaceGroupToEtcd(
	ctx context.Context, etcdClient *clientv3.Client,
	rootPath string, group *endpoint.KeyspaceGroup,
) error {
	key := strings.Join([]string{rootPath, endpoint.KeyspaceGroupIDPath(group.ID)}, "/")
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
	key := strings.Join([]string{rootPath, endpoint.KeyspaceGroupIDPath(id)}, "/")

	if _, err := etcdClient.Delete(ctx, key); err != nil {
		return err
	}

	return nil
}

// addKeyspaceGroupAssignment adds a keyspace group assignment to etcd.
func addKeyspaceGroupAssignment(
	ctx context.Context, etcdClient *clientv3.Client,
	assignToMe bool, rootPath, svcAddr string,
	groupID uint32, keyspaces []uint32,
) error {
	var location string
	if assignToMe {
		location = svcAddr
	} else {
		location = uuid.NewString()
	}
	group := &endpoint.KeyspaceGroup{
		ID:        groupID,
		Members:   []endpoint.KeyspaceGroupMember{{Address: location}},
		Keyspaces: keyspaces,
	}

	key := strings.Join([]string{rootPath, endpoint.KeyspaceGroupIDPath(groupID)}, "/")
	value, err := json.Marshal(group)
	if err != nil {
		return err
	}

	if _, err := etcdClient.Put(ctx, key, string(value)); err != nil {
		return err
	}

	return nil
}

func collectAssignedKeyspaceGroupIDs(re *require.Assertions, ksgMgr *KeyspaceGroupManager) []int {
	ids := []int{}
	for i := 0; i < len(ksgMgr.kgs); i++ {
		ksg := ksgMgr.kgs[i].Load()
		if ksg == nil {
			re.Nil(ksgMgr.ams[i].Load(), fmt.Sprintf("ksg is nil but am is not nil for id %d", i))
		} else {
			am := ksgMgr.ams[i].Load()
			re.NotNil(am, fmt.Sprintf("ksg is not nil but am is nil for id %d", i))
			re.Equal(i, int(am.ksgID))
			re.Equal(i, int(ksg.ID))
			for _, m := range ksg.Members {
				if m.Address == ksgMgr.tsoServiceID.ServiceAddr {
					ids = append(ids, i)
					break
				}
			}
		}
	}

	return ids
}

func (suite *keyspaceGroupManagerTestSuite) TestUpdateKeyspaceGroupMembership() {
	re := suite.Require()

	var keyspaceLookupTable map[uint32]struct{}

	// Start from an empty keyspace group.
	oldKeyspaces := []uint32{}
	newKeyspaces := []uint32{}
	defaultKeyspaceLookupTable := map[uint32]struct{}{}
	kgm := &KeyspaceGroupManager{}

	keyspaceLookupTable = kgm.updateKeyspaceGroupMembership(0, oldKeyspaces, newKeyspaces, defaultKeyspaceLookupTable)
	verifyLocalKeyspaceLookupTable(re, keyspaceLookupTable, newKeyspaces)
	verifyGlobalKeyspaceLookupTable(re, kgm, keyspaceLookupTable)

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
		oldKeyspaces = newKeyspaces
		newKeyspaces = keyspaces
		defaultKeyspaceLookupTable = keyspaceLookupTable
		keyspaceLookupTable = kgm.updateKeyspaceGroupMembership(0, oldKeyspaces, newKeyspaces, defaultKeyspaceLookupTable)
		verifyLocalKeyspaceLookupTable(re, keyspaceLookupTable, newKeyspaces)
		verifyGlobalKeyspaceLookupTable(re, kgm, keyspaceLookupTable)
	}
}

func verifyLocalKeyspaceLookupTable(re *require.Assertions, keyspaceLookupTable map[uint32]struct{}, newKeyspaces []uint32) {
	re.Equal(len(newKeyspaces), len(keyspaceLookupTable), fmt.Sprintf("%v %v", newKeyspaces, keyspaceLookupTable))
	for _, keyspace := range newKeyspaces {
		_, ok := keyspaceLookupTable[keyspace]
		re.True(ok)
	}
}

func verifyGlobalKeyspaceLookupTable(re *require.Assertions, kgm *KeyspaceGroupManager, keyspaceLookupTable map[uint32]struct{}) {
	kgm.keyspaceLookupTable.Range(func(key, value interface{}) bool {
		_, ok := keyspaceLookupTable[key.(uint32)]
		re.True(ok)
		re.Equal(uint32(0), value.(uint32))
		return true
	})
}
