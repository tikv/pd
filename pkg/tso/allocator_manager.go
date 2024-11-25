// Copyright 2020 TiKV Project Authors.
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
	"fmt"
	"math"
	"path"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	GlobalDCLocation            = "global"
	checkStep                   = time.Minute
	patrolStep                  = time.Second
	defaultAllocatorLeaderLease = 3
	localTSOAllocatorEtcdPrefix = "lta"
	localTSOSuffixEtcdPrefix    = "lts"
)

var (
	// PriorityCheck exported is only for test.
	PriorityCheck = time.Minute
)

// AllocatorGroupFilter is used to select AllocatorGroup.
type AllocatorGroupFilter func(ag *allocatorGroup) bool

type allocatorGroup struct {
	dcLocation string
	// ctx is built with cancel from a parent context when set up which can be different
	// in order to receive Done() signal correctly.
	// cancel would be call when allocatorGroup is deleted to stop background loop.
	ctx    context.Context
	cancel context.CancelFunc
	// For the Global TSO Allocator, leadership is a PD leader's
	// leadership, and for the Local TSO Allocator, leadership
	// is a DC-level certificate to allow an allocator to generate
	// TSO for local transactions in its DC.
	leadership *election.Leadership
	allocator  Allocator
}

// DCLocationInfo is used to record some dc-location related info,
// such as suffix sign and server IDs in this dc-location.
type DCLocationInfo struct {
	// dc-location/global (string) -> Member IDs
	ServerIDs []uint64
	// dc-location (string) -> Suffix sign. It is collected and maintained by the PD leader.
	Suffix int32
}

func (info *DCLocationInfo) clone() DCLocationInfo {
	copiedInfo := DCLocationInfo{
		Suffix: info.Suffix,
	}
	// Make a deep copy here for the slice
	copiedInfo.ServerIDs = make([]uint64, len(info.ServerIDs))
	copy(copiedInfo.ServerIDs, info.ServerIDs)
	return copiedInfo
}

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// IsLeaderElected returns true if the leader exists; otherwise false.
	IsLeaderElected() bool
	// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
	// otherwise returns a bool which indicates if it is needed to check later.
	CheckLeader() (leader member.ElectionLeader, checkAgain bool)
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// KeepLeader is used to keep the leader's leadership.
	KeepLeader(ctx context.Context)
	// CampaignLeader is used to campaign the leadership and make it become a leader in an election group.
	CampaignLeader(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetLeaderID returns current leader's member ID.
	GetLeaderID() uint64
	// GetLeaderPath returns the path of the leader.
	GetLeaderPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
	// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
	GetLastLeaderUpdatedTime() time.Time
	// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
	GetDCLocationPathPrefix() string
	// GetDCLocationPath returns the dc-location path of a member with the given member ID.
	GetDCLocationPath(id uint64) string
	// PreCheckLeader does some pre-check before checking whether it's the leader.
	PreCheckLeader() error
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	mu struct {
		syncutil.RWMutex
		// There are two kinds of TSO Allocators:
		//   1. Global TSO Allocator, as a global single point to allocate
		//      TSO for global transactions, such as cross-region cases.
		//   2. Local TSO Allocator, servers for DC-level transactions.
		// dc-location/global (string) -> TSO Allocator
		allocatorGroups    map[string]*allocatorGroup
		clusterDCLocations map[string]*DCLocationInfo
		// The max suffix sign we have so far, it will be used to calculate
		// the number of suffix bits we need in the TSO logical part.
		maxSuffix int32
	}
	// for the synchronization purpose of the allocator update checks
	wg sync.WaitGroup
	// for the synchronization purpose of the service loops
	svcLoopWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	// kgID is the keyspace group ID
	kgID uint32
	// member is for election use
	member ElectionMember
	// TSO config
	rootPath               string
	storage                endpoint.TSOStorage
	enableLocalTSO         bool
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	// leaderLease defines the time within which a TSO primary/leader must update its TTL
	// in etcd, otherwise etcd will expire the leader key and other servers can campaign
	// the primary/leader again. Etcd only supports seconds TTL, so here is second too.
	leaderLease    int64
	maxResetTSGap  func() time.Duration
	securityConfig *grpcutil.TLSConfig
	// for gRPC use
	localAllocatorConn struct {
		syncutil.RWMutex
		clientConns map[string]*grpc.ClientConn
	}
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(
	ctx context.Context,
	keyspaceGroupID uint32,
	member ElectionMember,
	rootPath string,
	storage endpoint.TSOStorage,
	cfg Config,
) *AllocatorManager {
	ctx, cancel := context.WithCancel(ctx)
	am := &AllocatorManager{
		ctx:                    ctx,
		cancel:                 cancel,
		kgID:                   keyspaceGroupID,
		member:                 member,
		rootPath:               rootPath,
		storage:                storage,
		enableLocalTSO:         cfg.IsLocalTSOEnabled(),
		saveInterval:           cfg.GetTSOSaveInterval(),
		updatePhysicalInterval: cfg.GetTSOUpdatePhysicalInterval(),
		leaderLease:            cfg.GetLeaderLease(),
		maxResetTSGap:          cfg.GetMaxResetTSGap,
		securityConfig:         cfg.GetTLSConfig(),
	}
	am.mu.allocatorGroups = make(map[string]*allocatorGroup)
	am.mu.clusterDCLocations = make(map[string]*DCLocationInfo)
	am.localAllocatorConn.clientConns = make(map[string]*grpc.ClientConn)

	// Set up the Global TSO Allocator here, it will be initialized once the member campaigns leader successfully.
	am.SetUpGlobalAllocator(am.ctx, am.member.GetLeadership())
	am.svcLoopWG.Add(1)
	go am.tsoAllocatorLoop()

	return am
}

// SetUpGlobalAllocator is used to set up the global allocator, which will initialize the allocator and put it into
// an allocator daemon. An TSO Allocator should only be set once, and may be initialized and reset multiple times
// depending on the election.
func (am *AllocatorManager) SetUpGlobalAllocator(ctx context.Context, leadership *election.Leadership) {
	am.mu.Lock()
	defer am.mu.Unlock()

	allocator := NewGlobalTSOAllocator(ctx, am)
	// Create a new allocatorGroup
	ctx, cancel := context.WithCancel(ctx)
	am.mu.allocatorGroups[GlobalDCLocation] = &allocatorGroup{
		dcLocation: GlobalDCLocation,
		ctx:        ctx,
		cancel:     cancel,
		leadership: leadership,
		allocator:  allocator,
	}
}

// getGroupID returns the keyspace group ID of the allocator manager.
func (am *AllocatorManager) getGroupID() uint32 {
	if am == nil {
		return 0
	}
	return am.kgID
}

// getGroupIDStr returns the keyspace group ID of the allocator manager in string format.
func (am *AllocatorManager) getGroupIDStr() string {
	if am == nil {
		return "0"
	}
	return strconv.FormatUint(uint64(am.kgID), 10)
}

// GetTimestampPath returns the timestamp path in etcd for the given DCLocation.
func (am *AllocatorManager) GetTimestampPath(dcLocation string) string {
	if am == nil {
		return ""
	}
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}

	am.mu.RLock()
	defer am.mu.RUnlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		return path.Join(am.rootPath, allocatorGroup.allocator.GetTimestampPath())
	}
	return ""
}

// tsoAllocatorLoop is used to run the TSO Allocator updating daemon.
func (am *AllocatorManager) tsoAllocatorLoop() {
	defer logutil.LogPanic()
	defer am.svcLoopWG.Done()

	am.AllocatorDaemon(am.ctx)
	log.Info("exit allocator loop", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
}

// close is used to shutdown TSO Allocator updating daemon.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (am *AllocatorManager) close() {
	log.Info("closing the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))

	if allocatorGroup, exist := am.getAllocatorGroup(GlobalDCLocation); exist {
		allocatorGroup.allocator.(*GlobalTSOAllocator).close()
	}

	for _, cc := range am.localAllocatorConn.clientConns {
		if err := cc.Close(); err != nil {
			log.Error("failed to close allocator manager grpc clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
	}

	am.cancel()
	am.svcLoopWG.Wait()

	log.Info("closed the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
}

// GetMember returns the ElectionMember of this AllocatorManager.
func (am *AllocatorManager) GetMember() ElectionMember {
	return am.member
}

// GetClusterDCLocationsFromEtcd fetches dcLocation topology from etcd
func (am *AllocatorManager) GetClusterDCLocationsFromEtcd() (clusterDCLocations map[string][]uint64, err error) {
	resp, err := etcdutil.EtcdKVGet(
		am.member.Client(),
		am.member.GetDCLocationPathPrefix(),
		clientv3.WithPrefix())
	if err != nil {
		return clusterDCLocations, err
	}
	clusterDCLocations = make(map[string][]uint64)
	for _, kv := range resp.Kvs {
		// The key will contain the member ID and the value is its dcLocation
		serverPath := strings.Split(string(kv.Key), "/")
		// Get serverID from serverPath, e.g, /pd/dc-location/1232143243253 -> 1232143243253
		serverID, err := strconv.ParseUint(serverPath[len(serverPath)-1], 10, 64)
		dcLocation := string(kv.Value)
		if err != nil {
			log.Warn("get server id and dcLocation from etcd failed, invalid server id",
				logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
				zap.Any("split-serverPath", serverPath),
				zap.String("dc-location", dcLocation),
				errs.ZapError(err))
			continue
		}
		clusterDCLocations[dcLocation] = append(clusterDCLocations[dcLocation], serverID)
	}
	return clusterDCLocations, nil
}

// GetDCLocationInfo returns a copy of DCLocationInfo of the given dc-location,
func (am *AllocatorManager) GetDCLocationInfo(dcLocation string) (DCLocationInfo, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	infoPtr, ok := am.mu.clusterDCLocations[dcLocation]
	if !ok {
		return DCLocationInfo{}, false
	}
	return infoPtr.clone(), true
}

// GetClusterDCLocations returns all dc-locations of a cluster with a copy of map,
// which satisfies dcLocation -> DCLocationInfo.
func (am *AllocatorManager) GetClusterDCLocations() map[string]DCLocationInfo {
	am.mu.RLock()
	defer am.mu.RUnlock()
	dcLocationMap := make(map[string]DCLocationInfo)
	for dcLocation, info := range am.mu.clusterDCLocations {
		dcLocationMap[dcLocation] = info.clone()
	}
	return dcLocationMap
}

// GetClusterDCLocationsNumber returns the number of cluster dc-locations.
func (am *AllocatorManager) GetClusterDCLocationsNumber() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.mu.clusterDCLocations)
}

// GetSuffixBits calculates the bits of suffix sign
// by the max number of suffix so far,
// which will be used in the TSO logical part.
func (am *AllocatorManager) GetSuffixBits() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return CalSuffixBits(am.mu.maxSuffix)
}

// CalSuffixBits calculates the bits of suffix by the max suffix sign.
func CalSuffixBits(maxSuffix int32) int {
	// maxSuffix + 1 because we have the Global TSO holds 0 as the suffix sign
	return int(math.Ceil(math.Log2(float64(maxSuffix + 1))))
}

// AllocatorDaemon is used to update every allocator's TSO and check whether we have
// any new local allocator that needs to be set up.
func (am *AllocatorManager) AllocatorDaemon(ctx context.Context) {
	log.Info("entering into allocator daemon", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))

	tsTicker := time.NewTicker(am.updatePhysicalInterval)
	failpoint.Inject("fastUpdatePhysicalInterval", func() {
		tsTicker.Reset(time.Millisecond)
	})
	defer tsTicker.Stop()

	for {
		select {
		case <-tsTicker.C:
			// Update the initialized TSO Allocator to advance TSO.
			am.allocatorUpdater()
		case <-ctx.Done():
			log.Info("exit allocator daemon", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
			return
		}
	}
}

// Update the Local TSO Allocator leaders TSO in memory concurrently.
func (am *AllocatorManager) allocatorUpdater() {
	// Filter out allocators without leadership and uninitialized
	allocatorGroups := am.getAllocatorGroups(FilterUninitialized(), FilterUnavailableLeadership())
	// Update each allocator concurrently
	for _, ag := range allocatorGroups {
		am.wg.Add(1)
		go am.updateAllocator(ag)
	}
	am.wg.Wait()
}

// updateAllocator is used to update the allocator in the group.
func (am *AllocatorManager) updateAllocator(ag *allocatorGroup) {
	defer logutil.LogPanic()
	defer am.wg.Done()

	select {
	case <-ag.ctx.Done():
		// Resetting the allocator will clear TSO in memory
		ag.allocator.Reset()
		log.Info("exit the allocator update loop", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
		return
	default:
	}
	if !ag.leadership.Check() {
		log.Info("allocator doesn't campaign leadership yet",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", ag.dcLocation))
		time.Sleep(200 * time.Millisecond)
		return
	}
	if err := ag.allocator.UpdateTSO(); err != nil {
		log.Warn("failed to update allocator's timestamp",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", ag.dcLocation),
			zap.String("name", am.member.Name()),
			errs.ZapError(err))
		am.ResetAllocatorGroup(ag.dcLocation, false)
		return
	}
}

// HandleRequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleRequest(ctx context.Context, dcLocation string, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "AllocatorManager.HandleRequest").End()
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}
	allocatorGroup, exist := am.getAllocatorGroup(dcLocation)
	if !exist {
		err := errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found, generate timestamp failed", dcLocation))
		return pdpb.Timestamp{}, err
	}

	return allocatorGroup.allocator.GenerateTSO(ctx, count)
}

// ResetAllocatorGroup will reset the allocator's leadership and TSO initialized in memory.
// It usually should be called before re-triggering an Allocator leader campaign.
func (am *AllocatorManager) ResetAllocatorGroup(dcLocation string, skipResetLeader bool) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
		if !skipResetLeader && allocatorGroup.leadership.Check() {
			allocatorGroup.leadership.Reset()
		}
	}
}

func (am *AllocatorManager) getAllocatorGroups(filters ...AllocatorGroupFilter) []*allocatorGroup {
	am.mu.RLock()
	defer am.mu.RUnlock()
	var allocatorGroups []*allocatorGroup
	for _, ag := range am.mu.allocatorGroups {
		if ag == nil {
			continue
		}
		if slice.NoneOf(filters, func(i int) bool { return filters[i](ag) }) {
			allocatorGroups = append(allocatorGroups, ag)
		}
	}
	return allocatorGroups
}

func (am *AllocatorManager) getAllocatorGroup(dcLocation string) (*allocatorGroup, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]
	return allocatorGroup, exist
}

// GetAllocator get the allocator by dc-location.
func (am *AllocatorManager) GetAllocator(dcLocation string) (Allocator, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}
	allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]
	if !exist {
		return nil, errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found", dcLocation))
	}
	return allocatorGroup.allocator, nil
}

// GetAllocators get all allocators with some filters.
func (am *AllocatorManager) GetAllocators(filters ...AllocatorGroupFilter) []Allocator {
	allocatorGroups := am.getAllocatorGroups(filters...)
	allocators := make([]Allocator, 0, len(allocatorGroups))
	for _, ag := range allocatorGroups {
		allocators = append(allocators, ag.allocator)
	}
	return allocators
}

// GetHoldingLocalAllocatorLeaders returns all Local TSO Allocator leaders this server holds.
func (am *AllocatorManager) GetHoldingLocalAllocatorLeaders() ([]*LocalTSOAllocator, error) {
	localAllocators := am.GetAllocators(
		FilterDCLocation(GlobalDCLocation),
		FilterUnavailableLeadership())
	localAllocatorLeaders := make([]*LocalTSOAllocator, 0, len(localAllocators))
	for _, localAllocator := range localAllocators {
		localAllocatorLeader, ok := localAllocator.(*LocalTSOAllocator)
		if !ok {
			return nil, errs.ErrGetLocalAllocator.FastGenByArgs("invalid local tso allocator found")
		}
		localAllocatorLeaders = append(localAllocatorLeaders, localAllocatorLeader)
	}
	return localAllocatorLeaders, nil
}

// GetLocalAllocatorLeaders returns all Local TSO Allocator leaders' member info.
func (am *AllocatorManager) GetLocalAllocatorLeaders() (map[string]*pdpb.Member, error) {
	localAllocators := am.GetAllocators(FilterDCLocation(GlobalDCLocation))
	localAllocatorLeaderMember := make(map[string]*pdpb.Member)
	for _, allocator := range localAllocators {
		localAllocator, ok := allocator.(*LocalTSOAllocator)
		if !ok {
			return nil, errs.ErrGetLocalAllocator.FastGenByArgs("invalid local tso allocator found")
		}
		localAllocatorLeaderMember[localAllocator.GetDCLocation()] = localAllocator.GetAllocatorLeader()
	}
	return localAllocatorLeaderMember, nil
}

// EnableLocalTSO returns the value of AllocatorManager.enableLocalTSO.
func (am *AllocatorManager) EnableLocalTSO() bool {
	return am.enableLocalTSO
}

// IsLeader returns whether the current member is the leader in the election group.
func (am *AllocatorManager) IsLeader() bool {
	if am == nil || am.member == nil || !am.member.IsLeader() {
		return false
	}
	return true
}

// GetLeaderAddr returns the address of leader in the election group.
func (am *AllocatorManager) GetLeaderAddr() string {
	if am == nil || am.member == nil {
		return ""
	}
	leaderAddrs := am.member.GetLeaderListenUrls()
	if len(leaderAddrs) < 1 {
		return ""
	}
	return leaderAddrs[0]
}

func (am *AllocatorManager) startGlobalAllocatorLoop() {
	globalTSOAllocator, ok := am.mu.allocatorGroups[GlobalDCLocation].allocator.(*GlobalTSOAllocator)
	if !ok {
		// it should never happen
		log.Error("failed to start global allocator loop, global allocator not found")
		return
	}
	globalTSOAllocator.wg.Add(1)
	go globalTSOAllocator.primaryElectionLoop()
}
