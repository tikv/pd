// Copyright 2016 TiKV Project Authors.
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

package member

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const (
	// The timeout to wait transfer etcd leader to complete.
	moveLeaderTimeout = 5 * time.Second
	// If the campaign times is more than this value in `campaignTimesRecordTimeout`, the PD will resign and campaign again.
	campaignLeaderFrequencyTimes = 3

	checkFailBackoffDuration = 200 * time.Millisecond
)

// Member is used for the election related logic. It implements Member interface.
type Member struct {
	leadership *election.Leadership
	leader     atomic.Value // stored as *pdpb.Member
	// etcd and cluster information.
	etcd   *embed.Etcd
	client *clientv3.Client
	id     uint64       // etcd server id.
	member *pdpb.Member // current PD's info.
	// memberValue is the serialized string of `member`. It will be saved in
	// etcd leader key when the PD node is successfully elected as the PD leader
	// of the cluster. Every write will use it to check PD leadership.
	memberValue string
	// lastLeaderUpdatedTime is the last time when the leader is updated.
	lastLeaderUpdatedTime atomic.Value
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *Member {
	return &Member{
		etcd:   etcd,
		client: client,
		id:     id,
	}
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (m *Member) ID() uint64 {
	return m.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (m *Member) Name() string {
	return m.member.Name
}

// GetMember returns the member.
func (m *Member) GetMember() any {
	return m.member
}

// MemberValue returns the member value.
func (m *Member) MemberValue() string {
	return m.memberValue
}

// MemberString returns the member string.
func (m *Member) MemberString() string {
	if m.member == nil {
		return ""
	}
	return m.member.String()
}

// Member returns the member.
func (m *Member) Member() *pdpb.Member {
	return m.member
}

// Etcd returns etcd related information.
func (m *Member) Etcd() *embed.Etcd {
	return m.etcd
}

// Client returns the etcd client.
func (m *Member) Client() *clientv3.Client {
	return m.client
}

// IsServing returns whether the server is PD leader or not by checking its leadership's lease and leader info.
func (m *Member) IsServing() bool {
	return m.leadership.Check() && m.GetLeader().GetMemberId() == m.member.GetMemberId()
}

// IsLeaderElected returns true if the leader exists; otherwise false
func (m *Member) IsLeaderElected() bool {
	return m.GetLeader() != nil
}

// GetServingUrls returns current leader's listen urls
func (m *Member) GetServingUrls() []string {
	return m.GetLeader().GetClientUrls()
}

// GetLeader returns current PD leader of PD cluster.
func (m *Member) GetLeader() *pdpb.Member {
	leader := m.leader.Load()
	if leader == nil {
		return nil
	}
	member := leader.(*pdpb.Member)
	if member.GetMemberId() == 0 {
		return nil
	}
	return member
}

// setLeader sets the member's PD leader.
func (m *Member) setLeader(member *pdpb.Member) {
	m.leader.Store(member)
	m.lastLeaderUpdatedTime.Store(time.Now())
}

// unsetLeader unsets the member's PD leader.
func (m *Member) unsetLeader() {
	m.leader.Store(&pdpb.Member{})
	m.lastLeaderUpdatedTime.Store(time.Now())
}

// PromoteSelf sets the member itself to a PD leader.
func (m *Member) PromoteSelf() {
	m.setLeader(m.member)
}

// GetElectionPath returns the path of the PD leader.
func (*Member) GetElectionPath() string {
	return keypath.ElectionPath(nil)
}

// GetLeadership returns the leadership of the PD member.
func (m *Member) GetLeadership() *election.Leadership {
	return m.leadership
}

// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
func (m *Member) GetLastLeaderUpdatedTime() time.Time {
	lastLeaderUpdatedTime := m.lastLeaderUpdatedTime.Load()
	if lastLeaderUpdatedTime == nil {
		return time.Time{}
	}
	return lastLeaderUpdatedTime.(time.Time)
}

// Campaign is used to campaign a PD member's leadership
// and make it become a PD leader.
// leader should be changed when campaign leader frequently.
func (m *Member) Campaign(ctx context.Context, leaseTimeout int64) error {
	m.leadership.AddCampaignTimes()
	failpoint.Inject("skipCampaignLeaderCheck", func() {
		failpoint.Return(m.leadership.Campaign(leaseTimeout, m.MemberValue()))
	})

	if m.leadership.GetCampaignTimesNum() > campaignLeaderFrequencyTimes {
		if err := m.ResignEtcdLeader(ctx, m.Name(), ""); err != nil {
			return err
		}
		m.leadership.ResetCampaignTimes()
		return errs.ErrLeaderFrequentlyChange.FastGenByArgs(m.Name(), m.GetElectionPath())
	}

	return m.leadership.Campaign(leaseTimeout, m.MemberValue())
}

// preCheckLeader does some pre-check before checking whether it's the leader.
func (m *Member) preCheckLeader() error {
	if m.GetEtcdLeader() == 0 {
		return errs.ErrEtcdLeaderNotFound
	}
	return nil
}

// getPersistentLeader gets the corresponding leader from etcd by given leaderPath (as the key).
func (m *Member) getPersistentLeader() (*pdpb.Member, int64, error) {
	leader := &pdpb.Member{}
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(m.client, m.GetElectionPath(), leader)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return leader, rev, nil
}

// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
// otherwise returns a bool which indicates if it is needed to check later.
func (m *Member) CheckLeader() (*Leader, bool) {
	if err := m.preCheckLeader(); err != nil {
		log.Warn("failed to pass pre-check, check pd leader later", errs.ZapError(err))
		time.Sleep(checkFailBackoffDuration)
		return nil, true
	}

	leader, revision, err := m.getPersistentLeader()
	if err != nil {
		log.Warn("getting pd leader meets error", errs.ZapError(err))
		time.Sleep(checkFailBackoffDuration)
		return nil, true
	}
	if leader == nil {
		// no leader yet
		return nil, false
	}

	if m.isSameLeader(leader) {
		// oh, we are already a PD leader, which indicates we may meet something wrong
		// in previous Campaign. We should delete the leadership and campaign again.
		log.Warn("the pd leader has not changed, delete and campaign again", zap.Stringer("old-pd-leader", leader))
		// Delete the leader itself and let others start a new election again.
		if err = m.leadership.DeleteLeaderKey(); err != nil {
			log.Error("deleting pd leader key meets error", errs.ZapError(err))
			time.Sleep(checkFailBackoffDuration)
			return nil, true
		}
		// Return nil and false to make sure the campaign will start immediately.
		return nil, false
	}

	return &Leader{
		wrapper:  m,
		member:   leader,
		revision: revision,
	}, false
}

// WatchLeader is used to watch the changes of the leader.
func (m *Member) WatchLeader(ctx context.Context, leader *pdpb.Member, revision int64) {
	m.setLeader(leader)
	m.leadership.Watch(ctx, revision)
	m.unsetLeader()
}

// Resign is used to reset the PD member's current leadership.
// Basically it will reset the leader lease and unset leader info.
func (m *Member) Resign() {
	m.leadership.Reset()
	m.unsetLeader()
}

// CheckPriority checks whether the etcd leader should be moved according to the priority.
func (m *Member) CheckPriority(ctx context.Context) {
	etcdLeader := m.GetEtcdLeader()
	if etcdLeader == m.ID() || etcdLeader == 0 {
		return
	}
	myPriority, err := m.GetMemberLeaderPriority(m.ID())
	if err != nil {
		log.Error("failed to load leader priority", errs.ZapError(err))
		return
	}
	leaderPriority, err := m.GetMemberLeaderPriority(etcdLeader)
	if err != nil {
		log.Error("failed to load etcd leader priority", errs.ZapError(err))
		return
	}
	if myPriority > leaderPriority {
		err := m.MoveEtcdLeader(ctx, etcdLeader, m.ID())
		if err != nil {
			log.Error("failed to transfer etcd leader", errs.ZapError(err))
		} else {
			log.Info("transfer etcd leader",
				zap.Uint64("from", etcdLeader),
				zap.Uint64("to", m.ID()))
		}
	}
}

// MoveEtcdLeader tries to transfer etcd leader.
func (m *Member) MoveEtcdLeader(ctx context.Context, old, new uint64) error {
	moveCtx, cancel := context.WithTimeout(ctx, moveLeaderTimeout)
	defer cancel()
	err := m.etcd.Server.MoveLeader(moveCtx, old, new)
	if err != nil {
		return errs.ErrEtcdMoveLeader.Wrap(err).GenWithStackByCause()
	}
	return nil
}

// GetEtcdLeader returns the etcd leader ID.
func (m *Member) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}

// isSameLeader checks whether a server is the leader itself.
func (m *Member) isSameLeader(leader any) bool {
	return leader.(*pdpb.Member).GetMemberId() == m.ID()
}

// InitMemberInfo initializes the member info.
func (m *Member) InitMemberInfo(advertiseClientUrls, advertisePeerUrls, name string) {
	member := &pdpb.Member{
		Name:       name,
		MemberId:   m.ID(),
		ClientUrls: strings.Split(advertiseClientUrls, ","),
		PeerUrls:   strings.Split(advertisePeerUrls, ","),
	}

	data, err := member.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal pd member meet error", zap.Stringer("pd-member", member), errs.ZapError(errs.ErrMarshalMember, err))
	}
	m.member = member
	m.memberValue = string(data)
	m.leadership = election.NewLeadership(m.client, m.GetElectionPath(), "leader election")
	log.Info("member joining election", zap.Stringer("member-info", m.member))
}

// ResignEtcdLeader resigns current PD's etcd leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (m *Member) ResignEtcdLeader(ctx context.Context, from string, nextEtcdLeader string) error {
	log.Info("try to resign etcd leader to next pd-server", zap.String("from", from), zap.String("to", nextEtcdLeader))
	// Determine next etcd leader candidates.
	var etcdLeaderIDs []uint64
	res, err := etcdutil.ListEtcdMembers(ctx, m.client)
	if err != nil {
		return err
	}

	// Do nothing when I am the only member of cluster.
	if len(res.Members) == 1 && res.Members[0].ID == m.id && nextEtcdLeader == "" {
		return nil
	}

	for _, member := range res.Members {
		if (nextEtcdLeader == "" && member.ID != m.id) || (nextEtcdLeader != "" && member.Name == nextEtcdLeader) {
			etcdLeaderIDs = append(etcdLeaderIDs, member.GetID())
		}
	}
	if len(etcdLeaderIDs) == 0 {
		return errors.New("no valid pd to transfer etcd leader")
	}
	nextEtcdLeaderID := etcdLeaderIDs[rand.IntN(len(etcdLeaderIDs))]
	return m.MoveEtcdLeader(ctx, m.ID(), nextEtcdLeaderID)
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (m *Member) SetMemberLeaderPriority(id uint64, priority int) error {
	key := keypath.MemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("save etcd leader priority failed, maybe not pd leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's etcd leader priority config.
func (m *Member) DeleteMemberLeaderPriority(id uint64) error {
	key := keypath.MemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("delete etcd leader priority failed, maybe not pd leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (m *Member) GetMemberLeaderPriority(id uint64) (int, error) {
	key := keypath.MemberLeaderPriorityPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return 0, err
	}
	if len(res.Kvs) == 0 {
		return 0, nil
	}
	priority, err := strconv.ParseInt(string(res.Kvs[0].Value), 10, 32)
	if err != nil {
		return 0, errs.ErrStrconvParseInt.Wrap(err).GenWithStackByCause()
	}
	return int(priority), nil
}

// GetMemberDeployPath loads a member's binary deploy path.
func (m *Member) GetMemberDeployPath(id uint64) (string, error) {
	key := keypath.MemberBinaryDeployPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// SetMemberDeployPath saves a member's binary deploy path.
func (m *Member) SetMemberDeployPath(id uint64) error {
	key := keypath.MemberBinaryDeployPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	execPath, err := os.Executable()
	deployPath := filepath.Dir(execPath)
	if err != nil {
		return errors.WithStack(err)
	}
	res, err := txn.Then(clientv3.OpPut(key, deployPath)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save deploy path")
	}
	return nil
}

// GetMemberBinaryVersion loads a member's binary version.
func (m *Member) GetMemberBinaryVersion(id uint64) (string, error) {
	key := keypath.MemberBinaryVersionPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// GetMemberGitHash loads a member's git hash.
func (m *Member) GetMemberGitHash(id uint64) (string, error) {
	key := keypath.MemberGitHashPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// SetMemberBinaryVersion saves a member's binary version.
func (m *Member) SetMemberBinaryVersion(id uint64, releaseVersion string) error {
	key := keypath.MemberBinaryVersionPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	res, err := txn.Then(clientv3.OpPut(key, releaseVersion)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save binary version")
	}
	return nil
}

// SetMemberGitHash saves a member's git hash.
func (m *Member) SetMemberGitHash(id uint64, gitHash string) error {
	key := keypath.MemberGitHashPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	res, err := txn.Then(clientv3.OpPut(key, gitHash)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save git hash")
	}
	return nil
}

// Close gracefully shuts down all servers/listeners.
func (m *Member) Close() {
	m.Etcd().Close()
}

// Leader is the leader in the election group backed by the embedded etcd.
type Leader struct {
	wrapper  *Member
	member   *pdpb.Member
	revision int64
}

// GetListenUrls returns current leader's client urls
func (l *Leader) GetListenUrls() []string {
	return l.member.GetClientUrls()
}

// GetRevision the revision of the leader in etcd
func (l *Leader) GetRevision() int64 {
	return l.revision
}

// String declares fmt.Stringer
func (l *Leader) String() string {
	return l.member.String()
}

// Watch on the leader
func (l *Leader) Watch(ctx context.Context) {
	l.wrapper.WatchLeader(ctx, l.member, l.revision)
}
