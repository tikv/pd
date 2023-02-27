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

package member

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Participant is used for the election related logic. Compared to its counterpart
// EmbeddedEtcdMember, Participant relies on etcd for election, but it's decoupled
// with the embedded etcd. It implements Member interface.
type Participant struct {
	leadership *election.Leadership
	// stored as member type
	leader atomic.Value
	client *clientv3.Client
	// id is unique among all participants
	id uint64
	// member is the current participant's info.
	// TODO: use a more general instead of pdpb.Member. The latter is tightly
	// coupled with the embedded etcd. For now, we can reuse it and ignore the
	// fields which are related to embedded etcd, such as PeerUrls and ClientUrls.
	member     *pdpb.Member
	rootPath   string
	leaderPath string
	// memberValue is the serialized string of `member`. It will be saved in the
	// eader key when this participant is successfully elected as the leader of
	// the group. Every write will use it to check the leadership.
	memberValue string
}

// NewParticipant create a new Participant.
func NewParticipant(client *clientv3.Client, id uint64) *Participant {
	return &Participant{
		client: client,
		id:     id,
	}
}

// InitInfo initializes the member info. The leader key is path.Join(rootPath, leaderName)
func (m *Participant) InitInfo(name string, rootPath string, leaderName string, purpose string) {
	leader := &pdpb.Member{
		Name:     name,
		MemberId: m.ID(),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal leader meet error", zap.Stringer("leader-name", leader), errs.ZapError(errs.ErrMarshalLeader, err))
	}
	m.member = leader
	m.memberValue = string(data)
	m.rootPath = rootPath
	m.leaderPath = path.Join(rootPath, leaderName)
	m.leadership = election.NewLeadership(m.client, m.GetLeaderPath(), purpose)
}

// ID returns the unique ID for this participant in the group. For example, it can be
// unique server id of a cluster or the unique keyspace group replica id of the election
// group comprised of the replicas of a keyspace group.
func (m *Participant) ID() uint64 {
	return m.id
}

// MemberValue returns the member value.
func (m *Participant) MemberValue() string {
	return m.memberValue
}

// Member returns the member.
func (m *Participant) Member() *pdpb.Member {
	return m.member
}

// Client returns the etcd client.
func (m *Participant) Client() *clientv3.Client {
	return m.client
}

// IsLeader returns whether the participant is the leader or not by checking its leadership's
// lease and leader info.
func (m *Participant) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().GetMemberId() == m.member.GetMemberId()
}

// GetLeaderID returns current leader's member ID.
func (m *Participant) GetLeaderID() uint64 {
	return m.GetLeader().GetMemberId()
}

// GetLeader returns current leader of the election group.
func (m *Participant) GetLeader() *pdpb.Member {
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

// setLeader sets the member's leader.
func (m *Participant) setLeader(member *pdpb.Member) {
	m.leader.Store(member)
}

// unsetLeader unsets the member's leader.
func (m *Participant) unsetLeader() {
	m.leader.Store(&pdpb.Member{})
}

// EnableLeader declares the member itself to be the leader.
func (m *Participant) EnableLeader() {
	m.setLeader(m.member)
}

// GetLeaderPath returns the path of the leader.
func (m *Participant) GetLeaderPath() string {
	return m.leaderPath
}

// GetLeadership returns the leadership of the member.
func (m *Participant) GetLeadership() *election.Leadership {
	return m.leadership
}

// CampaignLeader is used to campaign a member's leadership and make it become a leader.
func (m *Participant) CampaignLeader(leaseTimeout int64) error {
	return m.leadership.Campaign(leaseTimeout, m.MemberValue())
}

// KeepLeader is used to keep the leader's leadership.
func (m *Participant) KeepLeader(ctx context.Context) {
	m.leadership.Keep(ctx)
}

// PrecheckLeader does some pre-check before checking whether or not it's the leader.
// It returns true if it passes the pre-check, false otherwise.
func (m *Participant) PrecheckLeader() error {
	// No specific thing to check. Returns no error.
	return nil
}

// CheckLeader checks returns true if it is needed to check later.
func (m *Participant) CheckLeader() (*pdpb.Member, int64, bool) {
	if err := m.PrecheckLeader(); err != nil {
		log.Error("failed to pass pre-check, check the leader later", errs.ZapError(errs.ErrEtcdLeaderNotFound))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	leader, rev, err := election.GetLeader(m.client, m.GetLeaderPath())
	if err != nil {
		log.Error("getting the leader meets error", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}
	if leader != nil {
		if m.IsSameLeader(leader) {
			// oh, we are already the leader, which indicates we may meet something wrong
			// in previous CampaignLeader. We should delete the leadership and campaign again.
			log.Warn("the leader has not changed, delete and campaign again", zap.Stringer("old-leader", leader))
			// Delete the leader itself and let others start a new election again.
			if err = m.leadership.DeleteLeaderKey(); err != nil {
				log.Error("deleting the leader key meets error", errs.ZapError(err))
				time.Sleep(200 * time.Millisecond)
				return nil, 0, true
			}
			// Return nil and false to make sure the campaign will start immediately.
			return nil, 0, false
		}
	}
	return leader, rev, false
}

// WatchLeader is used to watch the changes of the leader.
func (m *Participant) WatchLeader(serverCtx context.Context, leader *pdpb.Member, revision int64) {
	m.setLeader(leader)
	m.leadership.Watch(serverCtx, revision)
	m.unsetLeader()
}

// ResetLeader is used to reset the member's current leadership.
// Basically it will reset the leader lease and unset leader info.
func (m *Participant) ResetLeader() {
	m.leadership.Reset()
	m.unsetLeader()
}

// IsSameLeader checks whether a server is the leader itself.
func (m *Participant) IsSameLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == m.ID()
}

// CheckPriority checks whether there is another participant has higher priority and resign it as the leader if so.
func (m *Participant) CheckPriority(ctx context.Context) {
	// TODO: implement weighted-election when it's in need
}

func (m *Participant) getMemberLeaderPriorityPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/leader_priority", id))
}

// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
func (m *Participant) GetDCLocationPathPrefix() string {
	return path.Join(m.rootPath, dcLocationConfigEtcdPrefix)
}

// GetDCLocationPath returns the dc-location path of a member with the given member ID.
func (m *Participant) GetDCLocationPath(id uint64) string {
	return path.Join(m.GetDCLocationPathPrefix(), fmt.Sprint(id))
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (m *Participant) SetMemberLeaderPriority(id uint64, priority int) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("save etcd leader priority failed, maybe not the leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's etcd leader priority config.
func (m *Participant) DeleteMemberLeaderPriority(id uint64) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("delete etcd leader priority failed, maybe not the leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// DeleteMemberDCLocationInfo removes a member's dc-location info.
func (m *Participant) DeleteMemberDCLocationInfo(id uint64) error {
	key := m.GetDCLocationPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("delete dc-location info failed, maybe not the leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (m *Participant) GetMemberLeaderPriority(id uint64) (int, error) {
	key := m.getMemberLeaderPriorityPath(id)
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

func (m *Participant) getMemberBinaryDeployPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/deploy_path", id))
}

// GetMemberDeployPath loads a member's binary deploy path.
func (m *Participant) GetMemberDeployPath(id uint64) (string, error) {
	key := m.getMemberBinaryDeployPath(id)
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
func (m *Participant) SetMemberDeployPath(id uint64) error {
	key := m.getMemberBinaryDeployPath(id)
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

func (m *Participant) getMemberGitHashPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/git_hash", id))
}

func (m *Participant) getMemberBinaryVersionPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/binary_version", id))
}

// GetMemberBinaryVersion loads a member's binary version.
func (m *Participant) GetMemberBinaryVersion(id uint64) (string, error) {
	key := m.getMemberBinaryVersionPath(id)
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
func (m *Participant) GetMemberGitHash(id uint64) (string, error) {
	key := m.getMemberGitHashPath(id)
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
func (m *Participant) SetMemberBinaryVersion(id uint64, releaseVersion string) error {
	key := m.getMemberBinaryVersionPath(id)
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
func (m *Participant) SetMemberGitHash(id uint64, gitHash string) error {
	key := m.getMemberGitHashPath(id)
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
