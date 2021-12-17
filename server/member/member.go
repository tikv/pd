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
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

const (
	// The timeout to wait transfer etcd leader to complete.
	moveLeaderTimeout          = 5 * time.Second
	dcLocationConfigEtcdPrefix = "dc-location"
)

// Member is used for the election related logic.
type Member struct {
	leadership *election.Leadership
	leader     atomic.Value // stored as *pdpb.Member
	// etcd and cluster information.
	etcd     *embed.Etcd
	client   *clientv3.Client
	id       uint64       // etcd server id.
	member   *pdpb.Member // current PD's info.
	rootPath string
	// memberValue is the serialized string of `member`. It will be save in
	// etcd leader key when the PD node is successfully elected as the PD leader
	// of the cluster. Every write will use it to check PD leadership.
	memberValue string
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

// MemberValue returns the member value.
func (m *Member) MemberValue() string {
	return m.memberValue
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

// IsLeader returns whether the server is PD leader or not by checking its leadership's lease and leader info.
func (m *Member) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().GetMemberId() == m.member.GetMemberId()
}

// GetLeaderID returns current PD leader's member ID.
func (m *Member) GetLeaderID() uint64 {
	return m.GetLeader().GetMemberId()
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
}

// unsetLeader unsets the member's PD leader.
func (m *Member) unsetLeader() {
	m.leader.Store(&pdpb.Member{})
}

// EnableLeader sets the member itself to a PD leader.
func (m *Member) EnableLeader() {
	m.setLeader(m.member)
}

// GetLeaderPath returns the path of the PD leader.
func (m *Member) GetLeaderPath() string {
	return path.Join(m.rootPath, "leader")
}

// GetLeadership returns the leadership of the PD member.
func (m *Member) GetLeadership() *election.Leadership {
	return m.leadership
}

// CampaignLeader is used to campaign a PD member's leadership
// and make it become a PD leader.
func (m *Member) CampaignLeader(leaseTimeout int64) error {
	return m.leadership.Campaign(leaseTimeout, m.MemberValue())
}

// KeepLeader is used to keep the PD leader's leadership.
func (m *Member) KeepLeader(ctx context.Context) {
	m.leadership.Keep(ctx)
}

// CheckLeader checks returns true if it is needed to check later.
func (m *Member) CheckLeader() (*pdpb.Member, int64, bool) {
	if m.GetEtcdLeader() == 0 {
		log.Error("no etcd leader, check pd leader later", errs.ZapError(errs.ErrEtcdLeaderNotFound))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	leader, rev, err := election.GetLeader(m.client, m.GetLeaderPath())
	if err != nil {
		log.Error("getting pd leader meets error", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}
	if leader != nil {
		if m.isSameLeader(leader) {
			// oh, we are already a PD leader, which indicates we may meet something wrong
			// in previous CampaignLeader. We should delete the leadership and campaign again.
			log.Warn("the pd leader has not changed, delete and campaign again", zap.Stringer("old-pd-leader", leader))
			// Delete the leader itself and let others start a new election again.
			if err = m.leadership.DeleteLeaderKey(); err != nil {
				log.Error("deleting pd leader key meets error", errs.ZapError(err))
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
func (m *Member) WatchLeader(serverCtx context.Context, leader *pdpb.Member, revision int64) {
	m.setLeader(leader)
	m.leadership.Watch(serverCtx, revision)
	m.unsetLeader()
}

// ResetLeader is used to reset the PD member's current leadership.
// Basically it will reset the leader lease and unset leader info.
func (m *Member) ResetLeader() {
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
func (m *Member) isSameLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == m.ID()
}

// MemberInfo initializes the member info.
func (m *Member) MemberInfo(cfg *config.Config, name string, rootPath string) {
	leader := &pdpb.Member{
		Name:       name,
		MemberId:   m.ID(),
		ClientUrls: strings.Split(cfg.AdvertiseClientUrls, ","),
		PeerUrls:   strings.Split(cfg.AdvertisePeerUrls, ","),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal pd leader meet error", zap.Stringer("pd-leader", leader), errs.ZapError(errs.ErrMarshalLeader, err))
	}
	m.member = leader
	m.memberValue = string(data)
	m.rootPath = rootPath
	m.leadership = election.NewLeadership(m.client, m.GetLeaderPath(), "pd leader election")
}

// ResignEtcdLeader resigns current PD's etcd leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (m *Member) ResignEtcdLeader(ctx context.Context, from string, nextEtcdLeader string) error {
	log.Info("try to resign etcd leader to next pd-server", zap.String("from", from), zap.String("to", nextEtcdLeader))
	// Determine next etcd leader candidates.
	var etcdLeaderIDs []uint64
	res, err := etcdutil.ListEtcdMembers(m.client)
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
	nextEtcdLeaderID := etcdLeaderIDs[rand.Intn(len(etcdLeaderIDs))]
	return m.MoveEtcdLeader(ctx, m.ID(), nextEtcdLeaderID)
}

func (m *Member) getMemberLeaderPriorityPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/leader_priority", id))
}

// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
func (m *Member) GetDCLocationPathPrefix() string {
	return path.Join(m.rootPath, dcLocationConfigEtcdPrefix)
}

// GetDCLocationPath returns the dc-location path of a member with the given member ID.
func (m *Member) GetDCLocationPath(id uint64) string {
	return path.Join(m.GetDCLocationPathPrefix(), fmt.Sprint(id))
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (m *Member) SetMemberLeaderPriority(id uint64, priority int) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("save etcd leader priority failed, maybe not pd leader")
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's etcd leader priority config.
func (m *Member) DeleteMemberLeaderPriority(id uint64) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("delete etcd leader priority failed, maybe not pd leader")
	}
	return nil
}

// DeleteMemberDCLocationInfo removes a member's dc-location info.
func (m *Member) DeleteMemberDCLocationInfo(id uint64) error {
	key := m.GetDCLocationPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("delete dc-location info failed, maybe not pd leader")
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (m *Member) GetMemberLeaderPriority(id uint64) (int, error) {
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

func (m *Member) getMemberBinaryDeployPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/deploy_path", id))
}

// GetMemberDeployPath loads a member's binary deploy path.
func (m *Member) GetMemberDeployPath(id uint64) (string, error) {
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
func (m *Member) SetMemberDeployPath(id uint64) error {
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

func (m *Member) getMemberGitHashPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/git_hash", id))
}

func (m *Member) getMemberBinaryVersionPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/binary_version", id))
}

// GetMemberBinaryVersion loads a member's binary version.
func (m *Member) GetMemberBinaryVersion(id uint64) (string, error) {
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
func (m *Member) GetMemberGitHash(id uint64) (string, error) {
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
func (m *Member) SetMemberBinaryVersion(id uint64, releaseVersion string) error {
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
func (m *Member) SetMemberGitHash(id uint64, gitHash string) error {
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

// Close gracefully shuts down all servers/listeners.
func (m *Member) Close() {
	m.Etcd().Close()
}
