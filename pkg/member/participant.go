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
	"path"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type leadershipCheckFunc func(*election.Leadership) bool

type participant interface {
	GetName() string
	GetId() uint64
	GetListenUrls() []string
	String() string
	Marshal() ([]byte, error)
	Reset()
	ProtoMessage()
}

// Participant is used for the election related logic. Compared to its counterpart
// EmbeddedEtcdMember, Participant relies on etcd for election, but it's decoupled
// from the embedded etcd. It implements Member interface.
type Participant struct {
	leadership *election.Leadership
	// stored as member type
	leader      atomic.Value
	client      *clientv3.Client
	rootPath    string
	leaderPath  string
	member      participant
	serviceName string
	// memberValue is the serialized string of `member`. It will be saved in the
	// leader key when this participant is successfully elected as the leader of
	// the group. Every write will use it to check the leadership.
	memberValue string
	// campaignChecker is used to check whether the additional constraints for a
	// campaign are satisfied. If it returns false, the campaign will fail.
	campaignChecker atomic.Value // Store as leadershipCheckFunc
	// lastLeaderUpdatedTime is the last time when the leader is updated.
	lastLeaderUpdatedTime atomic.Value
	// expectedPrimaryLease is the expected lease for the primary.
	expectedPrimaryLease atomic.Value // stored as *election.Lease
}

// NewParticipant create a new Participant.
func NewParticipant(client *clientv3.Client, serviceName string) *Participant {
	return &Participant{
		client:      client,
		serviceName: serviceName,
	}
}

// InitInfo initializes the member info. The leader key is path.Join(rootPath, leaderName)
func (m *Participant) InitInfo(p participant, rootPath string, leaderName string, purpose string) {
	data, err := p.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal leader meet error", zap.String("member-name", p.String()), errs.ZapError(errs.ErrMarshalLeader, err))
	}
	m.member = p
	m.memberValue = string(data)
	m.rootPath = rootPath
	m.leaderPath = path.Join(rootPath, leaderName)
	m.leadership = election.NewLeadership(m.client, m.GetLeaderPath(), purpose)
	m.lastLeaderUpdatedTime.Store(time.Now())
	log.Info("participant joining election", zap.String("participant-info", p.String()), zap.String("leader-path", m.leaderPath))
}

// ID returns the unique ID for this participant in the election group
func (m *Participant) ID() uint64 {
	return m.member.GetId()
}

// Name returns the unique name in the election group.
func (m *Participant) Name() string {
	return m.member.GetName()
}

// GetMember returns the member.
func (m *Participant) GetMember() any {
	return m.member
}

// MemberValue returns the member value.
func (m *Participant) MemberValue() string {
	return m.memberValue
}

// Client returns the etcd client.
func (m *Participant) Client() *clientv3.Client {
	return m.client
}

// IsLeader returns whether the participant is the leader or not by checking its leadership's
// lease and leader info.
func (m *Participant) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().GetId() == m.member.GetId() && m.campaignCheck()
}

// IsLeaderElected returns true if the leader exists; otherwise false
func (m *Participant) IsLeaderElected() bool {
	return m.GetLeader().GetId() != 0
}

// GetLeaderListenUrls returns current leader's listen urls
func (m *Participant) GetLeaderListenUrls() []string {
	return m.GetLeader().GetListenUrls()
}

// GetLeaderID returns current leader's member ID.
func (m *Participant) GetLeaderID() uint64 {
	return m.GetLeader().GetId()
}

// GetLeader returns current leader of the election group.
func (m *Participant) GetLeader() participant {
	leader := m.leader.Load()
	if leader == nil {
		return NewParticipantByService(m.serviceName)
	}
	return leader.(participant)
}

// setLeader sets the member's leader.
func (m *Participant) setLeader(member participant) {
	m.leader.Store(member)
	m.lastLeaderUpdatedTime.Store(time.Now())
}

// unsetLeader unsets the member's leader.
func (m *Participant) unsetLeader() {
	leader := NewParticipantByService(m.serviceName)
	m.leader.Store(leader)
	m.lastLeaderUpdatedTime.Store(time.Now())
}

// EnableLeader declares the member itself to be the leader.
func (m *Participant) EnableLeader() {
	m.setLeader(m.member)
}

// GetLeaderPath returns the path of the leader.
func (m *Participant) GetLeaderPath() string {
	return m.leaderPath
}

// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
func (m *Participant) GetLastLeaderUpdatedTime() time.Time {
	lastLeaderUpdatedTime := m.lastLeaderUpdatedTime.Load()
	if lastLeaderUpdatedTime == nil {
		return time.Time{}
	}
	return lastLeaderUpdatedTime.(time.Time)
}

// GetLeadership returns the leadership of the member.
func (m *Participant) GetLeadership() *election.Leadership {
	return m.leadership
}

// CampaignLeader is used to campaign the leadership and make it become a leader.
func (m *Participant) CampaignLeader(_ context.Context, leaseTimeout int64) error {
	if !m.campaignCheck() {
		return errs.ErrCheckCampaign
	}
	return m.leadership.Campaign(leaseTimeout, m.MemberValue())
}

// KeepLeader is used to keep the leader's leadership.
func (m *Participant) KeepLeader(ctx context.Context) {
	m.leadership.Keep(ctx)
}

// PreCheckLeader does some pre-check before checking whether or not it's the leader.
// It returns true if it passes the pre-check, false otherwise.
func (*Participant) PreCheckLeader() error {
	// No specific thing to check. Returns no error.
	return nil
}

// getPersistentLeader gets the corresponding leader from etcd by given leaderPath (as the key).
func (m *Participant) getPersistentLeader() (participant, int64, error) {
	leader := NewParticipantByService(m.serviceName)
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(m.client, m.GetLeaderPath(), leader)
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
func (m *Participant) CheckLeader() (ElectionLeader, bool) {
	if err := m.PreCheckLeader(); err != nil {
		log.Error("failed to pass pre-check, check the leader later", errs.ZapError(errs.ErrEtcdLeaderNotFound))
		time.Sleep(200 * time.Millisecond)
		return nil, true
	}

	leader, revision, err := m.getPersistentLeader()
	if err != nil {
		log.Error("getting the leader meets error", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, true
	}
	if leader == nil {
		// no leader yet
		return nil, false
	}

	if m.IsSameLeader(leader) {
		// oh, we are already the leader, which indicates we may meet something wrong
		// in previous CampaignLeader. We should delete the leadership and campaign again.
		log.Warn("the leader has not changed, delete and campaign again", zap.Stringer("old-leader", leader))
		// Delete the leader itself and let others start a new election again.
		if err = m.leadership.DeleteLeaderKey(); err != nil {
			log.Error("deleting the leader key meets error", errs.ZapError(err))
			time.Sleep(200 * time.Millisecond)
			return nil, true
		}
		// Return nil and false to make sure the campaign will start immediately.
		return nil, false
	}

	return &EtcdLeader{
		wrapper:     m,
		participant: leader,
		revision:    revision,
	}, false
}

// WatchLeader is used to watch the changes of the leader.
func (m *Participant) WatchLeader(ctx context.Context, leader participant, revision int64) {
	m.setLeader(leader)
	m.leadership.Watch(ctx, revision)
	m.unsetLeader()
}

// ResetLeader is used to reset the member's current leadership.
// Basically it will reset the leader lease and unset leader info.
func (m *Participant) ResetLeader() {
	m.leadership.Reset()
	m.unsetLeader()
}

// IsSameLeader checks whether a server is the leader itself.
func (m *Participant) IsSameLeader(leader participant) bool {
	return leader.GetId() == m.ID()
}

// CheckPriority checks whether there is another participant has higher priority and resign it as the leader if so.
func (*Participant) CheckPriority(_ context.Context) {
	// TODO: implement weighted-election when it's in need
}

func (m *Participant) getLeaderPriorityPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("participant/%d/leader_priority", id))
}

// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
func (m *Participant) GetDCLocationPathPrefix() string {
	return path.Join(m.rootPath, dcLocationConfigEtcdPrefix)
}

// GetDCLocationPath returns the dc-location path of a member with the given member ID.
func (m *Participant) GetDCLocationPath(id uint64) string {
	return path.Join(m.GetDCLocationPathPrefix(), fmt.Sprint(id))
}

// SetLeaderPriority saves the priority to be elected as the etcd leader.
func (m *Participant) SetLeaderPriority(id uint64, priority int) error {
	key := m.getLeaderPriorityPath(id)
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

// DeleteLeaderPriority removes the etcd leader priority config.
func (m *Participant) DeleteLeaderPriority(id uint64) error {
	key := m.getLeaderPriorityPath(id)
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

// DeleteDCLocationInfo removes the dc-location info.
func (m *Participant) DeleteDCLocationInfo(id uint64) error {
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

// GetLeaderPriority loads the priority to be elected as the etcd leader.
func (m *Participant) GetLeaderPriority(id uint64) (int, error) {
	key := m.getLeaderPriorityPath(id)
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

func (m *Participant) campaignCheck() bool {
	checker := m.campaignChecker.Load()
	if checker == nil {
		return true
	}
	checkerFunc, ok := checker.(leadershipCheckFunc)
	if !ok || checkerFunc == nil {
		return true
	}
	return checkerFunc(m.leadership)
}

// SetCampaignChecker sets the pre-campaign checker.
func (m *Participant) SetCampaignChecker(checker leadershipCheckFunc) {
	m.campaignChecker.Store(checker)
}

// SetExpectedPrimaryLease sets the expected lease for the primary.
func (m *Participant) SetExpectedPrimaryLease(lease *election.Lease) {
	m.expectedPrimaryLease.Store(lease)
}

// GetExpectedPrimaryLease gets the expected lease for the primary.
func (m *Participant) GetExpectedPrimaryLease() *election.Lease {
	l := m.expectedPrimaryLease.Load()
	if l == nil {
		return nil
	}
	return l.(*election.Lease)
}

// NewParticipantByService creates a new participant by service name.
func NewParticipantByService(serviceName string) (p participant) {
	switch serviceName {
	case constant.TSOServiceName:
		p = &tsopb.Participant{}
	case constant.SchedulingServiceName:
		p = &schedulingpb.Participant{}
	case constant.ResourceManagerServiceName:
		p = &resource_manager.Participant{}
	}
	return p
}
