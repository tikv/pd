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
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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
// Member, Participant relies on etcd for election, but it's decoupled
// from the embedded etcd. It implements Member interface.
type Participant struct {
	keypath.MsParam
	leadership *election.Leadership
	// stored as participant type
	primary     atomic.Value
	client      *clientv3.Client
	participant participant
	// participantValue is the serialized string of `participant`. It will be saved in the
	// primary key when this participant is successfully elected as the primary of
	// the group. Every write will use it to check the leadership.
	participantValue string
	// campaignChecker is used to check whether the additional constraints for a
	// campaign are satisfied. If it returns false, the campaign will fail.
	campaignChecker atomic.Value // Store as leadershipCheckFunc
	// expectedPrimaryLease is the expected lease for the primary.
	expectedPrimaryLease atomic.Value // stored as *election.Lease
}

// NewParticipant create a new Participant.
func NewParticipant(client *clientv3.Client, msParam keypath.MsParam) *Participant {
	return &Participant{
		MsParam: msParam,
		client:  client,
	}
}

// InitInfo initializes the participant info.
func (p *Participant) InitInfo(participant participant, purpose string) {
	data, err := participant.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal participant meet error", zap.String("participant-name", participant.String()), errs.ZapError(errs.ErrMarshalParticipant, err))
	}
	p.participant = participant
	p.participantValue = string(data)
	p.leadership = election.NewLeadership(p.client, p.GetElectionPath(), purpose)
	log.Info("participant joining election", zap.String("participant-info", participant.String()), zap.String("primary-path", p.GetElectionPath()))
}

// ID returns the unique ID for this participant in the election group
func (p *Participant) ID() uint64 {
	return p.participant.GetId()
}

// Name returns the unique name in the election group.
func (p *Participant) Name() string {
	return p.participant.GetName()
}

// GetMember returns the participant.
func (p *Participant) GetMember() any {
	return p.participant
}

// MemberValue returns the participant value.
func (p *Participant) MemberValue() string {
	return p.participantValue
}

// ParticipantString returns the participant string.
func (p *Participant) ParticipantString() string {
	if p.participant == nil {
		return ""
	}
	return p.participant.String()
}

// Client returns the etcd client.
func (p *Participant) Client() *clientv3.Client {
	return p.client
}

// IsServing returns whether the participant is the primary or not by checking its leadership's
// lease and primary info.
func (p *Participant) IsServing() bool {
	return p.leadership.Check() && p.getPrimary().GetId() == p.participant.GetId() && p.campaignCheck()
}

// IsPrimaryElected returns true if the primary exists; otherwise false
func (p *Participant) IsPrimaryElected() bool {
	return p.getPrimary().GetId() != 0
}

// GetServingUrls returns current primary's listen urls
func (p *Participant) GetServingUrls() []string {
	return p.getPrimary().GetListenUrls()
}

// GetPrimaryID returns current primary's participant ID.
func (p *Participant) GetPrimaryID() uint64 {
	return p.getPrimary().GetId()
}

// getPrimary returns current primary of the election group.
func (p *Participant) getPrimary() participant {
	primary := p.primary.Load()
	if primary == nil {
		return NewParticipantByService(p.ServiceName)
	}
	return primary.(participant)
}

// setPrimary sets the participant's primary.
func (p *Participant) setPrimary(participant participant) {
	p.primary.Store(participant)
}

// unsetPrimary unsets the participant's primary.
func (p *Participant) unsetPrimary() {
	primary := NewParticipantByService(p.ServiceName)
	p.primary.Store(primary)
}

// PromoteSelf declares the participant itself to be the primary.
func (p *Participant) PromoteSelf() {
	p.setPrimary(p.participant)
}

// GetElectionPath returns the path of the primary.
func (p *Participant) GetElectionPath() string {
	return keypath.ElectionPath(&p.MsParam)
}

// GetLeadership returns the leadership of the participant.
func (p *Participant) GetLeadership() *election.Leadership {
	return p.leadership
}

// Campaign is used to campaign the leadership and make it become a primary.
func (p *Participant) Campaign(_ context.Context, leaseTimeout int64) error {
	if !p.campaignCheck() {
		return errs.ErrCheckCampaign
	}
	return p.leadership.Campaign(leaseTimeout, p.MemberValue())
}

// getPersistentPrimary gets the corresponding primary from etcd by given electionPath (as the key).
func (p *Participant) getPersistentPrimary() (participant, int64, error) {
	primary := NewParticipantByService(p.ServiceName)
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(p.client, p.GetElectionPath(), primary)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return primary, rev, nil
}

// CheckPrimary checks if someone else is taking the leadership. If yes, returns the primary;
// otherwise returns a bool which indicates if it is needed to check later.
func (p *Participant) CheckPrimary() (*Primary, bool) {
	primary, revision, err := p.getPersistentPrimary()
	if err != nil {
		log.Error("getting the primary meets error", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, true
	}
	if primary == nil {
		// no primary yet
		return nil, false
	}

	if p.isSamePrimary(primary) {
		// oh, we are already the primary, which indicates we may meet something wrong
		// in previous Campaign. We should delete the leadership and campaign again.
		log.Warn("the primary has not changed, delete and campaign again", zap.Stringer("old-primary", primary))
		// Delete the primary itself and let others start a new election again.
		if err = p.leadership.DeleteLeaderKey(); err != nil {
			log.Error("deleting the primary key meets error", errs.ZapError(err))
			time.Sleep(200 * time.Millisecond)
			return nil, true
		}
		// Return nil and false to make sure the campaign will start immediately.
		return nil, false
	}

	return &Primary{
		wrapper:     p,
		participant: primary,
		revision:    revision,
	}, false
}

// WatchLeader is used to watch the changes of the primary.
func (p *Participant) WatchLeader(ctx context.Context, primary participant, revision int64) {
	p.setPrimary(primary)
	p.leadership.Watch(ctx, revision)
	p.unsetPrimary()
}

// Resign is used to reset the participant's current leadership.
// Basically it will reset the primary lease and unset primary info.
func (p *Participant) Resign() {
	p.leadership.Reset()
	p.unsetPrimary()
}

// isSamePrimary checks whether a server is the primary itself.
func (p *Participant) isSamePrimary(primary participant) bool {
	return primary.GetId() == p.ID()
}

func (p *Participant) campaignCheck() bool {
	checker := p.campaignChecker.Load()
	if checker == nil {
		return true
	}
	checkerFunc, ok := checker.(leadershipCheckFunc)
	if !ok || checkerFunc == nil {
		return true
	}
	return checkerFunc(p.leadership)
}

// SetCampaignChecker sets the pre-campaign checker.
func (p *Participant) SetCampaignChecker(checker leadershipCheckFunc) {
	p.campaignChecker.Store(checker)
}

// SetExpectedPrimaryLease sets the expected lease for the primary.
func (p *Participant) SetExpectedPrimaryLease(lease *election.Lease) {
	p.expectedPrimaryLease.Store(lease)
}

// GetExpectedPrimaryLease gets the expected lease for the primary.
func (p *Participant) GetExpectedPrimaryLease() *election.Lease {
	l := p.expectedPrimaryLease.Load()
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
	}
	return p
}

// Primary is the primary in the election group backed by the etcd, but it's
// decoupled from the embedded etcd.
type Primary struct {
	wrapper     *Participant
	participant participant
	revision    int64
}

// GetListenUrls returns current primary's client urls
func (l *Primary) GetListenUrls() []string {
	return l.participant.GetListenUrls()
}

// GetRevision the revision of the primary in etcd
func (l *Primary) GetRevision() int64 {
	return l.revision
}

// String declares fmt.Stringer
func (l *Primary) String() string {
	return l.participant.String()
}

// Watch on the primary
func (l *Primary) Watch(ctx context.Context) {
	l.wrapper.WatchLeader(ctx, l.participant, l.revision)
}
