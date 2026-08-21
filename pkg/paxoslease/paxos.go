package paxoslease

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type NodeID uint64

type ProposalID struct {
	Number uint64
	NodeID NodeID
}

func (p ProposalID) Greater(other ProposalID) bool {
	if p.Number != other.Number {
		return p.Number > other.Number
	}
	return p.NodeID > other.NodeID
}

type Lease struct {
	Owner     NodeID
	Duration  time.Duration
	StartTime time.Time
}

type MessageType int

const (
	MsgPrepare MessageType = iota
	MsgPromise
	MsgAccept
	MsgAccepted
	MsgHeartbeat
)

type Message struct {
	Type        MessageType
	From        NodeID
	To          NodeID
	ProposalID  ProposalID
	Value       Lease
	AcceptedID  ProposalID
	AcceptedVal Lease
}

type Transport interface {
	Send(to NodeID, msg Message)
	Broadcast(msg Message)
}

type Paxos struct {
	mu        sync.Mutex
	id        NodeID
	nodes     []NodeID
	quorum    int
	transport Transport

	// Acceptor state
	maxSeen     ProposalID
	acceptedID  ProposalID
	acceptedVal Lease

	// Proposer state
	currentProposalID ProposalID
	promises          map[NodeID]Message
	accepts           map[NodeID]bool
	isProposing       bool
	desiredDuration   time.Duration
	
	// Learner state
	// We map ProposalID -> set of nodes that accepted it
	acceptedCounts map[ProposalID]map[NodeID]bool
	currentLease   Lease

	logger *zap.Logger
	
	// for testing
	now func() time.Time
}

func NewPaxos(id NodeID, nodes []NodeID, transport Transport) *Paxos {
	return &Paxos{
		id:             id,
		nodes:          nodes,
		quorum:         (len(nodes) / 2) + 1,
		transport:      transport,
		promises:       make(map[NodeID]Message),
		accepts:        make(map[NodeID]bool),
		acceptedCounts: make(map[ProposalID]map[NodeID]bool),
		logger:         log.With(zap.Uint64("node-id", uint64(id))),
		now:            time.Now,
	}
}

func (p *Paxos) Campaign(leaseDuration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If current lease is valid and owned by someone else, don't campaign yet
	if p.isLeaseValid() && p.currentLease.Owner != p.id {
		return
	}

	p.currentProposalID.Number++
	p.currentProposalID.NodeID = p.id
	p.promises = make(map[NodeID]Message)
	p.accepts = make(map[NodeID]bool)
	p.isProposing = true
	p.desiredDuration = leaseDuration

	msg := Message{
		Type:       MsgPrepare,
		From:       p.id,
		ProposalID: p.currentProposalID,
	}
	p.transport.Broadcast(msg)
}

func (p *Paxos) HandleMessage(msg Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch msg.Type {
	case MsgPrepare:
		p.handlePrepare(msg)
	case MsgPromise:
		p.handlePromise(msg)
	case MsgAccept:
		p.handleAccept(msg)
	case MsgAccepted:
		p.handleAccepted(msg)
	}
}

func (p *Paxos) handlePrepare(msg Message) {
	if msg.ProposalID.Greater(p.maxSeen) || msg.ProposalID == p.maxSeen {
		p.maxSeen = msg.ProposalID
		reply := Message{
			Type:        MsgPromise,
			From:        p.id,
			To:          msg.From,
			ProposalID:  msg.ProposalID,
			AcceptedID:  p.acceptedID,
			AcceptedVal: p.acceptedVal,
		}
		p.transport.Send(msg.From, reply)
	}
}

func (p *Paxos) handlePromise(msg Message) {
	if !p.isProposing || msg.ProposalID != p.currentProposalID {
		return
	}

	p.promises[msg.From] = msg

	if len(p.promises) >= p.quorum {
		// Pick value
		maxAcceptedID := ProposalID{}
		valToPropose := Lease{
			Owner:     p.id,
			Duration:  p.desiredDuration,
			StartTime: p.now(),
		}

		// If any acceptor has accepted a value, pick the one with highest ProposalID
		foundAccepted := false
		for _, m := range p.promises {
			if m.AcceptedID.Number != 0 {
				if m.AcceptedID.Greater(maxAcceptedID) {
					maxAcceptedID = m.AcceptedID
					valToPropose = m.AcceptedVal
					foundAccepted = true
				}
			}
		}

		if foundAccepted {
			isOwner := valToPropose.Owner == p.id
			isExpired := p.now().Sub(valToPropose.StartTime) > valToPropose.Duration
			if isExpired || isOwner {
				// Expired or we are owner (renew), propose our own
				valToPropose = Lease{
					Owner:     p.id,
					Duration:  p.desiredDuration,
					StartTime: p.now(),
				}
			}
		}

		acceptMsg := Message{
			Type:       MsgAccept,
			From:       p.id,
			ProposalID: p.currentProposalID,
			Value:      valToPropose,
		}
		p.transport.Broadcast(acceptMsg)
		p.isProposing = false // Entered accept phase
	}
}

func (p *Paxos) handleAccept(msg Message) {
	if msg.ProposalID.Greater(p.maxSeen) || msg.ProposalID == p.maxSeen {
		p.maxSeen = msg.ProposalID
		p.acceptedID = msg.ProposalID
		p.acceptedVal = msg.Value

		reply := Message{
			Type:       MsgAccepted,
			From:       p.id,
			To:         msg.From,
			ProposalID: msg.ProposalID,
			Value:      msg.Value,
		}
		p.transport.Broadcast(reply) // Broadcast accepted so everyone learns
	}
}

func (p *Paxos) handleAccepted(msg Message) {
	if _, ok := p.acceptedCounts[msg.ProposalID]; !ok {
		p.acceptedCounts[msg.ProposalID] = make(map[NodeID]bool)
	}
	p.acceptedCounts[msg.ProposalID][msg.From] = true

	if len(p.acceptedCounts[msg.ProposalID]) >= p.quorum {
		// Chosen!
		// Only update current lease if this proposal is newer or same as what we have?
		// In PaxosLease, any chosen value is valid.
		// We should respect the one with highest proposal ID that is chosen.
		
		// Simplified: just update current lease
		p.currentLease = msg.Value
	}
}

func (p *Paxos) isLeaseValid() bool {
	return p.currentLease.Owner != 0 && p.now().Sub(p.currentLease.StartTime) < p.currentLease.Duration
}

func (p *Paxos) GetLeader() NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isLeaseValid() {
		return p.currentLease.Owner
	}
	return 0
}
