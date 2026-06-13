package paxoslease

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type MockTransport struct {
	nodes map[NodeID]*Paxos
}

func (t *MockTransport) Send(to NodeID, msg Message) {
	if n, ok := t.nodes[to]; ok {
		// Direct synchronous call for simplicity in testing logic flow, 
		// but real paxos is async. 
		// To avoid deadlock if HandleMessage locks something expected, be careful.
		// However, HandleMessage locks p.mu. If Send is called within p.mu, we have deadlock.
		// Paxos implementation: 
		// Campaign -> lock -> Broadcast -> Send -> HandleMessage -> lock (Deadlock if synchronous)
		// So we MUST run in goroutine.
		go n.HandleMessage(msg)
	}
}

func (t *MockTransport) Broadcast(msg Message) {
	for _, n := range t.nodes {
		go n.HandleMessage(msg)
	}
}

func TestPaxosLease(t *testing.T) {
	// Fixed start time
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	currentTime := baseTime
	
	mockNow := func() time.Time {
		return currentTime
	}

	nodes := []NodeID{1, 2, 3}
	transport := &MockTransport{nodes: make(map[NodeID]*Paxos)}

	p1 := NewPaxos(1, nodes, transport)
	p1.now = mockNow
	p2 := NewPaxos(2, nodes, transport)
	p2.now = mockNow
	p3 := NewPaxos(3, nodes, transport)
	p3.now = mockNow

	transport.nodes[1] = p1
	transport.nodes[2] = p2
	transport.nodes[3] = p3

	// Node 1 campaigns
	p1.Campaign(5 * time.Second)

	// Wait for consensus
	time.Sleep(100 * time.Millisecond)

	leader1 := p1.GetLeader()
	leader2 := p2.GetLeader()
	leader3 := p3.GetLeader()

	assert.Equal(t, NodeID(1), leader1)
	assert.Equal(t, NodeID(1), leader2)
	assert.Equal(t, NodeID(1), leader3)

	// Node 2 campaigns, should fail (or not proceed) because lease is valid
	// In the code: if p.isLeaseValid() && p.currentLease.Owner != p.id { return }
	// Since we haven't advanced time, lease is valid.
	p2.Campaign(5 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, NodeID(1), p2.GetLeader())
}

func TestPaxosLeaseExpiry(t *testing.T) {
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	var currentTime time.Time = baseTime
	
	mockNow := func() time.Time {
		return currentTime
	}

	nodes := []NodeID{1, 2, 3}
	transport := &MockTransport{nodes: make(map[NodeID]*Paxos)}

	p1 := NewPaxos(1, nodes, transport)
	p1.now = mockNow
	p2 := NewPaxos(2, nodes, transport)
	p2.now = mockNow
	p3 := NewPaxos(3, nodes, transport)
	p3.now = mockNow

	transport.nodes[1] = p1
	transport.nodes[2] = p2
	transport.nodes[3] = p3

	// 1. Node 1 obtains lease
	p1.Campaign(5 * time.Second)
	time.Sleep(100 * time.Millisecond)
	
	assert.Equal(t, NodeID(1), p1.GetLeader())
	assert.Equal(t, NodeID(1), p2.GetLeader())

	// 2. Advance time to expire lease (6 seconds later)
	currentTime = currentTime.Add(6 * time.Second)
	
	// Verify everyone sees no leader
	assert.Equal(t, NodeID(0), p1.GetLeader())
	assert.Equal(t, NodeID(0), p2.GetLeader())

	// 3. Node 2 campaigns
	p2.Campaign(5 * time.Second)
	time.Sleep(100 * time.Millisecond)

	// Node 2 should be leader
	assert.Equal(t, NodeID(2), p2.GetLeader())
	assert.Equal(t, NodeID(2), p1.GetLeader())
}

func TestPaxosLeaseRenewal(t *testing.T) {
	// Test that the owner can renew the lease
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	var currentTime time.Time = baseTime
	
	mockNow := func() time.Time {
		return currentTime
	}

	nodes := []NodeID{1, 2, 3}
	transport := &MockTransport{nodes: make(map[NodeID]*Paxos)}

	p1 := NewPaxos(1, nodes, transport)
	p1.now = mockNow
	p2 := NewPaxos(2, nodes, transport)
	p2.now = mockNow
	transport.nodes[1] = p1
	transport.nodes[2] = p2

	// Node 1 obtains lease
	p1.Campaign(5 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, NodeID(1), p1.GetLeader())

	// Advance time by 4 seconds (lease valid but close to expiry)
	currentTime = currentTime.Add(4 * time.Second)
	assert.Equal(t, NodeID(1), p1.GetLeader())

	// Node 1 campaigns again to renew
	p1.Campaign(5 * time.Second)
	time.Sleep(100 * time.Millisecond)

	// Check start time has updated?
	// We need to check internals or verify lease validity after another 2 seconds (total 6s from start)
	currentTime = currentTime.Add(2 * time.Second)
	// Total 6 seconds from initial start. Initial lease would have expired.
	// If renewal worked, it should still be valid.
	assert.Equal(t, NodeID(1), p1.GetLeader())
}
