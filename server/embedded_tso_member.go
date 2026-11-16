// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/election"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
)

// embeddedTSOMember keeps TSO's lease checks independent of full PD readiness.
// RaftCluster enables it only after timestamp initialization succeeds. Campaigns
// and their cleanup remain serialized by Server.leaderLoop.
type embeddedTSOMember struct {
	*member.Member
	term atomic.Pointer[election.Lease]
}

func (m *embeddedTSOMember) beginTerm() {
	// Timestamp initialization runs after a successful campaign. Capture its
	// lease so a later granted lease cannot enable TSO before initialization.
	m.term.Store(m.GetLeadership().GetLease())
}

func (m *embeddedTSOMember) endTerm() {
	m.term.Store(nil)
}

// IsServing checks that the initialized TSO still owns its live leadership lease.
func (m *embeddedTSOMember) IsServing() bool {
	lease := m.term.Load()
	return lease != nil && lease == m.GetLeadership().GetLease() && !lease.IsExpired()
}

// Resign withdraws TSO readiness before releasing the shared PD leadership.
func (m *embeddedTSOMember) Resign() {
	m.endTerm()
	m.Member.Resign()
}

// getTSOLeader also advertises a leader whose embedded TSO is ready while the
// rest of PD is still initializing. Other PD request gates use GetLeader and
// Member.IsServing so they cannot access partially initialized cluster state.
func (s *Server) getTSOLeader() *pdpb.Member {
	if leader := s.GetLeader(); leader != nil {
		return leader
	}
	if !s.IsClosed() && s.tsoMember != nil &&
		!s.IsServiceIndependent(mcs.TSOServiceName) &&
		s.tsoMember.IsServing() {
		return s.member.Member()
	}
	return nil
}
