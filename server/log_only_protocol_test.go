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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
)

func capabilityTestMembers(ids ...uint64) []*pdpb.Member {
	members := make([]*pdpb.Member, 0, len(ids))
	for _, id := range ids {
		members = append(members, &pdpb.Member{MemberId: id})
	}
	return members
}

func TestPDMemberCapabilityDeclaresOnlyRuntimeSelf(t *testing.T) {
	members := capabilityTestMembers(1, 2)
	members[1].LogOnlyProtocolVersion = 2 // A cached remote value is not self proof.
	result := advertiseLocalMemberProtocol(members, 1)
	require.Equal(t, core.LogOnlyProtocolVersion, result[0].GetLogOnlyProtocolVersion())
	require.Zero(t, result[1].GetLogOnlyProtocolVersion())
	require.Zero(t, members[0].GetLogOnlyProtocolVersion())
	require.Equal(t, uint32(2), members[1].GetLogOnlyProtocolVersion())
}

func TestRuntimeMemberProofDoesNotTrustLeaderOrAlphaVersion(t *testing.T) {
	for _, leaderID := range []uint64{1, 2} {
		response := &pdpb.GetMembersResponse{
			Header:  &pdpb.ResponseHeader{ClusterId: 42},
			Members: []*pdpb.Member{{MemberId: 2, BinaryVersion: "v99.0.0-alpha"}},
			Leader:  &pdpb.Member{MemberId: leaderID, LogOnlyProtocolVersion: 2},
		}
		version, err := runtimeMemberProtocolVersion(2, 42, response)
		require.NoError(t, err)
		require.Zero(t, version)
		response.Members[0].LogOnlyProtocolVersion = 2
		version, err = runtimeMemberProtocolVersion(2, 42, response)
		require.NoError(t, err)
		require.Equal(t, uint32(2), version)
		_, err = runtimeMemberProtocolVersion(2, 43, response)
		require.Error(t, err)
	}
}

func TestPDMemberProtocolNegotiationIsParallelAndNotPositivelyCached(t *testing.T) {
	members := capabilityTestMembers(1, 2, 3)
	loader := func(ctx context.Context) ([]*pdpb.Member, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return members, nil
	}
	started := make(chan uint64, 2)
	release := make(chan struct{})
	probe := func(ctx context.Context, member *pdpb.Member) (uint32, error) {
		started <- member.GetMemberId()
		select {
		case <-release:
			return 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan uint32, 1)
	go func() { result <- negotiatePDMemberProtocol(ctx, 1, loader, probe) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			require.FailNow(t, "all PD members must be probed in parallel")
		}
	}
	close(release)
	require.Equal(t, uint32(2), <-result)
	// A previously successful round must not authorize a now-old/unreachable member.
	version := negotiatePDMemberProtocol(context.Background(), 1, loader, func(_ context.Context, member *pdpb.Member) (uint32, error) {
		if member.GetMemberId() == 3 {
			return 0, errors.New("unreachable")
		}
		return 2, nil
	})
	require.Zero(t, version)
}

func TestPDMemberProtocolNegotiationRejectsMembershipChangeAndTimeout(t *testing.T) {
	reads := 0
	loader := func(context.Context) ([]*pdpb.Member, error) {
		reads++
		if reads == 1 {
			return capabilityTestMembers(1, 2), nil
		}
		return capabilityTestMembers(1, 2, 3), nil
	}
	version := negotiatePDMemberProtocol(context.Background(), 1, loader, func(context.Context, *pdpb.Member) (uint32, error) { return 2, nil })
	require.Zero(t, version)
	require.Equal(t, 2, reads)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	loader = func(context.Context) ([]*pdpb.Member, error) { return capabilityTestMembers(1, 2), nil }
	version = negotiatePDMemberProtocol(ctx, 1, loader, func(ctx context.Context, _ *pdpb.Member) (uint32, error) { <-ctx.Done(); return 0, ctx.Err() })
	require.Zero(t, version)
}

func TestPDMemberProtocolMinimumIncludesFutureLeaders(t *testing.T) {
	members := capabilityTestMembers(1, 2, 3)
	for _, leaderID := range []uint64{2, 3} {
		version := negotiatePDMemberProtocol(context.Background(), 1,
			func(context.Context) ([]*pdpb.Member, error) { return members, nil },
			func(_ context.Context, member *pdpb.Member) (uint32, error) {
				response := &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}, Leader: &pdpb.Member{MemberId: leaderID, LogOnlyProtocolVersion: 2}, Members: []*pdpb.Member{{MemberId: member.GetMemberId(), LogOnlyProtocolVersion: 2}}}
				if member.GetMemberId() == 3 {
					response.Members[0].LogOnlyProtocolVersion = 0
				}
				return runtimeMemberProtocolVersion(member.GetMemberId(), 42, response)
			})
		require.Zero(t, version)
	}
}
