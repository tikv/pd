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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/cluster"
)

const logOnlyCapabilityTimeout = 2 * time.Second

// Each endpoint may assert only its own runtime capability. Persisted metadata
// or a cached leader record cannot prove what software another member is running.
func advertiseLocalMemberProtocol(members []*pdpb.Member, selfID uint64) []*pdpb.Member {
	result := make([]*pdpb.Member, len(members))
	for i, member := range members {
		if member == nil {
			result[i] = &pdpb.Member{}
			continue
		}
		result[i] = typeutil.DeepClone(member, core.MemberFactory)
		result[i].LogOnlyProtocolVersion = 0
		if member.GetMemberId() == selfID {
			result[i].LogOnlyProtocolVersion = core.LogOnlyProtocolVersion
		}
	}
	return result
}

func (s *GrpcServer) getClusterLogOnlyProtocolVersion(ctx context.Context) uint32 {
	ctx, cancel := context.WithTimeout(ctx, logOnlyCapabilityTimeout)
	defer cancel()
	version := negotiatePDMemberProtocol(ctx, s.member.ID(),
		func(ctx context.Context) ([]*pdpb.Member, error) {
			return cluster.GetMembersWithContext(ctx, s.GetClient())
		},
		s.queryPDMemberProtocol)
	return min(version, s.Server.getLogOnlyProtocolVersion())
}

func negotiatePDMemberProtocol(ctx context.Context, selfID uint64,
	loadMembers func(context.Context) ([]*pdpb.Member, error),
	probe func(context.Context, *pdpb.Member) (uint32, error),
) uint32 {
	members, err := loadMembers(ctx)
	if err != nil || len(members) == 0 || selfID == 0 {
		return 0
	}
	containsSelf := false
	for _, member := range members {
		containsSelf = containsSelf || member.GetMemberId() == selfID
	}
	if !containsSelf {
		return 0
	}
	results := make(chan uint32, len(members))
	pending := 0
	for _, member := range members {
		if member.GetMemberId() == selfID {
			continue
		}
		pending++
		go func() {
			version, err := probe(ctx, member)
			if err != nil {
				version = 0
			}
			results <- version
		}()
	}
	version := core.LogOnlyProtocolVersion
	for range pending {
		select {
		case value := <-results:
			version = min(version, value)
		case <-ctx.Done():
			return 0
		}
	}
	if version == 0 {
		return 0
	}
	// A member joining while probes run must not escape this capability round.
	current, err := loadMembers(ctx)
	if err != nil || !sameCapabilityMembers(members, current) {
		return 0
	}
	return version
}

func sameCapabilityMembers(before, after []*pdpb.Member) bool {
	if len(before) != len(after) {
		return false
	}
	byID := make(map[uint64]*pdpb.Member, len(before))
	for _, member := range before {
		byID[member.GetMemberId()] = member
	}
	if len(byID) != len(before) {
		return false
	}
	for _, member := range after {
		previous, ok := byID[member.GetMemberId()]
		if !ok || len(previous.GetClientUrls()) != len(member.GetClientUrls()) {
			return false
		}
		for i, address := range previous.GetClientUrls() {
			if address != member.GetClientUrls()[i] {
				return false
			}
		}
		delete(byID, member.GetMemberId())
	}
	return len(byID) == 0
}

func (s *GrpcServer) queryPDMemberProtocol(ctx context.Context, member *pdpb.Member) (uint32, error) {
	var lastError error
	for _, address := range member.GetClientUrls() {
		conn, err := s.getDelegateClient(ctx, address)
		if err != nil {
			lastError = err
			continue
		}
		response, err := pdpb.NewPDClient(conn).GetMembers(ctx, &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()}})
		if err != nil {
			lastError = err
			continue
		}
		return runtimeMemberProtocolVersion(member.GetMemberId(), keypath.ClusterID(), response)
	}
	if lastError == nil {
		lastError = errors.New("PD member has no reachable client URL")
	}
	return 0, lastError
}

func runtimeMemberProtocolVersion(memberID, clusterID uint64, response *pdpb.GetMembersResponse) (uint32, error) {
	if response == nil || response.GetHeader().GetClusterId() != clusterID {
		return 0, errors.New("PD member capability response has an invalid cluster ID")
	}
	if response.GetHeader().GetError() != nil && response.GetHeader().GetError().GetType() != pdpb.ErrorType_OK {
		return 0, errors.New("PD member capability query failed")
	}
	found := false
	var version uint32
	for _, member := range response.GetMembers() {
		if member.GetMemberId() != memberID {
			continue
		}
		if found {
			return 0, errors.New("duplicate PD member capability declaration")
		}
		found, version = true, member.GetLogOnlyProtocolVersion()
	}
	if !found {
		return 0, errors.New("PD member did not declare its own capability")
	}
	return version, nil
}
