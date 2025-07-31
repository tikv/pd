// Copyright 2025 TiKV Project Authors.
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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/election"
)

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// Campaign is used to campaign the leadership and make it become a leader or primary in an election group.
	Campaign(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetElectionPath returns the path of the election. for PD it's the leader path, for microservices it's the primary path.
	GetElectionPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
}

// ElectionLeader defines the common interface of the leader, which is the pdpb.Member
// for in PD or the tsopb.Participant in the microservices.
type ElectionLeader interface {
	// GetListenUrls returns the listen urls
	GetListenUrls() []string
	// GetRevision the revision of the leader in etcd
	GetRevision() int64
	// String declares fmt.Stringer
	String() string
	// Watch on itself, the leader in the election group
	Watch(context.Context)
}
