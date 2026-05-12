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

// Election defines the interface for the election related logic.
type Election interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsServing returns whether the member is serving or not.
	// For PD, whether the member is the leader.
	// For microservices, whether the participant is the primary.
	IsServing() bool
	// PromoteSelf declares the member itself to be the leader or primary.
	PromoteSelf()
	// Campaign is used to campaign the leadership and make it become a leader or primary in an election group.
	Campaign(ctx context.Context, leaseTimeout int64) error
	// Resign is used to reset the member's current leadership.
	// For PD, it will reset the leader.
	// For microservices, it will reset the primary.
	Resign()
	// GetServingUrls returns current serving listen urls:
	// For PD, it returns the listen urls of the leader,
	// For microservices, it returns the listen urls of the primary.
	GetServingUrls() []string
	// GetElectionPath returns the path of the election:
	// For PD it's the leader path.
	// For microservices it's the primary path.
	GetElectionPath() string
	// GetLeadership returns the leadership of the member.
	GetLeadership() *election.Leadership
}
