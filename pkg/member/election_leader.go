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

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// ElectionLeader defines the common interface of the leader, which is the pdpb.Member
// for in PD/PD service or the tsopb.Participant in the microservices.
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

// EmbeddedEtcdLeader is the leader in the election group backed by the embedded etcd.
type EmbeddedEtcdLeader struct {
	wrapper  *EmbeddedEtcdMember
	member   *pdpb.Member
	revision int64
}

// GetListenUrls returns current leader's client urls
func (l *EmbeddedEtcdLeader) GetListenUrls() []string {
	return l.member.GetClientUrls()
}

// GetRevision the revision of the leader in etcd
func (l *EmbeddedEtcdLeader) GetRevision() int64 {
	return l.revision
}

// String declares fmt.Stringer
func (l *EmbeddedEtcdLeader) String() string {
	return l.member.String()
}

// Watch on the leader
func (l *EmbeddedEtcdLeader) Watch(ctx context.Context) {
	l.wrapper.WatchLeader(ctx, l.member, l.revision)
}

// EtcdLeader is the leader in the election group backed by the etcd, but it's
// decoupled from the embedded etcd.
type EtcdLeader struct {
	wrapper     *Participant
	participant participant
	revision    int64
}

// GetListenUrls returns current leader's client urls
func (l *EtcdLeader) GetListenUrls() []string {
	return l.participant.GetListenUrls()
}

// GetRevision the revision of the leader in etcd
func (l *EtcdLeader) GetRevision() int64 {
	return l.revision
}

// String declares fmt.Stringer
func (l *EtcdLeader) String() string {
	return l.participant.String()
}

// Watch on the leader
func (l *EtcdLeader) Watch(ctx context.Context) {
	l.wrapper.WatchLeader(ctx, l.participant, l.revision)
}
