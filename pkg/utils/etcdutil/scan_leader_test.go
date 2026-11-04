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

package etcdutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type fakeMaintenance struct {
	clientv3.Maintenance
	status func(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

func (f *fakeMaintenance) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return f.status(ctx, endpoint)
}

func newMember(id uint64, urls ...string) *etcdserverpb.Member {
	return &etcdserverpb.Member{ID: id, ClientURLs: urls}
}

func TestScanEtcdLeaderSkipsStalledMember(t *testing.T) {
	re := require.New(t)
	// Like an etcd client waiting for a stopped member, the first member never
	// answers until its request context ends.
	maintenance := &fakeMaintenance{status: func(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
		if endpoint == "stopped" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &clientv3.StatusResponse{Leader: 2}, nil
	}}
	members := []*etcdserverpb.Member{newMember(1, "stopped"), newMember(2, "alive")}

	// The outer context is much longer than the timeout, so the scan can only
	// reach the second member if the first probe is bounded by the timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	var found []uint64
	ScanEtcdLeader(ctx, maintenance, members, 50*time.Millisecond,
		func(member *etcdserverpb.Member, _ *clientv3.StatusResponse) bool {
			found = append(found, member.ID)
			return true
		})
	re.Equal([]uint64{2}, found)
	re.Less(time.Since(start), 5*time.Second)
}

func TestScanEtcdLeaderSkipsUnusableMembers(t *testing.T) {
	re := require.New(t)
	maintenance := &fakeMaintenance{status: func(_ context.Context, endpoint string) (*clientv3.StatusResponse, error) {
		if endpoint == "broken" {
			return nil, errors.New("unreachable")
		}
		return &clientv3.StatusResponse{Leader: 4}, nil
	}}
	members := []*etcdserverpb.Member{
		newMember(1), // not started yet
		newMember(2, "broken"),
		newMember(3, "follower"),
		newMember(4, "leader"),
	}

	var found []uint64
	ScanEtcdLeader(t.Context(), maintenance, members, time.Second,
		func(member *etcdserverpb.Member, status *clientv3.StatusResponse) bool {
			found = append(found, member.ID)
			re.Equal(member.ID, status.Leader)
			return true
		})
	re.Equal([]uint64{4}, found)
}

func TestScanEtcdLeaderCallbackControlsScan(t *testing.T) {
	re := require.New(t)
	var probed []string
	maintenance := &fakeMaintenance{status: func(_ context.Context, endpoint string) (*clientv3.StatusResponse, error) {
		probed = append(probed, endpoint)
		// Both members claim to be the leader, as a stale old leader may.
		if endpoint == "a" {
			return &clientv3.StatusResponse{Leader: 1}, nil
		}
		return &clientv3.StatusResponse{Leader: 2}, nil
	}}
	members := []*etcdserverpb.Member{newMember(1, "a"), newMember(2, "b")}

	var found []uint64
	ScanEtcdLeader(t.Context(), maintenance, members, time.Second,
		func(member *etcdserverpb.Member, _ *clientv3.StatusResponse) bool {
			found = append(found, member.ID)
			return member.ID == 2
		})
	re.Equal([]uint64{1, 2}, found)

	probed, found = nil, nil
	ScanEtcdLeader(t.Context(), maintenance, members, time.Second,
		func(member *etcdserverpb.Member, _ *clientv3.StatusResponse) bool {
			found = append(found, member.ID)
			return true
		})
	re.Equal([]uint64{1}, found)
	re.Equal([]string{"a"}, probed)
}
