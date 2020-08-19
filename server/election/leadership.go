// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// GetLeader gets the coresponding leader from etcd by given leaderPath (as the key).
func GetLeader(c *clientv3.Client, leaderPath string) (*pdpb.Member, int64, error) {
	leader := &pdpb.Member{}
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(c, leaderPath, leader)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return leader, rev, nil
}

// Leadership is used to manage the leadership campaigning.
type Leadership struct {
	// purpose is used to show what this election for
	Purpose string
	// The lease which is used to get this leadership
	lease  atomic.Value // stored as *lease
	client *clientv3.Client
	// leaderKey and leaderValue are key-value pair in etcd
	leaderKey   string
	leaderValue string
}

// NewLeadership creates a new Leadership.
func NewLeadership(client *clientv3.Client, purpose string) *Leadership {
	leadership := &Leadership{
		Purpose: purpose,
		client:  client,
	}
	return leadership
}

// getLease gets the lease of leadership, only if leadership is valid,
// i.e the owner is a true leader, the lease is not nil.
func (ls *Leadership) getLease() *lease {
	l := ls.lease.Load()
	if l == nil {
		return nil
	}
	return l.(*lease)
}

func (ls *Leadership) setLease(lease *lease) {
	ls.lease.Store(lease)
}

// ResetLease sets the lease of leadership to nil.
func (ls *Leadership) resetLease() {
	ls.setLease(nil)
}

// Campaign is used to campaign the leader with given lease and returns a leadership
func (ls *Leadership) Campaign(leaseTimeout int64, leaderPath, leaderData string) error {
	ls.leaderKey = leaderPath
	ls.leaderValue = leaderData
	// Create a new lease to campaign
	ls.setLease(&lease{
		Purpose: ls.Purpose,
		client:  ls.client,
		lease:   clientv3.NewLease(ls.client),
	})
	if err := ls.getLease().Grant(leaseTimeout); err != nil {
		return err
	}
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := kv.NewSlowLogTxn(ls.client).
		If(clientv3.Compare(clientv3.CreateRevision(leaderPath), "=", 0)).
		Then(clientv3.OpPut(leaderPath, leaderData, clientv3.WithLease(ls.getLease().ID))).
		Commit()
	log.Info("check campaign resp", zap.Any("resp", resp), zap.Error(err))
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("failed to campaign leader, other server may campaign ok")
	}
	log.Info("write leaderDate to leaderPath ok", zap.String("leaderPath", leaderPath), zap.String("purpose", ls.Purpose))
	return nil
}

// Keep will keep the leadership available by update the lease's expired time continuously
func (ls *Leadership) Keep(ctx context.Context) {
	ls.getLease().KeepAlive(ctx)
}

// Check returns whether the leadership is still avalibale
func (ls *Leadership) Check() bool {
	return ls != nil && ls.getLease() != nil && !ls.getLease().IsExpired()
}

// LeaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (ls *Leadership) LeaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	txn := kv.NewSlowLogTxn(ls.client)
	return txn.If(append(cs, ls.leaderCmp())...)
}

func (ls *Leadership) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(ls.leaderKey), "=", ls.leaderValue)
}

// DeleteLeader deletes the coresponding leader from etcd by given leaderPath (as the key).
func (ls *Leadership) DeleteLeader() error {
	// delete leader itself and let others start a new election again.
	resp, err := ls.LeaderTxn().Then(clientv3.OpDelete(ls.leaderKey)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

// Reset does some defer job such as closing lease, resetting lease etc.
func (ls *Leadership) Reset() {
	ls.getLease().Close()
	ls.resetLease()
}
