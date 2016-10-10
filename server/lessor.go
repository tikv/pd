// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"github.com/coreos/etcd/clientv3"
	concurv3 "github.com/coreos/etcd/clientv3/concurrency"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

var (
	errNoLeader = errors.New("no leader elected")
)

// Lessor is a wrapper for leader election.
type Lessor struct {
	session  *concurv3.Session
	election *concurv3.Election
	compare  clientv3.Cmp
}

// NewLessor returns a new Lessor.
func NewLessor(client *clientv3.Client, ttl int, prefix string) (*Lessor, error) {
	session, err := concurv3.NewSession(client, concurv3.WithTTL(ttl))
	if err != nil {
		return nil, errors.Trace(err)
	}

	election := concurv3.NewElection(session, prefix)

	lessor := &Lessor{
		session:  session,
		election: election,
	}
	return lessor, nil
}

// Close closes the lessor.
func (l *Lessor) Close() {
	l.Resign()
	l.session.Close()
}

func (l *Lessor) ctx() context.Context {
	return l.session.Client().Ctx()
}

// Campaign blocks until it is elected, or an error occurs.
// It puts the leader as eligible for the election.
func (l *Lessor) Campaign(leader *pdpb.Leader) error {
	value, err := leader.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	err = l.election.Campaign(l.ctx(), string(value))
	if err != nil {
		return errors.Trace(err)
	}

	l.compare = clientv3.Compare(clientv3.Value(l.election.Key()), "=", string(value))
	return nil
}

// Resign resigns the leadership.
func (l *Lessor) Resign() error {
	ctx, cancel := context.WithTimeout(l.ctx(), requestTimeout)
	defer cancel()
	return l.election.Resign(ctx)
}

// Done returns a channel that closes when the lease is invalid.
func (l *Lessor) Done() <-chan struct{} {
	return l.session.Done()
}

// Txn returns a transaction wrapper. It guarantees that the
// transaction will be executed only when the lease is valid.
func (l *Lessor) Txn() clientv3.Txn {
	txn := newSlowLogTxn(l.session.Client())
	return txn.If(l.compare)
}

// GetLeader returns the leader for the current election.
func GetLeader(client *clientv3.Client, prefix string) (*pdpb.Leader, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	defer cancel()

	resp, err := client.Get(ctx, prefix, clientv3.WithFirstCreate()...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.Trace(errNoLeader)
	}

	leader := &pdpb.Leader{}
	if err := leader.Unmarshal(resp.Kvs[0].Value); err != nil {
		return nil, errors.Trace(err)
	}
	return leader, nil
}
