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
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

const (
	requestTimeout  = time.Second * 10
	slowRequestTime = time.Second * 1
)

const (
	successLabel = "success"
	failureLabel = "failure"
)

var (
	errNotLeader = errors.New("server is not leader")
	errTxnFailed = errors.New("transaction precondition is not satisfied")
)

type slowLogTxn struct {
	client  *clientv3.Client
	compare []clientv3.Cmp
	success []clientv3.Op
	failure []clientv3.Op
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	return &slowLogTxn{client: client}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.compare = append(t.compare, cs...)
	return t
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.success = append(t.success, ops...)
	return t
}

func (t *slowLogTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.failure = append(t.failure, ops...)
	return t
}

func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	ctx, cancel := context.WithTimeout(t.client.Ctx(), requestTimeout)
	defer cancel()

	txn := t.client.Txn(ctx)
	if len(t.compare) > 0 {
		txn = txn.If(t.compare...)
	}
	if len(t.success) > 0 {
		txn = txn.Then(t.success...)
	}
	if len(t.failure) > 0 {
		txn = txn.Else(t.failure...)
	}

	start := time.Now()
	resp, err := txn.Commit()
	cost := time.Since(start)
	if cost > slowRequestTime {
		log.Warnf("txn runs too slow: cost %v err %v", cost, err)
	}

	label := successLabel
	if err != nil {
		label = failureLabel
	}
	txnCounter.WithLabelValues(label).Inc()
	txnDuration.WithLabelValues(label).Observe(cost.Seconds())

	if err != nil {
		return nil, errors.Trace(err)
	} else if !resp.Succeeded {
		return nil, errors.Trace(errTxnFailed)
	}
	return resp, nil
}

type notLeaderTxn struct {
	clientv3.Txn
}

func newNotLeaderTxn(client *clientv3.Client) clientv3.Txn {
	return &notLeaderTxn{
		Txn: client.Txn(client.Ctx()),
	}
}

func (t *notLeaderTxn) Commit() (*clientv3.TxnResponse, error) {
	return nil, errNotLeader
}
