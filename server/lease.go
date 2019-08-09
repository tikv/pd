// Copyright 2019 PingCAP, Inc.
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

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type LeaderLease struct {
	client *clientv3.Client
	lease  clientv3.Lease
	ID     clientv3.LeaseID
}

func NewLeaderLease(client *clientv3.Client) *LeaderLease {
	return &LeaderLease{
		client: client,
		lease:  clientv3.NewLease(client),
	}
}

func (l *LeaderLease) Grant(seconds int64) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(l.client.Ctx(), requestTimeout)
	leaseResp, err := l.lease.Grant(ctx, seconds)
	cancel()
	if cost := time.Since(start); cost > slowRequestTime {
		log.Warn("lease grants too slow", zap.Duration("cost", cost))
	}
	if err != nil {
		return errors.WithStack(err)
	}
	l.ID = leaseResp.ID
	return nil
}

func (l *LeaderLease) Close() error {
	return l.lease.Close()
}

func (l *LeaderLease) KeepAlive(ctx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return l.lease.KeepAlive(ctx, l.ID)
}