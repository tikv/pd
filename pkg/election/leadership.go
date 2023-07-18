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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	watchLoopUnhealthyTimeout = 60 * time.Second
	detectHealthyInterval     = 10 * time.Second
)

// GetLeader gets the corresponding leader from etcd by given leaderPath (as the key).
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
	purpose string
	// The lease which is used to get this leadership
	lease  atomic.Value // stored as *lease
	client *clientv3.Client
	// leaderKey and leaderValue are key-value pair in etcd
	leaderKey   string
	leaderValue string

	keepAliveCtx            context.Context
	keepAliveCancelFunc     context.CancelFunc
	keepAliveCancelFuncLock sync.Mutex
}

// NewLeadership creates a new Leadership.
func NewLeadership(client *clientv3.Client, leaderKey, purpose string) *Leadership {
	leadership := &Leadership{
		purpose:   purpose,
		client:    client,
		leaderKey: leaderKey,
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

// GetClient is used to get the etcd client.
func (ls *Leadership) GetClient() *clientv3.Client {
	if ls == nil {
		return nil
	}
	return ls.client
}

// GetLeaderKey is used to get the leader key of etcd.
func (ls *Leadership) GetLeaderKey() string {
	if ls == nil {
		return ""
	}
	return ls.leaderKey
}

// Campaign is used to campaign the leader with given lease and returns a leadership
func (ls *Leadership) Campaign(leaseTimeout int64, leaderData string, cmps ...clientv3.Cmp) error {
	ls.leaderValue = leaderData
	// Create a new lease to campaign
	newLease := &lease{
		Purpose: ls.purpose,
		client:  ls.client,
		lease:   clientv3.NewLease(ls.client),
	}
	ls.setLease(newLease)
	if err := newLease.Grant(leaseTimeout); err != nil {
		return err
	}
	finalCmps := make([]clientv3.Cmp, 0, len(cmps)+1)
	finalCmps = append(finalCmps, cmps...)
	// The leader key must not exist, so the CreateRevision is 0.
	finalCmps = append(finalCmps, clientv3.Compare(clientv3.CreateRevision(ls.leaderKey), "=", 0))
	resp, err := kv.NewSlowLogTxn(ls.client).
		If(finalCmps...).
		Then(clientv3.OpPut(ls.leaderKey, leaderData, clientv3.WithLease(newLease.ID.Load().(clientv3.LeaseID)))).
		Commit()
	log.Info("check campaign resp", zap.Any("resp", resp))
	if err != nil {
		newLease.Close()
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		newLease.Close()
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	log.Info("write leaderData to leaderPath ok", zap.String("leaderPath", ls.leaderKey), zap.String("purpose", ls.purpose))
	return nil
}

// Keep will keep the leadership available by update the lease's expired time continuously
func (ls *Leadership) Keep(ctx context.Context) {
	if ls == nil {
		return
	}
	ls.keepAliveCancelFuncLock.Lock()
	ls.keepAliveCtx, ls.keepAliveCancelFunc = context.WithCancel(ctx)
	ls.keepAliveCancelFuncLock.Unlock()
	go ls.getLease().KeepAlive(ls.keepAliveCtx)
}

// Check returns whether the leadership is still available.
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

// DeleteLeaderKey deletes the corresponding leader from etcd by the leaderPath as the key.
func (ls *Leadership) DeleteLeaderKey() error {
	resp, err := kv.NewSlowLogTxn(ls.client).Then(clientv3.OpDelete(ls.leaderKey)).Commit()
	if err != nil {
		return errs.ErrEtcdKVDelete.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	// Reset the lease as soon as possible.
	ls.Reset()
	log.Info("delete the leader key ok", zap.String("leaderPath", ls.leaderKey), zap.String("purpose", ls.purpose))
	return nil
}

// Watch is used to watch the changes of the leadership, usually is used to
// detect the leadership stepping down and restart an election as soon as possible.
func (ls *Leadership) Watch(serverCtx context.Context, revision int64) {
	if ls == nil {
		return
	}

	interval := detectHealthyInterval
	unhealthyTimeout := watchLoopUnhealthyTimeout
	failpoint.Inject("fastTick", func() {
		unhealthyTimeout = 5 * time.Second
		interval = 1 * time.Second
	})
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastHealthyTime := time.Now()

	watcher := clientv3.NewWatcher(ls.client)
	defer watcher.Close()
	var watchChanCancel context.CancelFunc // nolint https://github.com/golang/go/issues/25720
	defer func() {
		if watchChanCancel != nil {
			watchChanCancel()
		}
	}()
	for {
		failpoint.Inject("delayWatcher", nil)
		if watchChanCancel != nil {
			watchChanCancel()
		}
		// In order to prevent a watch stream being stuck in a partitioned node,
		// make sure to wrap context with "WithRequireLeader".
		watchChanCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(serverCtx))
		watchChanCancel = cancel

		// When etcd is not available, the watcher.Watch will block,
		// so we check the etcd availability first.
		if !etcdutil.IsHealthy(serverCtx, ls.client) {
			if time.Since(lastHealthyTime) > unhealthyTimeout {
				log.Error("the connect of leadership watcher is unhealthy",
					zap.Int64("revision", revision),
					zap.String("leader-key", ls.leaderKey),
					zap.String("purpose", ls.purpose))
				return
			}
			select {
			case <-serverCtx.Done():
				// server closed, return
				return
			case <-ticker.C:
				// continue to check the etcd availability
				continue
			}
		}

		watchChan := watcher.Watch(watchChanCtx, ls.leaderKey, clientv3.WithRev(revision))
	WatchChan:
		select {
		case <-serverCtx.Done():
			// server closed, return
			return
		case <-ticker.C:
			if !etcdutil.IsHealthy(serverCtx, ls.client) {
				if time.Since(lastHealthyTime) > unhealthyTimeout {
					log.Error("the connect of leadership watcher is unhealthy",
						zap.Int64("revision", revision),
						zap.String("leader-key", ls.leaderKey),
						zap.String("purpose", ls.purpose))
					return
				}
				goto WatchChan
			}
		case wresp := <-watchChan:
			// meet compacted error, use the compact revision.
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				lastHealthyTime = time.Now()
				continue
			} else if wresp.Err() != nil { // wresp.Err() contains CompactRevision not equal to 0
				log.Error("leadership watcher is canceled with",
					zap.Int64("revision", revision),
					zap.String("leader-key", ls.leaderKey),
					zap.String("purpose", ls.purpose),
					errs.ZapError(errs.ErrEtcdWatcherCancel, wresp.Err()))
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("current leadership is deleted",
						zap.Int64("revision", wresp.Header.Revision),
						zap.String("leader-key", ls.leaderKey),
						zap.String("purpose", ls.purpose))
					return
				}
			}
			revision = wresp.Header.Revision + 1
		}
		lastHealthyTime = time.Now()
		goto WatchChan // use goto to avoid to create a new watchChan
	}
}

// Reset does some defer jobs such as closing lease, resetting lease etc.
func (ls *Leadership) Reset() {
	if ls == nil || ls.getLease() == nil {
		return
	}
	ls.keepAliveCancelFuncLock.Lock()
	if ls.keepAliveCancelFunc != nil {
		ls.keepAliveCancelFunc()
	}
	ls.keepAliveCancelFuncLock.Unlock()
	ls.getLease().Close()
}
