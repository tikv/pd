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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultCampaignTimesSlot  = 10
	watchLoopUnhealthyTimeout = 60 * time.Second
)

var campaignTimesRecordTimeout = 5 * time.Minute

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
	keepAliveCancelFuncLock syncutil.Mutex
	// campaignTimes is used to record the campaign times of the leader within `campaignTimesRecordTimeout`.
	// It is ordered by time to prevent the leader from campaigning too frequently.
	campaignTimes []time.Time
}

// NewLeadership creates a new Leadership.
func NewLeadership(client *clientv3.Client, leaderKey, purpose string) *Leadership {
	leadership := &Leadership{
		purpose:       purpose,
		client:        client,
		leaderKey:     leaderKey,
		campaignTimes: make([]time.Time, 0, defaultCampaignTimesSlot),
	}
	return leadership
}

// getLease gets the lease of leadership, only if leadership is valid,
// i.e. the owner is a true leader, the lease is not nil.
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

// GetCampaignTimesNum is used to get the campaign times of the leader within `campaignTimesRecordTimeout`.
// Need to make sure `AddCampaignTimes` is called before this function.
func (ls *Leadership) GetCampaignTimesNum() int {
	if ls == nil {
		return 0
	}
	return len(ls.campaignTimes)
}

// ResetCampaignTimes is used to reset the campaign times of the leader.
func (ls *Leadership) ResetCampaignTimes() {
	if ls == nil {
		return
	}
	ls.campaignTimes = make([]time.Time, 0, defaultCampaignTimesSlot)
}

// AddCampaignTimes is used to add the campaign times of the leader.
func (ls *Leadership) AddCampaignTimes() {
	if ls == nil {
		return
	}
	for i := len(ls.campaignTimes) - 1; i >= 0; i-- {
		if time.Since(ls.campaignTimes[i]) > campaignTimesRecordTimeout {
			// remove the time which is more than `campaignTimesRecordTimeout`
			// array is sorted by time
			ls.campaignTimes = ls.campaignTimes[i+1:]
			break
		}
	}

	ls.campaignTimes = append(ls.campaignTimes, time.Now())
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

	failpoint.Inject("skipGrantLeader", func(val failpoint.Value) {
		var member pdpb.Member
		_ = member.Unmarshal([]byte(leaderData))
		name, ok := val.(string)
		if ok && member.Name == name {
			failpoint.Return(errors.Errorf("failed to grant lease"))
		}
	})

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
	log.Info("write leaderData to leaderPath ok", zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
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
	log.Info("delete the leader key ok", zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
	return nil
}

// Watch is used to watch the changes of the leadership, usually is used to
// detect the leadership stepping down and restart an election as soon as possible.
func (ls *Leadership) Watch(serverCtx context.Context, revision int64) {
	if ls == nil {
		return
	}

	var (
		watcher       clientv3.Watcher
		watcherCancel context.CancelFunc
	)
	defer func() {
		if watcherCancel != nil {
			watcherCancel()
		}
		if watcher != nil {
			watcher.Close()
		}
	}()

	unhealthyTimeout := watchLoopUnhealthyTimeout
	failpoint.Inject("fastTick", func() {
		unhealthyTimeout = 5 * time.Second
	})
	ticker := time.NewTicker(etcdutil.RequestProgressInterval)
	defer ticker.Stop()
	lastReceivedResponseTime := time.Now()

	for {
		failpoint.Inject("delayWatcher", nil)

		// When etcd is not available, the watcher.Watch will block,
		// so we check the etcd availability first.
		if !etcdutil.IsHealthy(serverCtx, ls.client) {
			if time.Since(lastReceivedResponseTime) > unhealthyTimeout {
				log.Error("the connection is unhealthy for a while, exit leader watch loop",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				return
			}
			log.Warn("the connection maybe unhealthy, retry to watch later",
				zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
			select {
			case <-serverCtx.Done():
				log.Info("server is closed, exit leader watch loop",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				return
			case <-ticker.C:
				continue // continue to check the etcd availability
			}
		}

		if watcherCancel != nil {
			watcherCancel()
		}
		if watcher != nil {
			watcher.Close()
		}
		watcher = clientv3.NewWatcher(ls.client)
		// In order to prevent a watch stream being stuck in a partitioned node,
		// make sure to wrap context with "WithRequireLeader".
		watcherCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(serverCtx))
		watcherCancel = cancel

		done := make(chan struct{})
		go grpcutil.CheckStream(watcherCtx, watcherCancel, done)
		watchChan := watcher.Watch(watcherCtx, ls.leaderKey,
			clientv3.WithRev(revision), clientv3.WithProgressNotify())
		done <- struct{}{}
		if err := watcherCtx.Err(); err != nil {
			log.Warn("error occurred while creating watch channel and retry it later in watch loop", zap.Error(err),
				zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
			select {
			case <-serverCtx.Done():
				log.Info("server is closed, exit leader watch loop",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				return
			case <-ticker.C:
				continue
			}
		}
		lastReceivedResponseTime = time.Now()
		log.Info("watch channel is created", zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))

	watchChanLoop:
		select {
		case <-serverCtx.Done():
			log.Info("server is closed, exit leader watch loop",
				zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
			return
		case <-ticker.C:
			// When etcd is not available, the watcher.RequestProgress will block,
			// so we check the etcd availability first.
			if !etcdutil.IsHealthy(serverCtx, ls.client) {
				log.Warn("the connection maybe unhealthy, retry to watch later",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				continue
			}
			// We need to request progress to etcd to prevent etcd hold the watchChan,
			// note: the ctx must be from watcherCtx, otherwise, the RequestProgress request cannot be sent properly.
			ctx, cancel := context.WithTimeout(watcherCtx, etcdutil.DefaultRequestTimeout)
			if err := watcher.RequestProgress(ctx); err != nil {
				log.Warn("failed to request progress in leader watch loop",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose), zap.Error(err))
			}
			cancel()
			// If no message comes from an etcd watchChan for WatchChTimeoutDuration,
			// create a new one and need not to reset lastReceivedResponseTime.
			if time.Since(lastReceivedResponseTime) >= etcdutil.WatchChTimeoutDuration {
				log.Warn("watch channel is blocked for a long time, recreating a new one",
					zap.Duration("timeout", time.Since(lastReceivedResponseTime)),
					zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				continue
			}
		case wresp := <-watchChan:
			failpoint.Inject("watchChanBlock", func() {
				// watchChanBlock is used to simulate the case that the watchChan is blocked for a long time.
				// So we discard these responses when the failpoint is injected.
				failpoint.Goto("watchChanLoop")
			})
			lastReceivedResponseTime = time.Now()
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision), zap.Int64("compact-revision", wresp.CompactRevision),
					zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				revision = wresp.CompactRevision
				continue
			} else if err := wresp.Err(); err != nil { // wresp.Err() contains CompactRevision not equal to 0
				log.Error("leadership watcher is canceled with", errs.ZapError(errs.ErrEtcdWatcherCancel, err),
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				return
			} else if wresp.IsProgressNotify() {
				log.Debug("watcher receives progress notify in watch loop",
					zap.Int64("revision", revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
				goto watchChanLoop
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("current leadership is deleted",
						zap.Int64("revision", wresp.Header.Revision), zap.String("leader-key", ls.leaderKey), zap.String("purpose", ls.purpose))
					return
				}
			}
			revision = wresp.Header.Revision + 1
		}
		goto watchChanLoop // Use goto to avoid creating a new watchChan
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
