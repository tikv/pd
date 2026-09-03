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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// DefaultLeaseInSeconds is the default lease time in seconds.
const DefaultLeaseInSeconds = 5

// registerRetryInterval is the interval to retry the registration when the
// registry key is occupied by a stale entry that has not expired yet.
const registerRetryInterval = time.Second

// registerRetryMargin is added on top of a lease's TTL when computing the
// retry deadline, to cover a single etcd leader change extending the lease's
// expiry beyond its granted TTL (see (*lessor).Promote and Lease.refresh in
// etcd's lease package) that happens after we last measured it.
const registerRetryMargin = 5 * time.Second

// errServiceAddrOccupied indicates that the registry key of the advertised
// address is already claimed by another live instance.
var errServiceAddrOccupied = errors.New("service registry key is occupied by another live instance")

// ServiceRegister is used to register the service to etcd.
type ServiceRegister struct {
	ctx    context.Context
	cancel context.CancelFunc
	cli    *clientv3.Client
	key    string
	value  string
	ttl    int64
	// leaseID is the lease this instance most recently registered the key
	// with, used to prove ownership of an existing entry on re-registration.
	// Zero (clientv3.NoLease) until the first successful put, so a freshly
	// started process can never match an existing key's lease by accident.
	leaseID clientv3.LeaseID
	// contendedLease and retryDeadline cache a one-time measurement of the
	// lease currently occupying the key while Register is retrying: the
	// first time a given lease ID is observed as occupying it, its actual
	// GrantedTTL (which already reflects etcd's minimum-lease-TTL floor) is
	// used to compute how long it could still take to expire, instead of
	// guessing. They are deliberately not refreshed on every retry against
	// the same lease ID, so a lease kept alive by a genuinely live owner
	// does not push the deadline out indefinitely.
	contendedLease clientv3.LeaseID
	retryDeadline  time.Time
}

// NewServiceRegister creates a new ServiceRegister.
func NewServiceRegister(ctx context.Context, cli *clientv3.Client, serviceName, serviceAddr, serializedValue string, ttl int64) *ServiceRegister {
	cctx, cancel := context.WithCancel(ctx)
	serviceKey := keypath.RegistryPath(serviceName, serviceAddr)
	return &ServiceRegister{
		ctx:    cctx,
		cancel: cancel,
		cli:    cli,
		key:    serviceKey,
		value:  serializedValue,
		ttl:    ttl,
	}
}

// Register registers the service to etcd.
func (sr *ServiceRegister) Register() error {
	var (
		id  clientv3.LeaseID
		err error
	)
	// A stale registry entry left by a crashed instance with the same advertised
	// address will be removed automatically once its lease expires, so retry
	// within the lease TTL before giving up. This starting deadline is a
	// fallback for before putWithTTL has measured the actual contending
	// lease; once it has, sr.retryDeadline (based on that lease's real
	// GrantedTTL) takes over if it implies a later deadline.
	deadline := time.Now().Add(time.Duration(sr.ttl)*time.Second + registerRetryMargin)
	for {
		id, err = sr.putWithTTL()
		if err == nil || !errors.Is(err, errServiceAddrOccupied) {
			break
		}
		if sr.retryDeadline.After(deadline) {
			deadline = sr.retryDeadline
		}
		if time.Now().After(deadline) {
			break
		}
		log.Warn("the service registry key is occupied, retrying",
			zap.String("key", sr.key), zap.Error(err))
		select {
		case <-sr.ctx.Done():
			sr.cancel()
			return fmt.Errorf("register the key %s canceled: %w", sr.key, sr.ctx.Err())
		case <-time.After(registerRetryInterval):
		}
	}
	if err != nil {
		sr.cancel()
		return fmt.Errorf("put the key with lease %s failed: %v", sr.key, err)
	}
	kresp, err := sr.cli.KeepAlive(sr.ctx, id)
	if err != nil {
		sr.cancel()
		return fmt.Errorf("keepalive failed: %v", err)
	}
	go func() {
		defer logutil.LogPanic()
		for {
			select {
			case <-sr.ctx.Done():
				log.Info("exit register process", zap.String("key", sr.key))
				return
			case _, ok := <-kresp:
				if !ok {
					log.Error("keep alive failed", zap.String("key", sr.key))
					kresp = sr.renewKeepalive()
				}
			}
		}
	}()

	return nil
}

func (sr *ServiceRegister) renewKeepalive() <-chan *clientv3.LeaseKeepAliveResponse {
	t := time.NewTicker(time.Duration(sr.ttl) * time.Second / 2)
	defer t.Stop()
	for {
		select {
		case <-sr.ctx.Done():
			log.Info("exit register process", zap.String("key", sr.key))
			return nil
		case <-t.C:
			id, err := sr.putWithTTL()
			if err != nil {
				log.Error("put the key with lease failed", zap.String("key", sr.key), zap.Error(err))
				continue
			}
			kresp, err := sr.cli.KeepAlive(sr.ctx, id)
			if err != nil {
				log.Error("client keep alive failed", zap.String("key", sr.key), zap.Error(err))
				continue
			}
			return kresp
		}
	}
}

// putWithTTL claims the registry key with a new lease. To prevent an instance
// that advertises a duplicate address from overwriting the registry entry of
// another live instance (and further joining the primary election with the
// same identity), the key is only claimed when it does not exist yet, or when
// it is still backed by the lease this instance previously registered it
// with.
func (sr *ServiceRegister) putWithTTL() (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(sr.ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	grantResp, err := sr.cli.Grant(ctx, sr.ttl)
	if err != nil {
		return 0, err
	}
	leaseID := grantResp.ID
	put := clientv3.OpPut(sr.key, sr.value, clientv3.WithLease(leaseID))
	resp, err := sr.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(sr.key), "=", 0)).
		Then(put).
		Else(clientv3.OpGet(sr.key)).
		Commit()
	if err != nil {
		sr.revokeLease(ctx, leaseID)
		return 0, err
	}
	if resp.Succeeded {
		sr.leaseID = leaseID
		return leaseID, nil
	}
	// The key already exists. Its value alone cannot prove it is this
	// instance's own prior registration: ServiceRegistryEntry.StartTimestamp
	// only has second precision, so two distinct instances started within
	// the same second at the same address can serialize identically. Only
	// take the key over when it is still backed by the lease this instance
	// itself previously registered; otherwise treat it as claimed by another
	// live instance (or a not-yet-expired entry from a prior process) and
	// let the caller's retry loop wait for it to expire.
	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 || sr.leaseID == clientv3.NoLease || clientv3.LeaseID(kvs[0].Lease) != sr.leaseID {
		existingValue := ""
		if len(kvs) > 0 {
			existingValue = string(kvs[0].Value)
			sr.observeContendedLease(ctx, clientv3.LeaseID(kvs[0].Lease))
		}
		sr.revokeLease(ctx, leaseID)
		return 0, fmt.Errorf("key %s, existing value %s: %w", sr.key, existingValue, errServiceAddrOccupied)
	}
	// Re-registering after a keepalive failure while the previous lease has
	// not expired yet: take it over with the new lease.
	takeoverResp, err := sr.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(sr.key), "=", sr.leaseID)).
		Then(put).
		Commit()
	if err != nil {
		sr.revokeLease(ctx, leaseID)
		return 0, err
	}
	if !takeoverResp.Succeeded {
		// The key changed in between, let the caller retry.
		sr.revokeLease(ctx, leaseID)
		return 0, fmt.Errorf("key %s changed during the takeover: %w", sr.key, errServiceAddrOccupied)
	}
	sr.leaseID = leaseID
	return leaseID, nil
}

// observeContendedLease records how long the lease currently occupying the
// key could still take to expire, the first time this specific lease ID is
// observed as occupying it. It deliberately does not re-measure on every
// call against the same lease ID, so a lease kept alive by a genuinely live
// owner does not push the retry deadline out indefinitely; only a change in
// which lease is occupying the key (a new registration event) triggers a
// fresh measurement. It uses GrantedTTL rather than the live, continuously
// renewed TTL: GrantedTTL is fixed for the life of the lease and already
// reflects etcd's own minimum-lease-TTL floor, so deriving the deadline from
// it is correct for any configured election timeout without guessing.
func (sr *ServiceRegister) observeContendedLease(ctx context.Context, existingLease clientv3.LeaseID) {
	if existingLease == clientv3.NoLease || existingLease == sr.contendedLease {
		return
	}
	ttlResp, err := sr.cli.TimeToLive(ctx, existingLease)
	if err != nil || ttlResp.GrantedTTL <= 0 {
		return
	}
	sr.contendedLease = existingLease
	sr.retryDeadline = time.Now().Add(time.Duration(ttlResp.GrantedTTL)*time.Second + registerRetryMargin)
}

// revokeLease revokes the lease in a best-effort manner to avoid leaking it
// when the registration fails.
func (sr *ServiceRegister) revokeLease(ctx context.Context, leaseID clientv3.LeaseID) {
	if _, err := sr.cli.Revoke(ctx, leaseID); err != nil {
		log.Warn("revoke the lease failed", zap.String("key", sr.key), zap.Error(err))
	}
}

// Deregister deregisters the service from etcd.
func (sr *ServiceRegister) Deregister() error {
	sr.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sr.ttl)*time.Second)
	defer cancel()
	_, err := sr.cli.Delete(ctx, sr.key)
	return err
}
