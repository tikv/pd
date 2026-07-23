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
	// within the lease TTL before giving up.
	deadline := time.Now().Add(time.Duration(sr.ttl+1) * time.Second)
	for {
		id, err = sr.putWithTTL()
		if err == nil || !errors.Is(err, errServiceAddrOccupied) || time.Now().After(deadline) {
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
// it still holds this instance's own value.
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
		return leaseID, nil
	}
	// The key already exists. If it holds a different value, it is claimed by
	// another live instance, so reject the registration instead of overwriting
	// the entry.
	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) > 0 && string(kvs[0].Value) != sr.value {
		sr.revokeLease(ctx, leaseID)
		return 0, fmt.Errorf("key %s, existing value %s: %w", sr.key, string(kvs[0].Value), errServiceAddrOccupied)
	}
	// The key still holds this instance's own value (e.g. re-registering after
	// a keepalive failure while the previous lease has not expired yet), take
	// it over with the new lease.
	takeoverResp, err := sr.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(sr.key), "=", sr.value)).
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
	return leaseID, nil
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
