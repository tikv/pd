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
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// DefaultLeaseInSeconds is the default lease time in seconds.
const DefaultLeaseInSeconds = 3

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
func NewServiceRegister(ctx context.Context, cli *clientv3.Client, clusterID, serviceName, serviceAddr, serializedValue string, ttl int64) *ServiceRegister {
	cctx, cancel := context.WithCancel(ctx)
	serviceKey := RegistryPath(clusterID, serviceName, serviceAddr)
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
	id, err := sr.putWithTTL()
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

func (sr *ServiceRegister) putWithTTL() (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(sr.ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	return etcdutil.EtcdKVPutWithTTL(ctx, sr.cli, sr.key, sr.value, sr.ttl)
}

// Deregister deregisters the service from etcd.
func (sr *ServiceRegister) Deregister() error {
	sr.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sr.ttl)*time.Second)
	defer cancel()
	_, err := sr.cli.Delete(ctx, sr.key)
	return err
}
