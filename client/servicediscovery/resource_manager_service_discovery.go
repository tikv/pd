// Copyright 2025 TiKV Project Authors.
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

package servicediscovery

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

const (
	resourceManagerServiceName = "resource_manager"
	// resourceManagerSvcDiscoveryFormat defines the key prefix for keyspace group primary election.
	// The entire key is in the format of "/ms/<cluster-id>/resource-manager/primary".
	resourceManagerSvcDiscoveryFormat = "/ms/%d/" + resourceManagerServiceName + "/primary"
	resourceManagerInitRetryTime      = 3
)

// ResourceManagerDiscovery is used to discover the resource manager service.
type ResourceManagerDiscovery struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	started      atomic.Bool
	clusterID    uint64
	metaCli      metastorage.Client
	discoveryKey string
	tlsCfg       *tls.Config
	// Client option.
	option             *opt.Option
	onLeaderChanged    func(string) error
	updateServiceURLCh chan struct{}

	mu         sync.RWMutex
	serviceURL string
	conn       *grpc.ClientConn
}

// NewResourceManagerDiscovery creates a new ResourceManagerDiscovery.
func NewResourceManagerDiscovery(ctx context.Context, clusterID uint64, metaCli metastorage.Client, tlsCfg *tls.Config, opt *opt.Option, leaderChangedCb func(string) error) *ResourceManagerDiscovery {
	ctx, cancel := context.WithCancel(ctx)
	d := &ResourceManagerDiscovery{
		ctx:                ctx,
		cancel:             cancel,
		clusterID:          clusterID,
		metaCli:            metaCli,
		discoveryKey:       fmt.Sprintf(resourceManagerSvcDiscoveryFormat, clusterID),
		tlsCfg:             tlsCfg,
		option:             opt,
		onLeaderChanged:    leaderChangedCb,
		updateServiceURLCh: make(chan struct{}, 1),
	}

	log.Info("[resource-manager] created resource manager discovery",
		zap.Uint64("cluster-id", clusterID),
		zap.String("discovery-key", d.discoveryKey))
	return d
}

// Init implements ServiceDiscovery.
func (r *ResourceManagerDiscovery) Init() {
	if !r.started.CompareAndSwap(false, true) {
		return
	}
	r.wg.Add(1)
	go r.initAndUpdateLoop()
}

func (r *ResourceManagerDiscovery) initAndUpdateLoop() {
	defer r.wg.Done()
	log.Info("[resource-manager] initializing service discovery",
		zap.Int("max-retry-times", resourceManagerInitRetryTime),
		zap.Duration("retry-interval", initRetryInterval))

	var (
		url      string
		revision int64
		err      error
	)
	if err := retry.Retry(r.ctx, resourceManagerInitRetryTime, initRetryInterval, func() error {
		url, revision, err = r.discoverServiceURL()
		return err
	}); err != nil {
		log.Error("[resource-manager] failed to discover service. initialization failed.", zap.Error(err))
	}
	r.resetConn(url)
	r.updateServiceURLLoop(revision)
}

func (r *ResourceManagerDiscovery) resetConn(url string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if url == "" {
		if r.conn != nil {
			r.conn.Close()
			r.conn = nil
		}
		r.serviceURL = ""
		_ = r.onLeaderChanged("")
		return
	}
	if r.serviceURL == url {
		return
	}
	newConn, err := grpcutil.GetClientConn(r.ctx, url, r.tlsCfg, r.option.GRPCDialOptions...)
	if err != nil {
		// Dial without `WithBlock`, normally it should not fail.
		log.Error("[resource-manager] failed to create gRPC connection",
			zap.String("url", url),
			zap.Error(err))
		return
	}
	if r.conn != nil {
		r.conn.Close()
	}
	r.serviceURL, r.conn = url, newConn
	_ = r.onLeaderChanged("")
}

func (r *ResourceManagerDiscovery) getServiceURL() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.serviceURL
}

// GetServiceURL returns the currently discovered resource manager service URL.
// It returns an empty string when there is no standalone service endpoint and
// the client should fall back to PD-provided resource manager.
func (r *ResourceManagerDiscovery) GetServiceURL() string {
	return r.getServiceURL()
}

// GetConn returns the gRPC connection to the resource manager service.
func (r *ResourceManagerDiscovery) GetConn() *grpc.ClientConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.conn == nil {
		log.Warn("[resource-manager] gRPC connection is not established yet",
			zap.String("discovery-key", r.discoveryKey))
		return nil
	}
	return r.conn
}

func (r *ResourceManagerDiscovery) discoverServiceURL() (string, int64, error) {
	resp, err := r.metaCli.Get(r.ctx, []byte(r.discoveryKey))
	if err != nil {
		log.Error("[resource-manager] failed to get resource-manager serving endpoint",
			zap.String("discovery-key", r.discoveryKey),
			zap.Error(err))
		return "", 0, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.Warn("[resource-manager] no resource-manager serving endpoint found",
			zap.String("discovery-key", r.discoveryKey))
		return "", 0, errs.ErrClientGetServingEndpoint
	} else if resp.Count > 1 {
		return "", 0, errs.ErrClientGetMultiResponse.FastGenByArgs(resp.Kvs)
	}

	url, err := r.parseURLFromStorageValue(resp.Kvs[0].Value)
	if err != nil {
		return "", 0, err
	}

	return url, resp.Header.Revision, nil
}

func (r *ResourceManagerDiscovery) parseURLFromStorageValue(value []byte) (string, error) {
	primary := &resource_manager.Participant{}
	if err := proto.Unmarshal(value, primary); err != nil {
		return "", errs.ErrClientProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	listenUrls := primary.GetListenUrls()
	if len(listenUrls) == 0 {
		log.Warn("[resource-manager] the keyspace serving endpoint list is empty",
			zap.String("discovery-key", r.discoveryKey))
		return "", errs.ErrClientGetServingEndpoint
	}
	// TODO: only support 1 listen url for now.
	return listenUrls[0], nil
}

// ScheduleUpateServiceURL schedules an update of the service URL.
func (r *ResourceManagerDiscovery) ScheduleUpateServiceURL() {
	select {
	case r.updateServiceURLCh <- struct{}{}:
	default:
	}
}

func (r *ResourceManagerDiscovery) updateServiceURLLoop(revision int64) {
	// If no service URL exists (e.g. running in PD-provided mode), we still need to
	// periodically check whether a standalone resource-manager appears later.
	// This enables runtime switching between deployment modes.
	ticker := time.NewTicker(initRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			log.Info("[resource-manager] exit update service URL loop due to context canceled")
			return
		case <-ticker.C:
			if r.getServiceURL() != "" {
				continue
			}
			url, newRevision, err := r.discoverServiceURL()
			if err != nil {
				// Endpoint is absent; keep connection cleared so the client can fall back to PD.
				if errors.ErrorEqual(err, errs.ErrClientGetServingEndpoint) {
					r.resetConn("")
				}
				continue
			}
			if newRevision > revision {
				r.resetConn(url)
				revision = newRevision
			}
		case <-r.updateServiceURLCh:
			log.Info("[resource-manager] updating service URL", zap.String("old-url", r.serviceURL))
			url, newRevision, err := r.discoverServiceURL()
			if err != nil {
				log.Warn("[resource-manager] failed to discover service URL",
					zap.String("discovery-key", r.discoveryKey),
					zap.Error(err))
				// Endpoint is absent; clear existing connection so the client can fall back to PD.
				if errors.ErrorEqual(err, errs.ErrClientGetServingEndpoint) {
					r.resetConn("")
				}
				continue
			}
			log.Info("[resource-manager] updated service URL",
				zap.String("new-url", url),
				zap.Int64("new-revision", newRevision),
				zap.Int64("revision", revision))
			if newRevision > revision {
				r.resetConn(url)
				revision = newRevision
			}
		}
	}
}

// Close closes the resource manager discovery and its gRPC connection.
func (r *ResourceManagerDiscovery) Close() {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}
