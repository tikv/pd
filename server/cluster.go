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
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errClusterNotBootstrapped = errors.New("cluster is not bootstrapped")
)

const (
	maxBatchRegionCount = 10000
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	cachedCluster *clusterInfo

	// balancer worker
	balancerWorker *balancerWorker
}

func (c *RaftCluster) start(meta metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.cachedCluster = newClusterInfo(c.clusterRoot)
	c.cachedCluster.idAlloc = c.s.idAlloc

	c.cachedCluster.setMeta(&meta)

	// Cache all stores when start the cluster. We don't have
	// many stores, so it is OK to cache them all.
	// And we should use these cache for later ChangePeer too.
	if err := c.cacheAllStores(); err != nil {
		return errors.Trace(err)
	}

	if err := c.cacheAllRegions(); err != nil {
		return errors.Trace(err)
	}

	c.balancerWorker = newBalancerWorker(c.cachedCluster, c.s.cfg.BalanceCfg)
	c.balancerWorker.run()

	c.running = true

	return nil
}

func (c *RaftCluster) stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.balancerWorker.stop()

	c.running = false
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

// GetConfig gets config information.
func (s *Server) GetConfig() *Config {
	s.cfgLock.RLock()
	defer s.cfgLock.RUnlock()

	return s.cfg.clone()
}

// SetConfig sets the config information.
func (s *Server) SetConfig(cfg *Config) {
	s.cfgLock.Lock()
	defer s.cfgLock.Unlock()

	s.cfg.setCfg(cfg)
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets raft cluster.
func (s *Server) GetRaftCluster() (*RaftCluster, error) {
	if s.cluster.isRunning() {
		return s.cluster, nil
	}

	return nil, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	value, err := getValue(s.client, s.getClusterRootPath())
	if err != nil {
		return errors.Trace(err)
	}
	if value == nil {
		return nil
	}

	clusterMeta := metapb.Cluster{}
	if err = proto.Unmarshal(value, &clusterMeta); err != nil {
		return errors.Trace(err)
	}

	if err = s.cluster.start(clusterMeta); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return strings.Join([]string{clusterRootPath, "s", fmt.Sprintf("%020d", storeID)}, "/")
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return strings.Join([]string{clusterRootPath, "r", fmt.Sprintf("%020d", regionID)}, "/")
}

func makeStoreKeyPrefix(clusterRootPath string) string {
	return strings.Join([]string{clusterRootPath, "s", ""}, "/")
}

func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	peers := regionMeta.GetPeers()
	if len(peers) != 1 {
		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
	}

	peer := peers[0]
	if peer.GetStoreId() != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
	}
	if peer.GetId() == 0 {
		return errors.New("invalid zero peer id")
	}

	return nil
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.Response, error) {
	clusterID := s.cfg.ClusterID

	log.Infof("try to bootstrap raft cluster %d with %v", clusterID, req)

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, errors.Trace(err)
	}

	clusterMeta := metapb.Cluster{
		Id:           proto.Uint64(clusterID),
		MaxPeerCount: proto.Uint32(uint32(s.cfg.MaxPeerCount)),
	}

	// Set cluster meta
	clusterValue, err := proto.Marshal(&clusterMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterRootPath := s.getClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := proto.Marshal(storeMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := proto.Marshal(req.GetRegion())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := s.Txn().If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Warnf("cluster %d already bootstrapped", clusterID)
		return newBootstrappedError(), nil
	}

	log.Infof("bootstrap cluster %d ok", clusterID)

	if err = s.cluster.start(clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		Bootstrap: &pdpb.BootstrapResponse{},
	}, nil
}

func (c *RaftCluster) cacheAllStores() error {
	start := time.Now()

	key := makeStoreKeyPrefix(c.clusterRoot)
	resp, err := kvGet(c.s.client, key, clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	for _, kv := range resp.Kvs {
		store := &metapb.Store{}
		if err = proto.Unmarshal(kv.Value, store); err != nil {
			return errors.Trace(err)
		}

		c.cachedCluster.addStore(store)
	}
	log.Infof("cache all %d stores cost %s", len(resp.Kvs), time.Now().Sub(start))
	return nil
}

func (c *RaftCluster) cacheAllRegions() error {
	start := time.Now()

	nextID := uint64(0)
	endRegionKey := makeRegionKey(c.clusterRoot, math.MaxUint64)

	c.cachedCluster.regions.Lock()
	defer c.cachedCluster.regions.Unlock()

	for {
		key := makeRegionKey(c.clusterRoot, nextID)
		resp, err := kvGet(c.s.client, key, clientv3.WithRange(endRegionKey))
		if err != nil {
			return errors.Trace(err)
		}

		if len(resp.Kvs) == 0 {
			// No more data
			break
		}

		for _, kv := range resp.Kvs {
			region := &metapb.Region{}
			if err = proto.Unmarshal(kv.Value, region); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			c.cachedCluster.regions.addRegion(region)
		}
	}

	log.Infof("cache all %d regions cost %s", len(c.cachedCluster.regions.regions), time.Now().Sub(start))
	return nil
}

func (c *RaftCluster) getRegion(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	return c.cachedCluster.regions.getRegion(regionKey)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	return c.cachedCluster.regions.getRegionByID(regionID)
}

// GetRegions gets regions from cluster.
func (c *RaftCluster) GetRegions() []*metapb.Region {
	return c.cachedCluster.regions.getRegions()
}

// GetStores gets stores from cluster.
func (c *RaftCluster) GetStores() []*metapb.Store {
	return c.cachedCluster.getMetaStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) (*metapb.Store, *StoreStatus, error) {
	if storeID == 0 {
		return nil, nil, errors.New("invalid zero store id")
	}

	store := c.cachedCluster.getStore(storeID)
	if store == nil {
		return nil, nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}

	return store.store, store.stats, nil
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	storeValue, err := proto.Marshal(store)
	if err != nil {
		return errors.Trace(err)
	}

	storePath := makeStoreKey(c.clusterRoot, store.GetId())
	resp, err := c.s.LeaderTxn().Then(clientv3.OpPut(storePath, string(storeValue))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("put store %v fail", store)
	}

	c.cachedCluster.addStore(store)

	return nil
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	return c.cachedCluster.getMeta()
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}

	metaValue, err := proto.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := c.s.LeaderTxn().Then(clientv3.OpPut(c.clusterRoot, string(metaValue))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("put cluster meta %v error", meta)
	}

	c.cachedCluster.setMeta(meta)

	return nil
}

// GetBalanceOperators gets the balance operators from cluster.
func (c *RaftCluster) GetBalanceOperators() map[uint64]Operator {
	return c.balancerWorker.getBalanceOperators()
}

// GetHistoryOperators gets the history operators from cluster.
func (c *RaftCluster) GetHistoryOperators() []Operator {
	return c.balancerWorker.getHistoryOperators()
}

// GetScores gets store scores from balancer.
func (c *RaftCluster) GetScores(store *metapb.Store, status *StoreStatus) []int {
	storeInfo := &storeInfo{
		store: store,
		stats: status,
	}

	return c.balancerWorker.storeScores(storeInfo)
}

// FetchEvents fetches the operator events.
func (c *RaftCluster) FetchEvents(key uint64, all bool) []LogEvent {
	return c.balancerWorker.fetchEvents(key, all)
}
