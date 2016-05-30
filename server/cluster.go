package server

import (
	"bytes"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

var (
	errClusterNotBootstrapped = errors.New("cluster is not bootstrapped")
)

// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is encoded_region_key
// region search key map -> /1/raft/k/encoded_region_key, value is metapb.Region
//
// Encode region search key:
//  1, the maximum end region key is empty, so the encode key is \xFF
//  2, other region end key is not empty, the encode key is \z end_key

type raftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	cachedCluster *ClusterInfo

	// for store conns
	storeConns *storeConns
}

func (c *raftCluster) Start(meta metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.running = true

	c.storeConns = newStoreConns(defaultConnFunc)
	c.storeConns.SetIdleTimeout(idleTimeout)

	c.cachedCluster = newClusterInfo()
	c.cachedCluster.setMeta(&meta)

	// Cache all stores when start the cluster. We don't have
	// many stores, so it is OK to cache them all.
	// And we should use these cache for later ChangePeer too.
	if err := c.cacheAllStores(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *raftCluster) Stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.storeConns.Close()

	c.running = false
}

func (c *raftCluster) IsRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

func (s *Server) getRaftCluster() (*raftCluster, error) {
	if s.cluster.IsRunning() {
		return s.cluster, nil
	}

	return nil, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.IsRunning() {
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

	if err = s.cluster.Start(clusterMeta); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func encodeRegionSearchKey(endKey []byte) string {
	if len(endKey) == 0 {
		return "\xFF"
	}

	return string(append([]byte{'z'}, endKey...))
}

func encodeRegionStartKey(startKey []byte) string {
	return string(append([]byte{'z'}, startKey...))
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return strings.Join([]string{clusterRootPath, "s", strconv.FormatUint(storeID, 10)}, "/")
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return strings.Join([]string{clusterRootPath, "r", strconv.FormatUint(regionID, 10)}, "/")
}

func makeRegionSearchKey(clusterRootPath string, endKey []byte) string {
	return strings.Join([]string{clusterRootPath, "k", encodeRegionSearchKey(endKey)}, "/")
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
		MaxPeerCount: proto.Uint32(s.cfg.MaxPeerCount),
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

	// Set region id -> search key
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, encodeRegionSearchKey(req.GetRegion().GetEndKey())))

	// Set region meta with search key
	regionSearchPath := makeRegionSearchKey(clusterRootPath, req.GetRegion().GetEndKey())
	regionValue, err := proto.Marshal(req.GetRegion())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(regionSearchPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := s.client.Txn(ctx).
		If(bootstrapCmp).
		Then(ops...).
		Commit()
	cancel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Warnf("cluster %d already bootstrapped", clusterID)
		return NewBootstrappedError(), nil
	}

	log.Infof("bootstrap cluster %d ok", clusterID)

	if err = s.cluster.Start(clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		Bootstrap: &pdpb.BootstrapResponse{},
	}, nil
}

func (c *raftCluster) cacheAllStores() error {
	kv := clientv3.NewKV(c.s.client)

	key := makeStoreKeyPrefix(c.clusterRoot)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := kv.Get(ctx, key, clientv3.WithPrefix())
	cancel()
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

	return nil
}

func (c *raftCluster) GetAllStores() ([]metapb.Store, error) {
	return c.cachedCluster.getMetaStores(), nil
}

func (c *raftCluster) GetStore(storeID uint64) (*metapb.Store, error) {
	if storeID == 0 {
		return nil, errors.New("invalid zero store id")
	}

	store := c.cachedCluster.getStore(storeID)
	if store == nil {
		return nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}

	return store.store, nil
}

func (c *raftCluster) getRegion(regionKey []byte, maxSearchEndKey []byte) (*metapb.Region, error) {
	// First find region from cache.
	return nil, nil
}

func (c *raftCluster) GetRegion(regionKey []byte) (*metapb.Region, error) {
	// We must use the next region key for search,
	// e,g, we have two regions 1, 2, and key ranges are ["", "abc"), ["abc", +infinite),
	// if we use "abc" to search the region, the first key >= "abc" may be
	// region 1, not region 2. So we use the next region key for search.
	nextRegionKey := append(regionKey, 0x00)
	searchKey := makeRegionSearchKey(c.clusterRoot, nextRegionKey)

	// Etcd range search is for range [searchKey, endKey), if we want to get
	// the latest region, we should use next max end key for search range.
	// TODO: we can generate these keys in initialization.
	maxEndKey := makeRegionSearchKey(c.clusterRoot, []byte{})
	maxSearchEndKey := maxEndKey + "\x00"

	// Find the first region with end key >= searchKey
	region := metapb.Region{}
	ok, err := getProtoMsg(c.s.client, searchKey, &region, clientv3.WithRange(string(maxSearchEndKey)), clientv3.WithLimit(1))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	if bytes.Compare(regionKey, region.GetStartKey()) >= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(regionKey, region.GetEndKey()) < 0) {
		return &region, nil
	}

	return nil, nil
}

func (c *raftCluster) PutStore(store *metapb.Store) error {
	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	storeValue, err := proto.Marshal(store)
	if err != nil {
		return errors.Trace(err)
	}

	storePath := makeStoreKey(c.clusterRoot, store.GetId())
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpPut(storePath, string(storeValue))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("put store %v fail", store)
	}

	c.cachedCluster.addStore(store)

	return nil
}

func (c *raftCluster) GetConfig() (*metapb.Cluster, error) {
	return c.cachedCluster.getMeta(), nil
}

func (c *raftCluster) PutConfig(meta *metapb.Cluster) error {
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}

	metaValue, err := proto.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpPut(c.clusterRoot, string(metaValue))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("put cluster meta %v error", meta)
	}

	c.cachedCluster.setMeta(meta)

	return nil
}
