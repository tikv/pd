package server

import (
	"bytes"
	"fmt"
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

const (
	askJobChannelSize = 1024
)

var (
	errClusterNotBootstrapped = errors.New("cluster is not bootstrapped")
)

// Raft cluster key format:
// cluster xxx -> /xxx/raft, value is metapb.Cluster
// cluster yyy -> /yyy/raft
// For cluster xxx
// store 1 -> /xxx/raft/s/1, value is metapb.Store
// region 1 -> /xxx/raft/r/1, value is encoded_region_key
// region search key map -> /xxx/raft/k/encoded_region_key, value is metapb.Region
//
// Operation queue, pd can only handle operations like auto-balance, split,
// merge sequentially, and every operation will be assigned a unique incremental ID.
// pending queue -> /xxx/raft/j/1, /xxx/raft/j/2, value is operation job.
//
// Encode region search key:
//  1, the maximum end region key is empty, so the encode key is \xFF
//  2, other region end key is not empty, the encode key is \z end_key

type raftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterName string
	clusterRoot string

	wg sync.WaitGroup

	quitCh chan struct{}

	askJobCh chan struct{}

	mu struct {
		sync.RWMutex
		// TODO: keep meta revision
		// cluster meta cache
		meta metapb.Cluster

		// store cache
		stores map[uint64]metapb.Store
	}

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

	c.askJobCh = make(chan struct{}, askJobChannelSize)
	c.quitCh = make(chan struct{})

	c.storeConns = newStoreConns(defaultConnFunc)
	c.storeConns.SetIdleTimeout(idleTimeout)

	// Force checking the pending job.
	c.askJobCh <- struct{}{}

	mu := &c.mu
	mu.meta = meta

	// Cache all stores when start the cluster. We don't have
	// many stores, so it is OK to cache them all.
	// And we should use these cache for later ChangePeer too.
	if err := c.cacheAllStores(); err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go c.onJobWorker()

	return nil
}

func (c *raftCluster) Stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	close(c.quitCh)
	c.wg.Wait()

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

	// Find in etcd
	value, err := getValue(s.client, s.getClusterRootPath())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}

	clusterMeta := metapb.Cluster{}
	if err = proto.Unmarshal(value, &clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	if err = s.cluster.Start(clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	return s.cluster, nil
}

func encodeRegionSearchKey(endKey []byte) string {
	if len(endKey) == 0 {
		return "\xFF"
	}

	return string(append([]byte{'z'}, endKey...))
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

func makeJobKey(clusterRootPath string, jobID uint64) string {
	// We must guarantee the job handling order, so use %020d to format the job key,
	// use etcd range get to get the first job and then handle it.
	// Should we use a 8 bytes binary BigEndian instead of 20 bytes string?
	return strings.Join([]string{clusterRootPath, "job", fmt.Sprintf("%020d", jobID)}, "/")
}

func makeStoreKeyPrefix(clusterRootPath string) string {
	return strings.Join([]string{clusterRootPath, "s", ""}, "/")
}

func checkBootstrapRequest(clusterName string, req *pdpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %s", clusterName)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %s", clusterName)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %s", clusterName)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	storeIDs := regionMeta.GetStoreIds()
	if len(storeIDs) != 1 {
		return errors.Errorf("invalid first region peer number %d, must be 1 for bootstrap %s", len(storeIDs), clusterName)
	}

	storeID := storeIDs[0]
	if storeID != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %s", storeID, storeMeta.GetId(), clusterName)
	} else if storeID == 0 {
		return errors.New("invalid zero peer store id")
	}

	return nil
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.Response, error) {
	clusterName := s.cfg.ClusterName

	log.Infof("try to bootstrap raft cluster %s with %v", clusterName, req)

	if err := checkBootstrapRequest(clusterName, req); err != nil {
		return nil, errors.Trace(err)
	}

	clusterMeta := metapb.Cluster{
		Name:          proto.String(clusterName),
		MaxPeerNumber: proto.Uint32(s.cfg.MaxPeerNumber),
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
		log.Warnf("cluster %s already bootstrapped", clusterName)
		return NewBootstrappedError(), nil
	}

	log.Infof("bootstrap cluster %s ok", clusterName)

	if err = s.cluster.Start(clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		Bootstrap: &pdpb.BootstrapResponse{},
	}, nil
}

func (c *raftCluster) cacheAllStores() error {
	mu := &c.mu
	mu.Lock()
	defer mu.Unlock()

	kv := clientv3.NewKV(c.s.client)

	key := makeStoreKeyPrefix(c.clusterRoot)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := kv.Get(ctx, key, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	mu.stores = make(map[uint64]metapb.Store)
	for _, kv := range resp.Kvs {
		store := metapb.Store{}
		if err = proto.Unmarshal(kv.Value, &store); err != nil {
			return errors.Trace(err)
		}

		storeID := store.GetId()
		mu.stores[storeID] = store
	}

	return nil
}

func (c *raftCluster) GetAllStores() ([]metapb.Store, error) {
	mu := &c.mu
	mu.RLock()
	defer mu.RUnlock()

	stores := make([]metapb.Store, 0, len(mu.stores))
	for _, store := range mu.stores {
		stores = append(stores, store)
	}

	return stores, nil
}

func (c *raftCluster) GetStore(storeID uint64) (*metapb.Store, error) {
	if storeID == 0 {
		return nil, errors.New("invalid zero store id")
	}

	mu := &c.mu
	mu.RLock()
	defer mu.RUnlock()

	// We cache all stores when start the cluster, and PutStore can also
	// update the cache, so we can use this cache to get directly.

	store, ok := mu.stores[storeID]
	if ok {
		return &store, nil
	}

	return nil, errors.Errorf("invalid store ID %d, not found", storeID)
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
		return nil, errors.Errorf("we must find a region for %q but fail, a serious bug", regionKey)
	}

	if bytes.Compare(regionKey, region.GetStartKey()) >= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(regionKey, region.GetEndKey()) < 0) {
		return &region, nil
	}

	return nil, errors.Errorf("invalid searched region %v for key %q", region, regionKey)
}

func (c *raftCluster) PutStore(store *metapb.Store) error {
	if store == nil || store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	storeValue, err := proto.Marshal(store)
	if err != nil {
		return errors.Trace(err)
	}

	storePath := makeStoreKey(c.clusterRoot, store.GetId())

	mu := &c.mu
	mu.Lock()
	defer mu.Unlock()

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

	mu.stores[store.GetId()] = *store

	return nil
}

func (c *raftCluster) GetConfig() (*metapb.Cluster, error) {
	mu := &c.mu
	mu.RLock()
	defer mu.RUnlock()

	meta := mu.meta
	return &meta, nil
}

func (c *raftCluster) PutConfig(meta *metapb.Cluster) error {
	if meta.GetName() != c.clusterName {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %s", meta, c.clusterName)
	}

	metaValue, err := proto.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	mu := &c.mu
	mu.Lock()
	defer mu.Unlock()

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

	mu.meta = *meta
	return nil
}
