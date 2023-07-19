// Copyright 2016 TiKV Project Authors.
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

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

const (
	// defaultEtcdClientTimeout is the default timeout for etcd client.
	defaultEtcdClientTimeout = 3 * time.Second

	// defaultDialKeepAliveTime is the time after which client pings the server to see if transport is alive.
	defaultDialKeepAliveTime = 10 * time.Second

	// defaultDialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	defaultDialKeepAliveTimeout = 3 * time.Second

	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = time.Second

	healthyPath = "health"
)

// CheckClusterID checks etcd cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID types.ID, um types.URLsMap, tlsConfig *tls.Config) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for _, u := range peerURLs {
		trp := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers(nil, []string{u}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Error("failed to get cluster from remote", errs.ZapError(errs.ErrEtcdGetCluster, gerr))
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}
	}
	return nil
}

// AddEtcdMember adds an etcd member.
func AddEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	addResp, err := client.MemberAdd(ctx, urls)
	cancel()
	return addResp, errors.WithStack(err)
}

// ListEtcdMembers returns a list of internal etcd members.
func ListEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	listResp, err := client.MemberList(ctx)
	cancel()
	if err != nil {
		return listResp, errs.ErrEtcdMemberList.Wrap(err).GenWithStackByCause()
	}
	return listResp, nil
}

// RemoveEtcdMember removes a member by the given id.
func RemoveEtcdMember(client *clientv3.Client, id uint64) (*clientv3.MemberRemoveResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	rmResp, err := client.MemberRemove(ctx, id)
	cancel()
	if err != nil {
		return rmResp, errs.ErrEtcdMemberRemove.Wrap(err).GenWithStackByCause()
	}
	return rmResp, nil
}

// EtcdKVGet returns the etcd GetResponse by given key or key prefix
func EtcdKVGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warn("kv gets too slow", zap.String("request-key", key), zap.Duration("cost", cost), errs.ZapError(err))
	}

	if err != nil {
		e := errs.ErrEtcdKVGet.Wrap(err).GenWithStackByCause()
		log.Error("load from etcd meet error", zap.String("key", key), errs.ZapError(e))
		return resp, e
	}
	return resp, nil
}

// IsHealthy checks if the etcd is healthy.
func IsHealthy(ctx context.Context, client *clientv3.Client) bool {
	timeout := DefaultRequestTimeout
	failpoint.Inject("fastTick", func() {
		timeout = 100 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), timeout)
	defer cancel()
	_, err := client.Get(ctx, healthyPath)
	// permission denied is OK since proposal goes through consensus to get it
	// See: https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L124
	return err == nil || err == rpctypes.ErrPermissionDenied
}

// GetValue gets value with key from etcd.
func GetValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func get(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := EtcdKVGet(c, key, opts...)
	if err != nil {
		return nil, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errs.ErrEtcdKVGetResponse.FastGenByArgs(resp.Kvs)
	}
	return resp, nil
}

// GetProtoMsgWithModRev returns boolean to indicate whether the key exists or not.
func GetProtoMsgWithModRev(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, int64, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return false, 0, err
	}
	if resp == nil {
		return false, 0, nil
	}
	value := resp.Kvs[0].Value
	if err = proto.Unmarshal(value, msg); err != nil {
		return false, 0, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, resp.Kvs[0].ModRevision, nil
}

// EtcdKVPutWithTTL put (key, value) into etcd with a ttl of ttlSeconds
func EtcdKVPutWithTTL(ctx context.Context, c *clientv3.Client, key string, value string, ttlSeconds int64) (*clientv3.PutResponse, error) {
	kv := clientv3.NewKV(c)
	grantResp, err := c.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, err
	}
	return kv.Put(ctx, key, value, clientv3.WithLease(grantResp.ID))
}

const (
	// etcdServerOfflineTimeout is the timeout for an unhealthy etcd endpoint to be offline from healthy checker.
	etcdServerOfflineTimeout = 30 * time.Minute
	// etcdServerDisconnectedTimeout is the timeout for an unhealthy etcd endpoint to be disconnected from healthy checker.
	etcdServerDisconnectedTimeout = 1 * time.Minute
)

func newClient(tlsConfig *tls.Config, endpoints ...string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errs.ErrNewEtcdClient.FastGenByArgs("empty etcd endpoints")
	}
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          defaultEtcdClientTimeout,
		TLS:                  tlsConfig,
		LogConfig:            &lgc,
		DialKeepAliveTime:    defaultDialKeepAliveTime,
		DialKeepAliveTimeout: defaultDialKeepAliveTimeout,
	})
	return client, err
}

// CreateEtcdClient creates etcd v3 client with detecting endpoints.
func CreateEtcdClient(tlsConfig *tls.Config, acURLs []url.URL) (*clientv3.Client, error) {
	urls := make([]string, 0, len(acURLs))
	for _, u := range acURLs {
		urls = append(urls, u.String())
	}
	client, err := newClient(tlsConfig, urls...)
	if err != nil {
		return nil, err
	}

	tickerInterval := defaultDialKeepAliveTime
	failpoint.Inject("fastTick", func() {
		tickerInterval = 100 * time.Millisecond
	})
	failpoint.Inject("closeTick", func() {
		failpoint.Return(client, err)
	})

	checker := &healthyChecker{
		tlsConfig: tlsConfig,
	}
	eps := syncUrls(client)
	checker.update(eps)

	// Create a goroutine to check the health of etcd endpoints periodically.
	go func(client *clientv3.Client) {
		defer logutil.LogPanic()
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		lastAvailable := time.Now()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("[etcd client] etcd client is closed, exit health check goroutine")
				checker.Range(func(key, value interface{}) bool {
					client := value.(*healthyClient)
					client.Close()
					return true
				})
				return
			case <-ticker.C:
				usedEps := client.Endpoints()
				healthyEps := checker.patrol(client.Ctx())
				if len(healthyEps) == 0 {
					// when all endpoints are unhealthy, try to reset endpoints to update connect
					// rather than delete them to avoid there is no any endpoint in client.
					// Note: reset endpoints will trigger subconn closed, and then trigger reconnect.
					// otherwise, the subconn will be retrying in grpc layer and use exponential backoff,
					// and it cannot recover as soon as possible.
					if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
						log.Info("[etcd client] no available endpoint, try to reset endpoints", zap.Strings("last-endpoints", usedEps))
						client.SetEndpoints([]string{}...)
						client.SetEndpoints(usedEps...)
					}
				} else {
					if !typeutil.AreStringSlicesEquivalent(healthyEps, usedEps) {
						client.SetEndpoints(healthyEps...)
						change := fmt.Sprintf("%d->%d", len(usedEps), len(healthyEps))
						etcdStateGauge.WithLabelValues("endpoints").Set(float64(len(healthyEps)))
						log.Info("[etcd client] update endpoints", zap.String("num-change", change),
							zap.Strings("last-endpoints", usedEps), zap.Strings("endpoints", client.Endpoints()))
					}
					lastAvailable = time.Now()
				}
			}
		}
	}(client)

	// Notes: use another goroutine to update endpoints to avoid blocking health check in the first goroutine.
	go func(client *clientv3.Client) {
		defer logutil.LogPanic()
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("[etcd client] etcd client is closed, exit update endpoint goroutine")
				return
			case <-ticker.C:
				eps := syncUrls(client)
				checker.update(eps)
			}
		}
	}(client)

	return client, err
}

type healthyClient struct {
	*clientv3.Client
	lastHealth time.Time
}

type healthyChecker struct {
	sync.Map  // map[string]*healthyClient
	tlsConfig *tls.Config
}

func (checker *healthyChecker) patrol(ctx context.Context) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
	var wg sync.WaitGroup
	count := 0
	checker.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	hch := make(chan string, count)
	healthyList := make([]string, 0, count)
	checker.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(key, value interface{}) {
			defer wg.Done()
			defer logutil.LogPanic()
			ep := key.(string)
			client := value.(*healthyClient)
			if IsHealthy(ctx, client.Client) {
				hch <- ep
				checker.Store(ep, &healthyClient{
					Client:     client.Client,
					lastHealth: time.Now(),
				})
				return
			}
		}(key, value)
		return true
	})
	wg.Wait()
	close(hch)
	for h := range hch {
		healthyList = append(healthyList, h)
	}
	return healthyList
}

func (checker *healthyChecker) update(eps []string) {
	for _, ep := range eps {
		// check if client exists, if not, create one, if exists, check if it's offline or disconnected.
		if client, ok := checker.Load(ep); ok {
			lastHealthy := client.(*healthyClient).lastHealth
			if time.Since(lastHealthy) > etcdServerOfflineTimeout {
				log.Info("[etcd client] some etcd server maybe offline", zap.String("endpoint", ep))
				checker.Delete(ep)
			}
			if time.Since(lastHealthy) > etcdServerDisconnectedTimeout {
				// try to reset client endpoint to trigger reconnect
				client.(*healthyClient).Client.SetEndpoints([]string{}...)
				client.(*healthyClient).Client.SetEndpoints(ep)
			}
			continue
		}
		checker.addClient(ep, time.Now())
	}
}

func (checker *healthyChecker) addClient(ep string, lastHealth time.Time) {
	client, err := newClient(checker.tlsConfig, ep)
	if err != nil {
		log.Error("[etcd client] failed to create etcd healthy client", zap.Error(err))
		return
	}
	checker.Store(ep, &healthyClient{
		Client:     client,
		lastHealth: lastHealth,
	})
}

func syncUrls(client *clientv3.Client) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(client.Ctx()), DefaultRequestTimeout)
	defer cancel()
	mresp, err := client.MemberList(ctx)
	if err != nil {
		log.Error("[etcd client] failed to list members", errs.ZapError(err))
		return []string{}
	}
	var eps []string
	for _, m := range mresp.Members {
		if len(m.Name) != 0 && !m.IsLearner {
			eps = append(eps, m.ClientURLs...)
		}
	}
	return eps
}

// CreateClients creates etcd v3 client and http client.
func CreateClients(tlsConfig *tls.Config, acUrls []url.URL) (*clientv3.Client, *http.Client, error) {
	client, err := CreateEtcdClient(tlsConfig, acUrls)
	if err != nil {
		return nil, nil, errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
	}
	httpClient := createHTTPClient(tlsConfig)
	return client, httpClient, nil
}

// createHTTPClient creates a http client with the given tls config.
func createHTTPClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   tlsConfig,
		},
	}
}

// InitClusterID creates a cluster ID for the given key if it hasn't existed.
// This function assumes the cluster ID has already existed and always use a
// cheaper read to retrieve it; if it doesn't exist, invoke the more expensive
// operation InitOrGetClusterID().
func InitClusterID(c *clientv3.Client, key string) (clusterID uint64, err error) {
	// Get any cluster key to parse the cluster ID.
	resp, err := EtcdKVGet(c, key)
	if err != nil {
		return 0, err
	}
	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		return InitOrGetClusterID(c, key)
	}
	return typeutil.BytesToUint64(resp.Kvs[0].Value)
}

// GetClusterID gets the cluster ID for the given key.
func GetClusterID(c *clientv3.Client, key string) (clusterID uint64, err error) {
	// Get any cluster key to parse the cluster ID.
	resp, err := EtcdKVGet(c, key)
	if err != nil {
		return 0, err
	}
	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	return typeutil.BytesToUint64(resp.Kvs[0].Value)
}

// InitOrGetClusterID creates a cluster ID for the given key with a CAS operation,
// if the cluster ID doesn't exist.
func InitOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(r.Uint32())
	value := typeutil.Uint64ToBytes(clusterID)

	// Multiple servers may try to init the cluster ID at the same time.
	// Only one server can commit this transaction, then other servers
	// can get the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

const (
	defaultLoadDataFromEtcdTimeout   = 30 * time.Second
	defaultLoadFromEtcdRetryInterval = 200 * time.Millisecond
	defaultLoadFromEtcdRetryTimes    = int(defaultLoadDataFromEtcdTimeout / defaultLoadFromEtcdRetryInterval)
	defaultLoadBatchSize             = 400
	defaultWatchChangeRetryInterval  = 1 * time.Second
	defaultForceLoadMinimalInterval  = 200 * time.Millisecond
)

// LoopWatcher loads data from etcd and sets a watcher for it.
type LoopWatcher struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	name   string
	client *clientv3.Client

	// key is the etcd key to watch.
	key string
	// opts is used to set etcd options.
	opts []clientv3.OpOption

	// forceLoadCh is used to force loading data from etcd.
	forceLoadCh chan struct{}
	// isLoadedCh is used to notify that the data has been loaded from etcd first time.
	isLoadedCh chan error

	// putFn is used to handle the put event.
	putFn func(*mvccpb.KeyValue) error
	// deleteFn is used to handle the delete event.
	deleteFn func(*mvccpb.KeyValue) error
	// postEventFn is used to call after handling all events.
	postEventFn func() error

	// forceLoadMu is used to ensure two force loads have minimal interval.
	forceLoadMu sync.RWMutex
	// lastTimeForceLoad is used to record the last time force loading data from etcd.
	lastTimeForceLoad time.Time

	// loadTimeout is used to set the timeout for loading data from etcd.
	loadTimeout time.Duration
	// loadRetryTimes is used to set the retry times for loading data from etcd.
	loadRetryTimes int
	// loadBatchSize is used to set the batch size for loading data from etcd.
	loadBatchSize int64
	// watchChangeRetryInterval is used to set the retry interval for watching etcd change.
	watchChangeRetryInterval time.Duration
	// updateClientCh is used to update the etcd client.
	// It's only used for testing.
	updateClientCh chan *clientv3.Client
}

// NewLoopWatcher creates a new LoopWatcher.
func NewLoopWatcher(ctx context.Context, wg *sync.WaitGroup, client *clientv3.Client, name, key string,
	putFn, deleteFn func(*mvccpb.KeyValue) error, postEventFn func() error, opts ...clientv3.OpOption) *LoopWatcher {
	return &LoopWatcher{
		ctx:                      ctx,
		client:                   client,
		name:                     name,
		key:                      key,
		wg:                       wg,
		forceLoadCh:              make(chan struct{}, 1),
		isLoadedCh:               make(chan error, 1),
		updateClientCh:           make(chan *clientv3.Client, 1),
		putFn:                    putFn,
		deleteFn:                 deleteFn,
		postEventFn:              postEventFn,
		opts:                     opts,
		lastTimeForceLoad:        time.Now(),
		loadTimeout:              defaultLoadDataFromEtcdTimeout,
		loadRetryTimes:           defaultLoadFromEtcdRetryTimes,
		loadBatchSize:            defaultLoadBatchSize,
		watchChangeRetryInterval: defaultWatchChangeRetryInterval,
	}
}

// StartWatchLoop starts a loop to watch the key.
func (lw *LoopWatcher) StartWatchLoop() {
	defer logutil.LogPanic()
	defer lw.wg.Done()

	ctx, cancel := context.WithCancel(lw.ctx)
	defer cancel()
	watchStartRevision := lw.initFromEtcd(ctx)

	log.Info("start to watch loop", zap.String("name", lw.name), zap.String("key", lw.key))
	for {
		select {
		case <-ctx.Done():
			log.Info("server is closed, exit watch loop", zap.String("name", lw.name), zap.String("key", lw.key))
			return
		default:
		}
		nextRevision, err := lw.watch(ctx, watchStartRevision)
		if err != nil {
			log.Error("watcher canceled unexpectedly and a new watcher will start after a while for watch loop",
				zap.String("name", lw.name),
				zap.String("key", lw.key),
				zap.Int64("next-revision", nextRevision),
				zap.Time("retry-at", time.Now().Add(lw.watchChangeRetryInterval)),
				zap.Error(err))
			watchStartRevision = nextRevision
			time.Sleep(lw.watchChangeRetryInterval)
			failpoint.Inject("updateClient", func() {
				lw.client = <-lw.updateClientCh
			})
		}
	}
}

func (lw *LoopWatcher) initFromEtcd(ctx context.Context) int64 {
	var (
		watchStartRevision int64
		err                error
	)
	ticker := time.NewTicker(defaultLoadFromEtcdRetryInterval)
	defer ticker.Stop()
	ctx, cancel := context.WithTimeout(ctx, lw.loadTimeout)
	defer cancel()

	for i := 0; i < lw.loadRetryTimes; i++ {
		failpoint.Inject("loadTemporaryFail", func(val failpoint.Value) {
			if maxFailTimes, ok := val.(int); ok && i < maxFailTimes {
				err = errors.New("fail to read from etcd")
				failpoint.Continue()
			}
		})
		failpoint.Inject("delayLoad", func(val failpoint.Value) {
			if sleepIntervalSeconds, ok := val.(int); ok && sleepIntervalSeconds > 0 {
				time.Sleep(time.Duration(sleepIntervalSeconds) * time.Second)
			}
		})
		watchStartRevision, err = lw.load(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			lw.isLoadedCh <- errors.Errorf("ctx is done before load data from etcd")
			return watchStartRevision
		case <-ticker.C:
		}
	}
	if err != nil {
		log.Warn("meet error when loading in watch loop", zap.String("name", lw.name), zap.String("key", lw.key), zap.Error(err))
	} else {
		log.Info("load finished in watch loop", zap.String("name", lw.name), zap.String("key", lw.key))
	}
	lw.isLoadedCh <- err
	return watchStartRevision
}

func (lw *LoopWatcher) watch(ctx context.Context, revision int64) (nextRevision int64, err error) {
	watcher := clientv3.NewWatcher(lw.client)
	defer watcher.Close()
	var watchChanCancel context.CancelFunc
	defer func() {
		if watchChanCancel != nil {
			watchChanCancel()
		}
	}()
	for {
		if watchChanCancel != nil {
			watchChanCancel()
		}
		// In order to prevent a watch stream being stuck in a partitioned node,
		// make sure to wrap context with "WithRequireLeader".
		watchChanCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		watchChanCancel = cancel
		opts := append(lw.opts, clientv3.WithRev(revision))
		watchChan := watcher.Watch(watchChanCtx, lw.key, opts...)
	WatchChanLoop:
		select {
		case <-ctx.Done():
			return revision, nil
		case <-lw.forceLoadCh:
			revision, err = lw.load(ctx)
			if err != nil {
				log.Warn("force load key failed in watch loop", zap.String("name", lw.name),
					zap.String("key", lw.key), zap.Error(err))
			}
			continue
		case wresp := <-watchChan:
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision in watch loop",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				continue
			} else if wresp.Err() != nil { // wresp.Err() contains CompactRevision not equal to 0
				log.Error("watcher is canceled in watch loop",
					zap.Int64("revision", revision),
					errs.ZapError(errs.ErrEtcdWatcherCancel, wresp.Err()))
				return revision, wresp.Err()
			}
			for _, event := range wresp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					if err := lw.putFn(event.Kv); err != nil {
						log.Error("put failed in watch loop", zap.String("name", lw.name),
							zap.String("key", lw.key), zap.Error(err))
					} else {
						log.Debug("put in watch loop", zap.String("name", lw.name),
							zap.ByteString("key", event.Kv.Key),
							zap.ByteString("value", event.Kv.Value))
					}
				case clientv3.EventTypeDelete:
					if err := lw.deleteFn(event.Kv); err != nil {
						log.Error("delete failed in watch loop", zap.String("name", lw.name),
							zap.String("key", lw.key), zap.Error(err))
					} else {
						log.Debug("delete in watch loop", zap.String("name", lw.name),
							zap.ByteString("key", event.Kv.Key))
					}
				}
			}
			if err := lw.postEventFn(); err != nil {
				log.Error("run post event failed in watch loop", zap.String("name", lw.name),
					zap.String("key", lw.key), zap.Error(err))
			}
			revision = wresp.Header.Revision + 1
			goto WatchChanLoop // use goto to avoid to create a new watchChan
		}
	}
}

func (lw *LoopWatcher) load(ctx context.Context) (nextRevision int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	startKey := lw.key
	// If limit is 0, it means no limit.
	// If limit is not 0, we need to add 1 to limit to get the next key.
	limit := lw.loadBatchSize
	if limit != 0 {
		limit++
	}
	for {
		// Sort by key to get the next key and we don't need to worry about the performance,
		// Because the default sort is just SortByKey and SortAscend
		opts := append(lw.opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(limit))
		resp, err := clientv3.NewKV(lw.client).Get(ctx, startKey, opts...)
		if err != nil {
			log.Error("load failed in watch loop", zap.String("name", lw.name),
				zap.String("key", lw.key), zap.Error(err))
			return 0, err
		}
		for i, item := range resp.Kvs {
			if resp.More && i == len(resp.Kvs)-1 {
				// The last key is the start key of the next batch.
				// To avoid to get the same key in the next load, we need to skip the last key.
				startKey = string(item.Key)
				continue
			}
			err = lw.putFn(item)
			if err != nil {
				log.Error("put failed in watch loop when loading", zap.String("name", lw.name), zap.String("key", lw.key), zap.Error(err))
			}
		}
		// Note: if there are no keys in etcd, the resp.More is false. It also means the load is finished.
		if !resp.More {
			if err := lw.postEventFn(); err != nil {
				log.Error("run post event failed in watch loop", zap.String("name", lw.name),
					zap.String("key", lw.key), zap.Error(err))
			}
			return resp.Header.Revision + 1, err
		}
	}
}

// ForceLoad forces to load the key.
func (lw *LoopWatcher) ForceLoad() {
	// When NotLeader error happens, a large volume of force load requests will be received here,
	// so the minimal interval between two force loads (from etcd) is used to avoid the congestion.
	// Two-phase locking is also used to let most of the requests return directly without acquiring
	// the write lock and causing the system to choke.
	lw.forceLoadMu.RLock()
	if time.Since(lw.lastTimeForceLoad) < defaultForceLoadMinimalInterval {
		lw.forceLoadMu.RUnlock()
		return
	}
	lw.forceLoadMu.RUnlock()

	lw.forceLoadMu.Lock()
	if time.Since(lw.lastTimeForceLoad) < defaultForceLoadMinimalInterval {
		lw.forceLoadMu.Unlock()
		return
	}
	lw.lastTimeForceLoad = time.Now()
	lw.forceLoadMu.Unlock()

	select {
	case lw.forceLoadCh <- struct{}{}:
	default:
	}
}

// WaitLoad waits for the result to obtain whether data is loaded.
func (lw *LoopWatcher) WaitLoad() error {
	return <-lw.isLoadedCh
}

// SetLoadRetryTimes sets the retry times when loading data from etcd.
func (lw *LoopWatcher) SetLoadRetryTimes(times int) {
	lw.loadRetryTimes = times
}

// SetLoadTimeout sets the timeout when loading data from etcd.
func (lw *LoopWatcher) SetLoadTimeout(timeout time.Duration) {
	lw.loadTimeout = timeout
}

// SetLoadBatchSize sets the batch size when loading data from etcd.
func (lw *LoopWatcher) SetLoadBatchSize(size int64) {
	lw.loadBatchSize = size
}
