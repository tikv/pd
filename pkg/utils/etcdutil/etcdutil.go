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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
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

	// MaxEtcdTxnOps is the max value of operations in an etcd txn. The default limit of etcd txn op is 128.
	// We use 120 here to leave some space for other operations.
	// See: https://github.com/etcd-io/etcd/blob/d3e43d4de6f6d9575b489dd7850a85e37e0f6b6c/server/embed/config.go#L61
	MaxEtcdTxnOps = 120
)

// CheckClusterID checks etcd cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID etcdtypes.ID, um etcdtypes.URLsMap, tlsConfig *tls.Config) error {
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
func ListEtcdMembers(ctx context.Context, client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	failpoint.Inject("SlowEtcdMemberList", func(val failpoint.Value) {
		d := val.(int)
		time.Sleep(time.Duration(d) * time.Second)
	})
	newCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	// After the etcd server is upgraded to v3.5.x, the MemberList API will return the member list in a linearizable way by default.
	// It is introduced by https://github.com/etcd-io/etcd/pull/11639
	// If Linearizable is set to false, the member list will be returned with server's local data.
	// If Linearizable is set to true, it is served with linearizable guarantee. If the server is disconnected from quorum, `MemberList` call will fail.
	c := clientv3.RetryClusterClient(client)
	resp, err := c.MemberList(newCtx, &etcdserverpb.MemberListRequest{Linearizable: false})
	cancel()
	if err != nil {
		return (*clientv3.MemberListResponse)(resp), errs.ErrEtcdMemberList.Wrap(err).GenWithStackByCause()
	}
	return (*clientv3.MemberListResponse)(resp), nil
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
	failpoint.Inject("SlowEtcdKVGet", func(val failpoint.Value) {
		d := val.(int)
		time.Sleep(time.Duration(d) * time.Second)
	})
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
func EtcdKVPutWithTTL(ctx context.Context, c *clientv3.Client, key string, value string, ttlSeconds int64) (clientv3.LeaseID, error) {
	kv := clientv3.NewKV(c)
	grantResp, err := c.Grant(ctx, ttlSeconds)
	if err != nil {
		return 0, err
	}
	_, err = kv.Put(ctx, key, value, clientv3.WithLease(grantResp.ID))
	return grantResp.ID, err
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
func CreateEtcdClient(tlsConfig *tls.Config, acURLs []url.URL, sourceOpt ...string) (*clientv3.Client, error) {
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
	source := "default-etcd-client"
	if len(sourceOpt) > 0 {
		source = sourceOpt[0]
	}
	initHealthChecker(tickerInterval, tlsConfig, client, source)

	return client, err
}

// CreateHTTPClient creates a http client with the given tls config.
func CreateHTTPClient(tlsConfig *tls.Config) *http.Client {
	// FIXME: Currently, there is no timeout set for certain requests, such as GetRegions,
	// which may take a significant amount of time. However, it might be necessary to
	// define an appropriate timeout in the future.
	cli := &http.Client{}
	if tlsConfig != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		cli.Transport = transport
	}
	return cli
}

const (
	defaultEtcdRetryInterval      = time.Second
	defaultLoadFromEtcdRetryTimes = 3
	maxLoadBatchSize              = int64(10000)
	minLoadBatchSize              = int64(100)

	// RequestProgressInterval is the interval to call RequestProgress for watcher.
	RequestProgressInterval = 1 * time.Second
	// WatchChTimeoutDuration is the timeout duration for a watchChan.
	WatchChTimeoutDuration = DefaultRequestTimeout
)

// LoopWatcher loads data from etcd and sets a watcher for it.
type LoopWatcher struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	name   string
	client *clientv3.Client

	// key is the etcd key to watch.
	key string
	// isWithPrefix indicates whether the watcher is with prefix.
	isWithPrefix bool

	// forceLoadCh is used to force loading data from etcd.
	forceLoadCh chan struct{}
	// isLoadedCh is used to notify that the data has been loaded from etcd first time.
	isLoadedCh chan error

	// putFn is used to handle the put event.
	putFn func(*mvccpb.KeyValue) error
	// deleteFn is used to handle the delete event.
	deleteFn func(*mvccpb.KeyValue) error
	// postEventsFn is used to call after handling all events.
	postEventsFn func([]*clientv3.Event) error
	// preEventsFn is used to call before handling all events.
	preEventsFn func([]*clientv3.Event) error

	// forceLoadMu is used to ensure two force loads have minimal interval.
	forceLoadMu syncutil.RWMutex
	// lastTimeForceLoad is used to record the last time force loading data from etcd.
	lastTimeForceLoad time.Time

	// loadRetryTimes is used to set the retry times for loading data from etcd.
	loadRetryTimes int
	// loadBatchSize is used to set the batch size for loading data from etcd.
	loadBatchSize int64
	// watchChangeRetryInterval is used to set the retry interval for watching etcd change.
	watchChangeRetryInterval time.Duration
	// updateClientCh is used to update the etcd client.
	// It's only used for testing.
	updateClientCh chan *clientv3.Client
	// watchChTimeoutDuration is the timeout duration for a watchChan.
	watchChTimeoutDuration time.Duration
}

// NewLoopWatcher creates a new LoopWatcher.
func NewLoopWatcher(
	ctx context.Context, wg *sync.WaitGroup,
	client *clientv3.Client,
	name, key string,
	preEventsFn func([]*clientv3.Event) error,
	putFn, deleteFn func(*mvccpb.KeyValue) error,
	postEventsFn func([]*clientv3.Event) error,
	isWithPrefix bool,
) *LoopWatcher {
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
		postEventsFn:             postEventsFn,
		preEventsFn:              preEventsFn,
		isWithPrefix:             isWithPrefix,
		lastTimeForceLoad:        time.Now(),
		loadRetryTimes:           defaultLoadFromEtcdRetryTimes,
		loadBatchSize:            maxLoadBatchSize,
		watchChangeRetryInterval: defaultEtcdRetryInterval,
		watchChTimeoutDuration:   WatchChTimeoutDuration,
	}
}

// StartWatchLoop starts a loop to watch the key.
func (lw *LoopWatcher) StartWatchLoop() {
	lw.wg.Add(1)
	go func() {
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
	}()
}

func (lw *LoopWatcher) initFromEtcd(ctx context.Context) int64 {
	var (
		watchStartRevision int64
		err                error
	)
	ticker := time.NewTicker(defaultEtcdRetryInterval)
	defer ticker.Stop()
	for i := range lw.loadRetryTimes {
		failpoint.Inject("loadTemporaryFail", func(val failpoint.Value) {
			if maxFailTimes, ok := val.(int); ok && i < maxFailTimes {
				err = errors.New("fail to read from etcd")
				failpoint.Continue()
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
	ticker := time.NewTicker(RequestProgressInterval)
	defer ticker.Stop()

	for {
		if watcherCancel != nil {
			watcherCancel()
		}
		if watcher != nil {
			watcher.Close()
		}
		watcher = clientv3.NewWatcher(lw.client)
		// In order to prevent a watch stream being stuck in a partitioned node,
		// make sure to wrap context with "WithRequireLeader".
		watcherCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		watcherCancel = cancel
		opts := []clientv3.OpOption{clientv3.WithRev(revision), clientv3.WithProgressNotify()}
		if lw.isWithPrefix {
			opts = append(opts, clientv3.WithPrefix())
		}
		done := make(chan struct{})
		go grpcutil.CheckStream(watcherCtx, watcherCancel, done)
		watchChan := watcher.Watch(watcherCtx, lw.key, opts...)
		done <- struct{}{}
		if err := watcherCtx.Err(); err != nil {
			log.Warn("error occurred while creating watch channel and retry it", zap.Error(err),
				zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
			select {
			case <-ctx.Done():
				return revision, nil
			case <-ticker.C:
				continue
			}
		}
		lastReceivedResponseTime := time.Now()
		log.Info("watch channel is created in watch loop",
			zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))

	watchChanLoop:
		select {
		case <-ctx.Done():
			return revision, nil
		case <-lw.forceLoadCh:
			revision, err = lw.load(ctx)
			if err != nil {
				log.Warn("force load key failed in watch loop",
					zap.String("name", lw.name), zap.String("key", lw.key), zap.Int64("revision", revision), zap.Error(err))
			} else {
				log.Info("force load key successfully in watch loop",
					zap.String("name", lw.name), zap.String("key", lw.key), zap.Int64("revision", revision))
			}
			continue
		case <-ticker.C:
			// We need to request progress to etcd to prevent etcd hold the watchChan,
			// note: the ctx must be from watcherCtx, otherwise, the RequestProgress request cannot be sent properly.
			ctx, cancel := context.WithTimeout(watcherCtx, DefaultRequestTimeout)
			if err := watcher.RequestProgress(ctx); err != nil {
				log.Warn("failed to request progress in leader watch loop",
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key), zap.Error(err))
			}
			cancel()
			// If no message comes from an etcd watchChan for WatchChTimeoutDuration,
			// create a new one and need not to reset lastReceivedResponseTime.
			if time.Since(lastReceivedResponseTime) >= lw.watchChTimeoutDuration {
				log.Warn("watch channel is blocked for a long time, recreating a new one in watch loop",
					zap.Duration("timeout", time.Since(lastReceivedResponseTime)),
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
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
				log.Warn("required revision has been compacted, use the compact revision in watch loop",
					zap.Int64("required-revision", revision), zap.Int64("compact-revision", wresp.CompactRevision),
					zap.String("name", lw.name), zap.String("key", lw.key))
				revision = wresp.CompactRevision
				continue
			} else if err := wresp.Err(); err != nil { // wresp.Err() contains CompactRevision not equal to 0
				log.Error("watcher is canceled in watch loop", errs.ZapError(errs.ErrEtcdWatcherCancel, err),
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
				return revision, err
			} else if wresp.IsProgressNotify() {
				log.Debug("watcher receives progress notify in watch loop",
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
				goto watchChanLoop
			}
			if err := lw.preEventsFn(wresp.Events); err != nil {
				log.Error("run pre event failed in watch loop", zap.Error(err),
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
			}
			for _, event := range wresp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					if err := lw.putFn(event.Kv); err != nil {
						log.Error("put failed in watch loop", zap.Error(err),
							zap.Int64("revision", revision), zap.String("name", lw.name),
							zap.String("watch-key", lw.key), zap.ByteString("event-kv-key", event.Kv.Key))
					} else {
						log.Debug("put successfully in watch loop", zap.String("name", lw.name),
							zap.ByteString("key", event.Kv.Key),
							zap.ByteString("value", event.Kv.Value))
					}
				case clientv3.EventTypeDelete:
					if err := lw.deleteFn(event.Kv); err != nil {
						log.Error("delete failed in watch loop", zap.Error(err),
							zap.Int64("revision", revision), zap.String("name", lw.name),
							zap.String("watch-key", lw.key), zap.ByteString("event-kv-key", event.Kv.Key))
					} else {
						log.Debug("delete successfully in watch loop", zap.String("name", lw.name),
							zap.ByteString("key", event.Kv.Key))
					}
				}
			}
			if err := lw.postEventsFn(wresp.Events); err != nil {
				log.Error("run post event failed in watch loop", zap.Error(err),
					zap.Int64("revision", revision), zap.String("name", lw.name), zap.String("key", lw.key))
			}
			revision = wresp.Header.Revision + 1
		}
		goto watchChanLoop // Use goto to avoid creating a new watchChan
	}
}

func (lw *LoopWatcher) load(ctx context.Context) (nextRevision int64, err error) {
	startKey := lw.key
	limit := lw.loadBatchSize
	opts := lw.buildLoadingOpts(limit)

	if err := lw.preEventsFn([]*clientv3.Event{}); err != nil {
		log.Error("run pre event failed in watch loop", zap.String("name", lw.name),
			zap.String("key", lw.key), zap.Error(err))
	}
	defer func() {
		if err := lw.postEventsFn([]*clientv3.Event{}); err != nil {
			log.Error("run post event failed in watch loop", zap.String("name", lw.name),
				zap.String("key", lw.key), zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return 0, nil
		default:
		}
		resp, err := EtcdKVGet(lw.client, startKey, opts...)
		failpoint.Inject("meetEtcdError", func() {
			if limit > minLoadBatchSize {
				err = errors.New(codes.ResourceExhausted.String())
			}
		})
		if err != nil {
			log.Error("load failed in watch loop", zap.String("name", lw.name),
				zap.String("key", lw.key), zap.Error(err))
			if strings.Contains(err.Error(), codes.ResourceExhausted.String()) ||
				strings.Contains(err.Error(), codes.DeadlineExceeded.String()) {
				if limit == 0 {
					limit = maxLoadBatchSize
				} else if limit > minLoadBatchSize {
					limit /= 2
				} else {
					return 0, err
				}
				opts = lw.buildLoadingOpts(limit)
				continue
			}
			return 0, err
		}
		for i, item := range resp.Kvs {
			if i == len(resp.Kvs)-1 && resp.More {
				// If there are more keys, we need to load the next batch.
				// The last key in current batch is the start key of the next batch.
				startKey = string(item.Key)
				// To avoid to get the same key in the next batch,
				// we need to skip the last key for the current batch.
				continue
			}
			err = lw.putFn(item)
			if err != nil {
				log.Error("put failed in watch loop when loading", zap.String("name", lw.name), zap.String("watch-key", lw.key),
					zap.ByteString("key", item.Key), zap.ByteString("value", item.Value), zap.Error(err))
			} else {
				log.Debug("put successfully in watch loop when loading", zap.String("name", lw.name), zap.String("watch-key", lw.key),
					zap.ByteString("key", item.Key), zap.ByteString("value", item.Value))
			}
		}
		// Note: if there are no keys in etcd, the resp.More is false. It also means the load is finished.
		if !resp.More {
			return resp.Header.Revision + 1, err
		}
	}
}

func (lw *LoopWatcher) buildLoadingOpts(limit int64) []clientv3.OpOption {
	// Sort by key to get the next key and we don't need to worry about the performance,
	// Because the default sort is just SortByKey and SortAscend
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)}
	// In most cases, 'Get(foo, WithPrefix())' is equivalent to 'Get(foo, WithRange(GetPrefixRangeEnd(foo))'.
	// However, when the startKey changes, the two are no longer equivalent.
	// For example, the end key for 'WithRange(GetPrefixRangeEnd(foo))' is consistently 'fop'.
	// But when using 'Get(foo1, WithPrefix())', the end key becomes 'foo2', not 'fop'.
	// So, we use 'WithRange()' to avoid this problem.
	if lw.isWithPrefix {
		opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(lw.key)))
	}
	// If limit is 0, it means no limit.
	// If limit is not 0, we need to add 1 to limit to get the next key.
	if limit == 0 {
		return opts
	}
	return append(opts, clientv3.WithLimit(limit+1))
}

// ForceLoad forces to load the key.
func (lw *LoopWatcher) ForceLoad() {
	// When NotLeader error happens, a large volume of force load requests will be received here,
	// so the minimal interval between two force loads (from etcd) is used to avoid the congestion.
	// Two-phase locking is also used to let most of the requests return directly without acquiring
	// the write lock and causing the system to choke.
	lw.forceLoadMu.RLock()
	if time.Since(lw.lastTimeForceLoad) < defaultEtcdRetryInterval {
		lw.forceLoadMu.RUnlock()
		return
	}
	lw.forceLoadMu.RUnlock()

	lw.forceLoadMu.Lock()
	if time.Since(lw.lastTimeForceLoad) < defaultEtcdRetryInterval {
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

// SetLoadBatchSize sets the batch size when loading data from etcd.
func (lw *LoopWatcher) SetLoadBatchSize(size int64) {
	lw.loadBatchSize = size
}
