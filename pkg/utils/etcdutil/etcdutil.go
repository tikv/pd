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
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

const (
	// defaultEtcdClientTimeout is the default timeout for etcd client.
	defaultEtcdClientTimeout = 3 * time.Second

	// defaultAutoSyncInterval is the interval to sync etcd cluster.
	defaultAutoSyncInterval = 60 * time.Second

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

// CreateClientsWithMultiEndpoint creates etcd v3 client and http client.
func CreateClientsWithMultiEndpoint(tlsConfig *tls.Config, acUrls []url.URL) (*clientv3.Client, *http.Client, error) {
	client, err := createEtcdClientWithMultiEndpoint(tlsConfig, acUrls)
	if err != nil {
		return nil, nil, errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
	}
	httpClient := createHTTPClient(tlsConfig)
	return client, httpClient, nil
}

// CreateClients creates etcd v3 client and http client.
func CreateClients(tlsConfig *tls.Config, acUrls url.URL) (*clientv3.Client, *http.Client, error) {
	client, err := createEtcdClient(tlsConfig, acUrls)
	if err != nil {
		return nil, nil, errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
	}
	httpClient := createHTTPClient(tlsConfig)
	return client, httpClient, nil
}

// createEtcdClientWithMultiEndpoint creates etcd v3 client.
// Note: it will be used by micro service server and support multi etcd endpoints.
// FIXME: But it cannot switch etcd endpoints as soon as possible when one of endpoints is with io hang.
func createEtcdClientWithMultiEndpoint(tlsConfig *tls.Config, acUrls []url.URL) (*clientv3.Client, error) {
	if len(acUrls) == 0 {
		return nil, errs.ErrNewEtcdClient.FastGenByArgs("no available etcd address")
	}
	endpoints := make([]string, 0, len(acUrls))
	for _, u := range acUrls {
		endpoints = append(endpoints, u.String())
	}
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	autoSyncInterval := defaultAutoSyncInterval
	dialKeepAliveTime := defaultDialKeepAliveTime
	dialKeepAliveTimeout := defaultDialKeepAliveTimeout
	failpoint.Inject("autoSyncInterval", func() {
		autoSyncInterval = 10 * time.Millisecond
	})
	failpoint.Inject("closeKeepAliveCheck", func() {
		autoSyncInterval = 0
		dialKeepAliveTime = 0
		dialKeepAliveTimeout = 0
	})
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          defaultEtcdClientTimeout,
		AutoSyncInterval:     autoSyncInterval,
		TLS:                  tlsConfig,
		LogConfig:            &lgc,
		DialKeepAliveTime:    dialKeepAliveTime,
		DialKeepAliveTimeout: dialKeepAliveTimeout,
	})
	if err == nil {
		log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints))
	}
	return client, err
}

// createEtcdClient creates etcd v3 client.
// Note: it will be used by legacy pd-server, and only connect to leader only.
func createEtcdClient(tlsConfig *tls.Config, acURL url.URL) (*clientv3.Client, error) {
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{acURL.String()},
		DialTimeout: defaultEtcdClientTimeout,
		TLS:         tlsConfig,
		LogConfig:   &lgc,
	})
	if err == nil {
		log.Info("create etcd v3 client", zap.String("endpoints", acURL.String()))
	}
	return client, err
}

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
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
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
