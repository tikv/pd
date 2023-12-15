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
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestMemberHelpers(t *testing.T) {
	re := require.New(t)
	servers, client1, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp1.Members, 1)
	// types.ID is an alias of uint64.
	re.Equal(uint64(etcd1.Server.ID()), listResp1.Members[0].ID)

	// Test AddEtcdMember
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})

	// Test CheckClusterID
	urlsMap, err := types.NewURLsMap(etcd2.Config().InitialCluster)
	re.NoError(err)
	err = CheckClusterID(etcd1.Server.Cluster().ID(), urlsMap, &tls.Config{MinVersion: tls.VersionTLS12})
	re.NoError(err)

	// Test RemoveEtcdMember
	_, err = RemoveEtcdMember(client1, uint64(etcd2.Server.ID()))
	re.NoError(err)

	listResp3, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp3.Members, 1)
	re.Equal(uint64(etcd1.Server.ID()), listResp3.Members[0].ID)
}

func TestEtcdKVGet(t *testing.T) {
	re := require.New(t)
	_, client, clean := NewTestEtcdCluster(t, 1)
	defer clean()

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	kv := clientv3.NewKV(client)
	for i := range keys {
		_, err := kv.Put(context.TODO(), keys[i], vals[i])
		re.NoError(err)
	}

	// Test simple point get
	resp, err := EtcdKVGet(client, "test/key1")
	re.NoError(err)
	re.Equal("val1", string(resp.Kvs[0].Value))

	// Test range get
	withRange := clientv3.WithRange("test/zzzz")
	withLimit := clientv3.WithLimit(3)
	resp, err = EtcdKVGet(client, "test/", withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	re.NoError(err)
	re.Len(resp.Kvs, 3)

	for i := range resp.Kvs {
		re.Equal(keys[i], string(resp.Kvs[i].Key))
		re.Equal(vals[i], string(resp.Kvs[i].Value))
	}

	lastKey := string(resp.Kvs[len(resp.Kvs)-1].Key)
	next := clientv3.GetPrefixRangeEnd(lastKey)
	resp, err = EtcdKVGet(client, next, withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	re.NoError(err)
	re.Len(resp.Kvs, 2)
}

func TestEtcdKVPutWithTTL(t *testing.T) {
	re := require.New(t)
	_, client, clean := NewTestEtcdCluster(t, 1)
	defer clean()

	_, err := EtcdKVPutWithTTL(context.TODO(), client, "test/ttl1", "val1", 2)
	re.NoError(err)
	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl2", "val2", 4)
	re.NoError(err)

	time.Sleep(3 * time.Second)
	// test/ttl1 is outdated
	resp, err := EtcdKVGet(client, "test/ttl1")
	re.NoError(err)
	re.Equal(int64(0), resp.Count)
	// but test/ttl2 is not
	resp, err = EtcdKVGet(client, "test/ttl2")
	re.NoError(err)
	re.Equal("val2", string(resp.Kvs[0].Value))

	time.Sleep(2 * time.Second)

	// test/ttl2 is also outdated
	resp, err = EtcdKVGet(client, "test/ttl2")
	re.NoError(err)
	re.Equal(int64(0), resp.Count)
}

func TestInitClusterID(t *testing.T) {
	re := require.New(t)
	_, client, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	pdClusterIDPath := "test/TestInitClusterID/pd/cluster_id"
	// Get any cluster key to parse the cluster ID.
	resp, err := EtcdKVGet(client, pdClusterIDPath)
	re.NoError(err)
	re.Equal(0, len(resp.Kvs))

	clusterID, err := InitClusterID(client, pdClusterIDPath)
	re.NoError(err)
	re.NotEqual(0, clusterID)

	clusterID1, err := InitClusterID(client, pdClusterIDPath)
	re.NoError(err)
	re.Equal(clusterID, clusterID1)
}

func TestEtcdClientSync(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))

	servers, client1, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Add a new member.
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})
	testutil.Eventually(re, func() bool {
		// wait for etcd client sync endpoints
		return len(client1.Endpoints()) == 2
	})

	// Remove the first member and close the etcd1.
	_, err := RemoveEtcdMember(client1, uint64(etcd1.Server.ID()))
	re.NoError(err)
	etcd1.Close()

	// Check the client can get the new member with the new endpoints.
	testutil.Eventually(re, func() bool {
		// wait for etcd client sync endpoints
		return len(client1.Endpoints()) == 1
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))
}

func TestEtcdScaleInAndOut(t *testing.T) {
	re := require.New(t)
	// Start a etcd server.
	servers, _, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Create two etcd clients with etcd1 as endpoint.
	client1, err := CreateEtcdClient(nil, cfg1.LCUrls) // execute member change operation with this client
	re.NoError(err)
	defer client1.Close()
	client2, err := CreateEtcdClient(nil, cfg1.LCUrls) // check member change with this client
	re.NoError(err)
	defer client2.Close()

	// Add a new member and check members
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client2, []*embed.Etcd{etcd1, etcd2})

	// scale in etcd1
	_, err = RemoveEtcdMember(client1, uint64(etcd1.Server.ID()))
	re.NoError(err)
	checkMembers(re, client2, []*embed.Etcd{etcd2})
}

func TestRandomKillEtcd(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))
	// Start a etcd server.
	etcds, client1, clean := NewTestEtcdCluster(t, 3)
	defer clean()
	testutil.Eventually(re, func() bool {
		return len(client1.Endpoints()) == 3
	})

	// Randomly kill an etcd server and restart it
	cfgs := []embed.Config{etcds[0].Config(), etcds[1].Config(), etcds[2].Config()}
	for i := 0; i < 10; i++ {
		killIndex := rand.Intn(len(etcds))
		etcds[killIndex].Close()
		testutil.Eventually(re, func() bool {
			return IsHealthy(context.Background(), client1)
		})
		etcd, err := embed.StartEtcd(&cfgs[killIndex])
		re.NoError(err)
		<-etcd.Server.ReadyNotify()
		etcds[killIndex] = etcd
		testutil.Eventually(re, func() bool {
			return IsHealthy(context.Background(), client1)
		})
	}
	for _, etcd := range etcds {
		if etcd != nil {
			etcd.Close()
		}
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))
}

func TestEtcdWithHangLeaderEnableCheck(t *testing.T) {
	re := require.New(t)
	var err error
	// Test with enable check.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))
	err = checkEtcdWithHangLeader(t)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))

	// Test with disable check.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/closeTick", "return(true)"))
	err = checkEtcdWithHangLeader(t)
	re.Error(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/closeTick"))
}

func checkEtcdWithHangLeader(t *testing.T) error {
	re := require.New(t)
	// Start a etcd server.
	servers, _, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Create a proxy to etcd1.
	proxyAddr := tempurl.Alloc()
	var enableDiscard atomic.Bool
	go proxyWithDiscard(re, cfg1.LCUrls[0].String(), proxyAddr, &enableDiscard)

	// Create a etcd client with etcd1 as endpoint.
	urls, err := types.NewURLs([]string{proxyAddr})
	re.NoError(err)
	client1, err := CreateEtcdClient(nil, urls)
	re.NoError(err)
	defer client1.Close()

	// Add a new member
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})
	time.Sleep(1 * time.Second) // wait for etcd client sync endpoints

	// Hang the etcd1 and wait for the client to connect to etcd2.
	enableDiscard.Store(true)
	time.Sleep(time.Second)
	_, err = EtcdKVGet(client1, "test/key1")
	return err
}

func proxyWithDiscard(re *require.Assertions, server, proxy string, enableDiscard *atomic.Bool) {
	server = strings.TrimPrefix(server, "http://")
	proxy = strings.TrimPrefix(proxy, "http://")
	l, err := net.Listen("tcp", proxy)
	re.NoError(err)
	for {
		connect, err := l.Accept()
		re.NoError(err)
		go func(connect net.Conn) {
			serverConnect, err := net.Dial("tcp", server)
			re.NoError(err)
			pipe(connect, serverConnect, enableDiscard)
		}(connect)
	}
}

func pipe(src net.Conn, dst net.Conn, enableDiscard *atomic.Bool) {
	errChan := make(chan error, 1)
	go func() {
		err := ioCopy(src, dst, enableDiscard)
		errChan <- err
	}()
	go func() {
		err := ioCopy(dst, src, enableDiscard)
		errChan <- err
	}()
	<-errChan
	dst.Close()
	src.Close()
}

func ioCopy(dst io.Writer, src io.Reader, enableDiscard *atomic.Bool) (err error) {
	buffer := make([]byte, 32*1024)
	for {
		if enableDiscard.Load() {
			io.Copy(io.Discard, src)
		}
		readNum, errRead := src.Read(buffer)
		if readNum > 0 {
			writeNum, errWrite := dst.Write(buffer[:readNum])
			if errWrite != nil {
				return errWrite
			}
			if readNum != writeNum {
				return io.ErrShortWrite
			}
		}
		if errRead != nil {
			err = errRead
			break
		}
	}
	return err
}

type loopWatcherTestSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	cleans []func()
	etcd   *embed.Etcd
	client *clientv3.Client
	config *embed.Config
}

func TestLoopWatcherTestSuite(t *testing.T) {
	suite.Run(t, new(loopWatcherTestSuite))
}

func (suite *loopWatcherTestSuite) SetupSuite() {
	var err error
	t := suite.T()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cleans = make([]func(), 0)
	// Start a etcd server and create a client with etcd1 as endpoint.
	suite.config = newTestSingleConfig(t)
	suite.startEtcd()
	suite.client, err = CreateEtcdClient(nil, suite.config.LCUrls)
	suite.NoError(err)
	suite.cleans = append(suite.cleans, func() {
		suite.client.Close()
	})
}

func (suite *loopWatcherTestSuite) TearDownSuite() {
	suite.cancel()
	suite.wg.Wait()
	for _, clean := range suite.cleans {
		clean()
	}
}

func (suite *loopWatcherTestSuite) TestLoadWithoutKey() {
	cache := struct {
		syncutil.RWMutex
		data map[string]struct{}
	}{
		data: make(map[string]struct{}),
	}
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestLoadWithoutKey",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache.Lock()
			defer cache.Unlock()
			cache.data[string(kv.Key)] = struct{}{}
			return nil
		},
		func(kv *mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	suite.NoError(err) // although no key, watcher returns no error
	cache.RLock()
	defer cache.RUnlock()
	suite.Len(cache.data, 0)
}

func (suite *loopWatcherTestSuite) TestCallBack() {
	cache := struct {
		syncutil.RWMutex
		data map[string]struct{}
	}{
		data: make(map[string]struct{}),
	}
	result := make([]string, 0)
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestCallBack",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			result = append(result, string(kv.Key))
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			cache.Lock()
			defer cache.Unlock()
			delete(cache.data, string(kv.Key))
			return nil
		},
		func([]*clientv3.Event) error {
			cache.Lock()
			defer cache.Unlock()
			for _, r := range result {
				cache.data[r] = struct{}{}
			}
			result = result[:0]
			return nil
		},
		clientv3.WithPrefix(),
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	suite.NoError(err)

	// put 10 keys
	for i := 0; i < 10; i++ {
		suite.put(fmt.Sprintf("TestCallBack%d", i), "")
	}
	time.Sleep(time.Second)
	cache.RLock()
	suite.Len(cache.data, 10)
	cache.RUnlock()

	// delete 10 keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("TestCallBack%d", i)
		_, err = suite.client.Delete(suite.ctx, key)
		suite.NoError(err)
	}
	time.Sleep(time.Second)
	cache.RLock()
	suite.Empty(cache.data)
	cache.RUnlock()
}

func (suite *loopWatcherTestSuite) TestWatcherLoadLimit() {
	for count := 1; count < 10; count++ {
		for limit := 0; limit < 10; limit++ {
			ctx, cancel := context.WithCancel(suite.ctx)
			for i := 0; i < count; i++ {
				suite.put(fmt.Sprintf("TestWatcherLoadLimit%d", i), "")
			}
			cache := struct {
				syncutil.RWMutex
				data []string
			}{
				data: make([]string, 0),
			}
			watcher := NewLoopWatcher(
				ctx,
				&suite.wg,
				suite.client,
				"test",
				"TestWatcherLoadLimit",
				func([]*clientv3.Event) error { return nil },
				func(kv *mvccpb.KeyValue) error {
					cache.Lock()
					defer cache.Unlock()
					cache.data = append(cache.data, string(kv.Key))
					return nil
				},
				func(kv *mvccpb.KeyValue) error {
					return nil
				},
				func([]*clientv3.Event) error {
					return nil
				},
				clientv3.WithPrefix(),
			)
			watcher.StartWatchLoop()
			err := watcher.WaitLoad()
			suite.NoError(err)
			cache.RLock()
			suite.Len(cache.data, count)
			cache.RUnlock()
			cancel()
		}
	}
}

func (suite *loopWatcherTestSuite) TestWatcherBreak() {
	cache := struct {
		syncutil.RWMutex
		data string
	}{}
	checkCache := func(expect string) {
		testutil.Eventually(suite.Require(), func() bool {
			cache.RLock()
			defer cache.RUnlock()
			return cache.data == expect
		}, testutil.WithWaitFor(time.Second))
	}

	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestWatcherBreak",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			if string(kv.Key) == "TestWatcherBreak" {
				cache.Lock()
				defer cache.Unlock()
				cache.data = string(kv.Value)
			}
			return nil
		},
		func(kv *mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
	)
	watcher.watchChangeRetryInterval = 100 * time.Millisecond
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	suite.NoError(err)
	checkCache("")

	// we use close client and update client in failpoint to simulate the network error and recover
	failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient", "return(true)")

	// Case1: restart the etcd server
	suite.etcd.Close()
	suite.startEtcd()
	suite.put("TestWatcherBreak", "0")
	checkCache("0")
	suite.etcd.Server.Stop()
	time.Sleep(DefaultRequestTimeout)
	suite.etcd.Close()
	suite.startEtcd()
	suite.put("TestWatcherBreak", "1")
	checkCache("1")

	// Case2: close the etcd client and put a new value after watcher restarts
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.LCUrls)
	suite.NoError(err)
	watcher.updateClientCh <- suite.client
	suite.put("TestWatcherBreak", "2")
	checkCache("2")

	// Case3: close the etcd client and put a new value before watcher restarts
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.LCUrls)
	suite.NoError(err)
	suite.put("TestWatcherBreak", "3")
	watcher.updateClientCh <- suite.client
	checkCache("3")

	// Case4: close the etcd client and put a new value with compact
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.LCUrls)
	suite.NoError(err)
	suite.put("TestWatcherBreak", "4")
	resp, err := EtcdKVGet(suite.client, "TestWatcherBreak")
	suite.NoError(err)
	revision := resp.Header.Revision
	resp2, err := suite.etcd.Server.Compact(suite.ctx, &etcdserverpb.CompactionRequest{Revision: revision})
	suite.NoError(err)
	suite.Equal(revision, resp2.Header.Revision)
	watcher.updateClientCh <- suite.client
	checkCache("4")

	// Case5: there is an error data in cache
	cache.Lock()
	cache.data = "error"
	cache.Unlock()
	watcher.ForceLoad()
	checkCache("4")

	failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient")
}

func (suite *loopWatcherTestSuite) TestWatcherRequestProgress() {
	checkWatcherRequestProgress := func(injectWatchChanBlock bool) {
		fname := testutil.InitTempFileLogger("debug")
		defer os.RemoveAll(fname)

		watcher := NewLoopWatcher(
			suite.ctx,
			&suite.wg,
			suite.client,
			"test",
			"TestWatcherChanBlock",
			func([]*clientv3.Event) error { return nil },
			func(kv *mvccpb.KeyValue) error { return nil },
			func(kv *mvccpb.KeyValue) error { return nil },
			func([]*clientv3.Event) error { return nil },
		)

		suite.wg.Add(1)
		go func() {
			defer suite.wg.Done()
			watcher.watch(suite.ctx, 0)
		}()

		if injectWatchChanBlock {
			failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)")
			testutil.Eventually(suite.Require(), func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watch channel is blocked for a long time")
			})
			failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock")
		} else {
			testutil.Eventually(suite.Require(), func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watcher receives progress notify in watch loop")
			})
		}
	}
	checkWatcherRequestProgress(false)
	checkWatcherRequestProgress(true)
}

func (suite *loopWatcherTestSuite) startEtcd() {
	etcd1, err := embed.StartEtcd(suite.config)
	suite.NoError(err)
	suite.etcd = etcd1
	<-etcd1.Server.ReadyNotify()
	suite.cleans = append(suite.cleans, func() {
		suite.etcd.Close()
	})
}

func (suite *loopWatcherTestSuite) put(key, value string) {
	kv := clientv3.NewKV(suite.client)
	_, err := kv.Put(suite.ctx, key, value)
	suite.NoError(err)
	resp, err := kv.Get(suite.ctx, key)
	suite.NoError(err)
	suite.Equal(value, string(resp.Kvs[0].Value))
}
