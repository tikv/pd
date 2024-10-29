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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestAddMember(t *testing.T) {
	re := require.New(t)
	servers, client1, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})
}

func TestMemberHelpers(t *testing.T) {
	re := require.New(t)
	servers, client1, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1.Ctx(), client1)
	re.NoError(err)
	re.Len(listResp1.Members, 1)
	// types.ID is an alias of uint64.
	re.Equal(uint64(etcd1.Server.ID()), listResp1.Members[0].ID)

	// Test AddEtcdMember
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})

	// Test CheckClusterID
	urlsMap, err := etcdtypes.NewURLsMap(etcd2.Config().InitialCluster)
	re.NoError(err)
	err = CheckClusterID(etcd1.Server.Cluster().ID(), urlsMap, &tls.Config{MinVersion: tls.VersionTLS12})
	re.NoError(err)

	// Test RemoveEtcdMember
	_, err = RemoveEtcdMember(client1, uint64(etcd2.Server.ID()))
	re.NoError(err)

	listResp3, err := ListEtcdMembers(client1.Ctx(), client1)
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
	// wait for etcd client sync endpoints
	checkEtcdEndpointNum(re, client1, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// remove one member that is not the one we connected to.
	resp, err := ListEtcdMembers(ctx, client1)
	re.NoError(err)

	var memIDToRemove uint64
	for _, m := range resp.Members {
		if m.ID != resp.Header.MemberId {
			memIDToRemove = m.ID
			break
		}
	}

	_, err = RemoveEtcdMember(client1, memIDToRemove)
	re.NoError(err)

	// Check the client can get the new member with the new endpoints.
	checkEtcdEndpointNum(re, client1, 1)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))
}

func checkEtcdEndpointNum(re *require.Assertions, client *clientv3.Client, num int) {
	testutil.Eventually(re, func() bool {
		return len(client.Endpoints()) == num
	})
}

func checkEtcdClientHealth(re *require.Assertions, client *clientv3.Client) {
	testutil.Eventually(re, func() bool {
		return IsHealthy(context.Background(), client)
	})
}

func TestEtcdScaleInAndOut(t *testing.T) {
	re := require.New(t)
	// Start a etcd server.
	servers, _, clean := NewTestEtcdCluster(t, 1)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Create two etcd clients with etcd1 as endpoint.
	client1, err := CreateEtcdClient(nil, cfg1.ListenClientUrls) // execute member change operation with this client
	re.NoError(err)
	defer client1.Close()
	client2, err := CreateEtcdClient(nil, cfg1.ListenClientUrls) // check member change with this client
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
	checkEtcdEndpointNum(re, client1, 3)

	// Randomly kill an etcd server and restart it
	cfgs := []embed.Config{etcds[0].Config(), etcds[1].Config(), etcds[2].Config()}
	for range len(cfgs) * 2 {
		killIndex := rand.Intn(len(etcds))
		etcds[killIndex].Close()
		checkEtcdEndpointNum(re, client1, 2)
		checkEtcdClientHealth(re, client1)
		etcd, err := embed.StartEtcd(&cfgs[killIndex])
		re.NoError(err)
		<-etcd.Server.ReadyNotify()
		etcds[killIndex] = etcd
		checkEtcdEndpointNum(re, client1, 3)
		checkEtcdClientHealth(re, client1)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxyWithDiscard(ctx, re, cfg1.ListenClientUrls[0].String(), proxyAddr, &enableDiscard)

	// Create an etcd client with etcd1 as endpoint.
	urls, err := etcdtypes.NewURLs([]string{proxyAddr})
	re.NoError(err)
	client1, err := CreateEtcdClient(nil, urls)
	re.NoError(err)
	defer client1.Close()

	// Add a new member
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})

	// Hang the etcd1 and wait for the client to connect to etcd2.
	enableDiscard.Store(true)
	time.Sleep(3 * time.Second)
	_, err = EtcdKVGet(client1, "test/key1")
	return err
}

func proxyWithDiscard(ctx context.Context, re *require.Assertions, server, proxy string, enableDiscard *atomic.Bool) {
	server = strings.TrimPrefix(server, "http://")
	proxy = strings.TrimPrefix(proxy, "http://")
	l, err := net.Listen("tcp", proxy)
	re.NoError(err)
	defer l.Close()
	for {
		type accepted struct {
			conn net.Conn
			err  error
		}
		accept := make(chan accepted, 1)
		go func() {
			// closed by `l.Close()`
			conn, err := l.Accept()
			accept <- accepted{conn, err}
		}()

		select {
		case <-ctx.Done():
			return
		case a := <-accept:
			if a.err != nil {
				return
			}
			go func(connect net.Conn) {
				serverConnect, err := net.DialTimeout("tcp", server, 3*time.Second)
				re.NoError(err)
				pipe(ctx, connect, serverConnect, enableDiscard)
			}(a.conn)
		}
	}
}

func pipe(ctx context.Context, src net.Conn, dst net.Conn, enableDiscard *atomic.Bool) {
	errChan := make(chan error, 1)
	go func() {
		err := ioCopy(ctx, src, dst, enableDiscard)
		errChan <- err
	}()
	go func() {
		err := ioCopy(ctx, dst, src, enableDiscard)
		errChan <- err
	}()
	<-errChan
	dst.Close()
	src.Close()
}

func ioCopy(ctx context.Context, dst io.Writer, src io.Reader, enableDiscard *atomic.Bool) error {
	buffer := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if enableDiscard.Load() {
				_, err := io.Copy(io.Discard, src)
				return err
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
				return errRead
			}
		}
	}
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
	re := suite.Require()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cleans = make([]func(), 0)
	// Start a etcd server and create a client with etcd1 as endpoint.
	suite.config = NewTestSingleConfig()
	suite.config.Dir = suite.T().TempDir()
	suite.startEtcd(re)
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls)
	re.NoError(err)
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

func (suite *loopWatcherTestSuite) TestLoadNoExistedKey() {
	re := suite.Require()
	cache := make(map[string]struct{})
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestLoadNoExistedKey",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache[string(kv.Key)] = struct{}{}
			return nil
		},
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		false, /* withPrefix */
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	re.NoError(err) // although no key, watcher returns no error
	re.Empty(cache)
}

func (suite *loopWatcherTestSuite) TestLoadWithLimitChange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/meetEtcdError", `return()`))
	cache := make(map[string]struct{})
	testutil.GenerateTestDataConcurrently(int(maxLoadBatchSize)*2, func(i int) {
		suite.put(re, fmt.Sprintf("TestLoadWithLimitChange%d", i), "")
	})
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestLoadWithLimitChange",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache[string(kv.Key)] = struct{}{}
			return nil
		},
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	re.NoError(err)
	re.Len(cache, int(maxLoadBatchSize)*2)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/meetEtcdError"))
}

func (suite *loopWatcherTestSuite) TestCallBack() {
	re := suite.Require()
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
		true, /* withPrefix */
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	re.NoError(err)

	// put 10 keys
	for i := range 10 {
		suite.put(re, fmt.Sprintf("TestCallBack%d", i), "")
	}
	time.Sleep(time.Second)
	cache.RLock()
	re.Len(cache.data, 10)
	cache.RUnlock()

	// delete 10 keys
	for i := range 10 {
		key := fmt.Sprintf("TestCallBack%d", i)
		_, err = suite.client.Delete(suite.ctx, key)
		re.NoError(err)
	}
	time.Sleep(time.Second)
	cache.RLock()
	re.Empty(cache.data)
	cache.RUnlock()
}

func (suite *loopWatcherTestSuite) TestWatcherLoadLimit() {
	re := suite.Require()
	for count := 1; count < 10; count++ {
		for limit := range 10 {
			ctx, cancel := context.WithCancel(suite.ctx)
			for i := range count {
				suite.put(re, fmt.Sprintf("TestWatcherLoadLimit%d", i), "")
			}
			cache := make([]string, 0)
			watcher := NewLoopWatcher(
				ctx,
				&suite.wg,
				suite.client,
				"test",
				"TestWatcherLoadLimit",
				func([]*clientv3.Event) error { return nil },
				func(kv *mvccpb.KeyValue) error {
					cache = append(cache, string(kv.Key))
					return nil
				},
				func(*mvccpb.KeyValue) error {
					return nil
				},
				func([]*clientv3.Event) error {
					return nil
				},
				true, /* withPrefix */
			)
			watcher.SetLoadBatchSize(int64(limit))
			watcher.StartWatchLoop()
			err := watcher.WaitLoad()
			re.NoError(err)
			re.Len(cache, count)
			cancel()
		}
	}
}

func (suite *loopWatcherTestSuite) TestWatcherLoadLargeKey() {
	re := suite.Require()
	// use default limit to test 65536 key in etcd
	count := 65536
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	testutil.GenerateTestDataConcurrently(count, func(i int) {
		suite.put(re, fmt.Sprintf("TestWatcherLoadLargeKey/test-%d", i), "")
	})
	cache := make([]string, 0)
	watcher := NewLoopWatcher(
		ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestWatcherLoadLargeKey",
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache = append(cache, string(kv.Key))
			return nil
		},
		func(*mvccpb.KeyValue) error {
			return nil
		},
		func([]*clientv3.Event) error {
			return nil
		},
		true, /* withPrefix */
	)
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	re.NoError(err)
	re.Len(cache, count)
}

func (suite *loopWatcherTestSuite) TestWatcherBreak() {
	re := suite.Require()
	cache := struct {
		syncutil.RWMutex
		data string
	}{}
	checkCache := func(expect string) {
		testutil.Eventually(re, func() bool {
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
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		false, /* withPrefix */
	)
	watcher.watchChangeRetryInterval = 100 * time.Millisecond
	watcher.StartWatchLoop()
	err := watcher.WaitLoad()
	re.NoError(err)
	checkCache("")

	// we use close client and update client in failpoint to simulate the network error and recover
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient", "return(true)"))

	// Case1: restart the etcd server
	suite.etcd.Close()
	suite.startEtcd(re)
	suite.put(re, "TestWatcherBreak", "0")
	checkCache("0")
	suite.etcd.Server.Stop()
	time.Sleep(DefaultRequestTimeout)
	suite.etcd.Close()
	suite.startEtcd(re)
	suite.put(re, "TestWatcherBreak", "1")
	checkCache("1")

	// Case2: close the etcd client and put a new value after watcher restarts
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls)
	re.NoError(err)
	watcher.updateClientCh <- suite.client
	suite.put(re, "TestWatcherBreak", "2")
	checkCache("2")

	// Case3: close the etcd client and put a new value before watcher restarts
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls)
	re.NoError(err)
	suite.put(re, "TestWatcherBreak", "3")
	watcher.updateClientCh <- suite.client
	checkCache("3")

	// Case4: close the etcd client and put a new value with compact
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls)
	re.NoError(err)
	suite.put(re, "TestWatcherBreak", "4")
	resp, err := EtcdKVGet(suite.client, "TestWatcherBreak")
	re.NoError(err)
	revision := resp.Header.Revision
	resp2, err := suite.etcd.Server.Compact(suite.ctx, &etcdserverpb.CompactionRequest{Revision: revision})
	re.NoError(err)
	re.Equal(revision, resp2.Header.Revision)
	watcher.updateClientCh <- suite.client
	checkCache("4")

	// Case5: there is an error data in cache
	cache.Lock()
	cache.data = "error"
	cache.Unlock()
	watcher.ForceLoad()
	checkCache("4")

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient"))
}

func (suite *loopWatcherTestSuite) TestWatcherRequestProgress() {
	re := suite.Require()
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
			func(*mvccpb.KeyValue) error { return nil },
			func(*mvccpb.KeyValue) error { return nil },
			func([]*clientv3.Event) error { return nil },
			false, /* withPrefix */
		)
		watcher.watchChTimeoutDuration = 2 * RequestProgressInterval

		suite.wg.Add(1)
		go func() {
			defer suite.wg.Done()
			watcher.watch(suite.ctx, 0)
		}()

		if injectWatchChanBlock {
			failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)")
			testutil.Eventually(re, func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watch channel is blocked for a long time")
			})
			failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock")
		} else {
			testutil.Eventually(re, func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watcher receives progress notify in watch loop")
			})
		}
	}
	checkWatcherRequestProgress(false)
	checkWatcherRequestProgress(true)
}

func (suite *loopWatcherTestSuite) startEtcd(re *require.Assertions) {
	etcd1, err := embed.StartEtcd(suite.config)
	re.NoError(err)
	suite.etcd = etcd1
	<-etcd1.Server.ReadyNotify()
	suite.cleans = append(suite.cleans, func() {
		if suite.etcd.Server != nil {
			select {
			case _, ok := <-suite.etcd.Err():
				if !ok {
					return
				}
			default:
			}
			suite.etcd.Close()
		}
	})
}

func (suite *loopWatcherTestSuite) put(re *require.Assertions, key, value string) {
	kv := clientv3.NewKV(suite.client)
	_, err := kv.Put(suite.ctx, key, value)
	re.NoError(err)
	resp, err := kv.Get(suite.ctx, key)
	re.NoError(err)
	re.Equal(value, string(resp.Kvs[0].Value))
}
