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
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestAddMember(t *testing.T) {
	re := require.New(t)
	servers, client1, clean := NewTestEtcdCluster(t, 1, nil)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})
}

func TestMemberHelpers(t *testing.T) {
	re := require.New(t)
	servers, client1, clean := NewTestEtcdCluster(t, 1, nil)
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
	_, client, clean := NewTestEtcdCluster(t, 1, nil)
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
	_, client, clean := NewTestEtcdCluster(t, 1, nil)
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

	servers, client1, clean := NewTestEtcdCluster(t, 1, nil)
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

	// Use testutil.Eventually to handle potential transient errors during member removal.
	// The removed member will be shut down immediately, which may cause the removal RPC
	// to fail with "server stopped", but we verify success by checking the member list.
	testutil.Eventually(re, func() bool {
		_, err := RemoveEtcdMember(client1, memIDToRemove)
		if err == nil {
			return true
		}
		// Verify if the member was actually removed by checking the member list
		listResp, listErr := ListEtcdMembers(client1.Ctx(), client1)
		return listErr == nil && len(listResp.Members) == 1
	})

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
	servers, _, clean := NewTestEtcdCluster(t, 1, nil)
	defer clean()
	etcd1, cfg1 := servers[0], servers[0].Config()

	// Create two etcd clients with etcd1 as endpoint.
	client1, err := CreateEtcdClient(nil, cfg1.ListenClientUrls, TestEtcdClientPurpose, true) // execute member change operation with this client
	re.NoError(err)
	defer client1.Close()
	client2, err := CreateEtcdClient(nil, cfg1.ListenClientUrls, TestEtcdClientPurpose, true) // check member change with this client
	re.NoError(err)
	defer client2.Close()

	// Add a new member and check members
	etcd2 := MustAddEtcdMember(t, &cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client2, []*embed.Etcd{etcd1, etcd2})

	// Create a client connected to etcd2 to perform the removal
	cfg2 := etcd2.Config()
	client3, err := CreateEtcdClient(nil, cfg2.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	defer client3.Close()

	// scale in etcd1 using client3 (connected to etcd2)
	_, err = RemoveEtcdMember(client3, uint64(etcd1.Server.ID()))
	re.NoError(err)
	checkMembers(re, client3, []*embed.Etcd{etcd2})
}

func TestRandomKillEtcd(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))
	// Start a etcd server.
	etcds, client1, clean := NewTestEtcdCluster(t, 3, nil)
	defer clean()
	checkEtcdEndpointNum(re, client1, 3)

	// Randomly kill an etcd server and restart it
	cfgs := []embed.Config{etcds[0].Config(), etcds[1].Config(), etcds[2].Config()}
	for range len(cfgs) * 2 {
		killIndex := rand.IntN(len(etcds))
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
	err = checkEtcdWithHangLeader(t, true)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))

	// Test with disable check.
	err = checkEtcdWithHangLeader(t, false)
	re.Error(err)
}

func checkEtcdWithHangLeader(t *testing.T, enableChecker bool) error {
	re := require.New(t)
	// Start a etcd server.
	servers, _, clean := NewTestEtcdCluster(t, 1, nil)
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
	client1, err := CreateEtcdClient(nil, urls, TestEtcdClientPurpose, enableChecker)
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

func TestNextCompactionReloadRetryInterval(t *testing.T) {
	re := require.New(t)
	base := time.Second
	maxInterval := 4 * time.Second

	re.Equal(base, nextCompactionReloadRetryInterval(0, base, maxInterval))
	re.Equal(2*base, nextCompactionReloadRetryInterval(base, base, maxInterval))
	re.Equal(maxInterval, nextCompactionReloadRetryInterval(2*base, base, maxInterval))
	re.Equal(maxInterval, nextCompactionReloadRetryInterval(maxInterval, base, maxInterval))
}

func (suite *loopWatcherTestSuite) SetupSuite() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cleans = make([]func(), 0)
	// Start a etcd server and create a client with etcd1 as endpoint.
	suite.config = NewTestEtcdConfig()
	suite.config.Dir = suite.T().TempDir()
	suite.startEtcd(re)
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
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
	ctx, cancel := context.WithCancel(suite.ctx)
	var watcherWG sync.WaitGroup
	defer func() {
		cancel()
		watcherWG.Wait()
	}()
	cache := make(map[string]struct{})
	watcher := NewLoopWatcher(
		ctx,
		&watcherWG,
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

func (suite *loopWatcherTestSuite) TestWatcherLoadReturnsLifecycleErrors() {
	suite.Run("pre callback", func() {
		re := suite.Require()
		preErr := errors.New("pre callback failed")
		postCalled := false
		watcher := NewLoopWatcher(
			suite.ctx,
			&suite.wg,
			suite.client,
			"test",
			"TestWatcherLoadReturnsLifecycleErrors/pre",
			func([]*clientv3.Event) error { return preErr },
			func(*mvccpb.KeyValue) error {
				re.Fail("put callback should not be called")
				return nil
			},
			func(*mvccpb.KeyValue) error { return nil },
			func([]*clientv3.Event) error {
				postCalled = true
				return nil
			},
			true, /* withPrefix */
		)

		_, err := watcher.load(suite.ctx)
		re.ErrorIs(err, preErr)
		re.True(postCalled)
	})

	suite.Run("post callback", func() {
		re := suite.Require()
		postErr := errors.New("post callback failed")
		watcher := NewLoopWatcher(
			suite.ctx,
			&suite.wg,
			suite.client,
			"test",
			"TestWatcherLoadReturnsLifecycleErrors/post",
			func([]*clientv3.Event) error { return nil },
			func(*mvccpb.KeyValue) error { return nil },
			func(*mvccpb.KeyValue) error { return nil },
			func([]*clientv3.Event) error { return postErr },
			true, /* withPrefix */
		)

		_, err := watcher.load(suite.ctx)
		re.ErrorIs(err, postErr)
	})
}

func (suite *loopWatcherTestSuite) TestLoadWithLimitChange() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	var watcherWG sync.WaitGroup
	defer func() {
		cancel()
		watcherWG.Wait()
	}()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/meetEtcdError", `return()`))
	cache := make(map[string]struct{})
	testutil.GenerateTestDataConcurrently(int(maxLoadBatchSize)*2, func(i int) {
		suite.put(re, fmt.Sprintf("TestLoadWithLimitChange%d", i), "")
	})
	watcher := NewLoopWatcher(
		ctx,
		&watcherWG,
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
	ctx, cancel := context.WithCancel(suite.ctx)
	var watcherWG sync.WaitGroup
	defer func() {
		cancel()
		watcherWG.Wait()
	}()
	cache := struct {
		syncutil.RWMutex
		data map[string]struct{}
	}{
		data: make(map[string]struct{}),
	}
	result := make([]string, 0)
	watcher := NewLoopWatcher(
		ctx,
		&watcherWG,
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
		_, err = suite.client.Delete(ctx, key)
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
	re.Nil(watcher.loadedKeys)
}

func (suite *loopWatcherTestSuite) TestWatcherLoadPreservesFirstCallbackError() {
	tests := []struct {
		name       string
		batchSize  int64
		errorIndex int
	}{
		{name: "same page", batchSize: 0, errorIndex: 0},
		{name: "middle page", batchSize: 1, errorIndex: 1},
	}
	for _, test := range tests {
		suite.Run(test.name, func() {
			re := suite.Require()
			prefix := fmt.Sprintf("TestWatcherLoadPreservesFirstCallbackError/%s/", strings.ReplaceAll(test.name, " ", "-"))
			for i := range 3 {
				suite.put(re, fmt.Sprintf("%s%d", prefix, i), "")
			}

			firstErr := fmt.Errorf("callback failed at index %d", test.errorIndex)
			errorKey := fmt.Sprintf("%s%d", prefix, test.errorIndex)
			processed := make([]string, 0, 3)
			watcher := NewLoopWatcher(
				suite.ctx,
				&suite.wg,
				suite.client,
				"test",
				prefix,
				func([]*clientv3.Event) error { return nil },
				func(kv *mvccpb.KeyValue) error {
					processed = append(processed, string(kv.Key))
					if string(kv.Key) == errorKey {
						return firstErr
					}
					return nil
				},
				func(*mvccpb.KeyValue) error { return nil },
				func([]*clientv3.Event) error { return nil },
				true, /* withPrefix */
			)
			watcher.SetLoadBatchSize(test.batchSize)
			_, err := watcher.load(suite.ctx)
			re.ErrorIs(err, firstErr)
			re.Len(processed, 3)
		})
	}
}

func (suite *loopWatcherTestSuite) TestWatcherLoadUsesSingleRevision() {
	re := suite.Require()
	const prefix = "TestWatcherLoadUsesSingleRevision/"
	for _, suffix := range []string{"a", "b", "c"} {
		suite.put(re, prefix+suffix, "old")
	}

	values := make(map[string]string)
	updated := false
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		prefix,
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			values[string(kv.Key)] = string(kv.Value)
			if !updated {
				updated = true
				_, err := suite.client.Put(suite.ctx, prefix+"b", "new")
				return err
			}
			return nil
		},
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	watcher.SetLoadBatchSize(1)
	_, err := watcher.load(suite.ctx)
	re.NoError(err)
	re.Equal("old", values[prefix+"a"])
	re.Equal("old", values[prefix+"b"])
	re.Equal("old", values[prefix+"c"])
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
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	watcher.updateClientCh <- suite.client
	suite.put(re, "TestWatcherBreak", "2")
	checkCache("2")

	// Case3: close the etcd client and put a new value before watcher restarts
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	suite.put(re, "TestWatcherBreak", "3")
	watcher.updateClientCh <- suite.client
	checkCache("3")

	// Case4: close the etcd client and put a new value with compact
	suite.client.Close()
	suite.client, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
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

func (suite *loopWatcherTestSuite) TestWatcherReloadsAfterCompaction() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	var watcherWG sync.WaitGroup

	watcherClient, err := CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	var replacementClient *clientv3.Client
	failpointEnabled := false
	defer func() {
		cancel()
		watcherWG.Wait()
		if failpointEnabled {
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient"))
		}
		if replacementClient != nil {
			re.NoError(replacementClient.Close())
		}
		_ = watcherClient.Close()
	}()
	cache := struct {
		syncutil.RWMutex
		data string
	}{}
	checkCache := func(expected string) {
		testutil.Eventually(re, func() bool {
			cache.RLock()
			defer cache.RUnlock()
			return cache.data == expected
		}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(10*time.Millisecond))
	}

	const key = "TestWatcherReloadsAfterCompaction"
	watcher := NewLoopWatcher(
		ctx,
		&watcherWG,
		watcherClient,
		"test",
		key,
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache.Lock()
			defer cache.Unlock()
			cache.data = string(kv.Value)
			return nil
		},
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		false, /* withPrefix */
	)
	watcher.watchChangeRetryInterval = 100 * time.Millisecond
	watcher.StartWatchLoop()
	re.NoError(watcher.WaitLoad())

	_, err = suite.client.Put(ctx, key, "before-compaction")
	re.NoError(err)
	checkCache("before-compaction")

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient", "return(true)"))
	failpointEnabled = true
	re.NoError(watcherClient.Close())

	_, err = suite.client.Put(ctx, key, "after-compaction")
	re.NoError(err)
	advanceResp, err := suite.client.Put(ctx, key+"-advance-revision", "")
	re.NoError(err)
	_, err = suite.etcd.Server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: advanceResp.Header.Revision})
	re.NoError(err)

	replacementClient, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	watcher.updateClientCh <- replacementClient
	checkCache("after-compaction")
}

func (suite *loopWatcherTestSuite) TestWatcherReconcilesDeletedKeysAfterCompaction() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	var watcherWG sync.WaitGroup

	const prefix = "TestWatcherReconcilesDeletedKeysAfterCompaction/"
	keepKey := prefix + "keep"
	deletedKey := prefix + "deleted"
	suite.put(re, keepKey, "keep")
	suite.put(re, deletedKey, "deleted")

	watcherClient, err := CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	var replacementClient *clientv3.Client
	failpointEnabled := false
	defer func() {
		cancel()
		watcherWG.Wait()
		if failpointEnabled {
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient"))
		}
		if replacementClient != nil {
			re.NoError(replacementClient.Close())
		}
		_ = watcherClient.Close()
	}()
	cache := struct {
		syncutil.RWMutex
		data map[string]string
	}{
		data: make(map[string]string),
	}
	checkCache := func(expected map[string]string) {
		testutil.Eventually(re, func() bool {
			cache.RLock()
			defer cache.RUnlock()
			return maps.Equal(cache.data, expected)
		}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(10*time.Millisecond))
	}

	watcher := NewLoopWatcher(
		ctx,
		&watcherWG,
		watcherClient,
		"test",
		prefix,
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache.Lock()
			defer cache.Unlock()
			cache.data[string(kv.Key)] = string(kv.Value)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			cache.Lock()
			defer cache.Unlock()
			delete(cache.data, string(kv.Key))
			return nil
		},
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	watcher.SetReconcileDeletedKeys()
	watcher.watchChangeRetryInterval = 100 * time.Millisecond
	watcher.StartWatchLoop()
	re.NoError(watcher.WaitLoad())
	checkCache(map[string]string{keepKey: "keep", deletedKey: "deleted"})

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/updateClient", "return(true)"))
	failpointEnabled = true
	re.NoError(watcherClient.Close())

	_, err = suite.client.Delete(ctx, deletedKey)
	re.NoError(err)
	advanceResp, err := suite.client.Put(ctx, strings.TrimSuffix(prefix, "/")+"-advance-revision", "")
	re.NoError(err)
	_, err = suite.etcd.Server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: advanceResp.Header.Revision})
	re.NoError(err)

	replacementClient, err = CreateEtcdClient(nil, suite.config.ListenClientUrls, TestEtcdClientPurpose, true)
	re.NoError(err)
	watcher.updateClientCh <- replacementClient
	checkCache(map[string]string{keepKey: "keep"})
}

func (suite *loopWatcherTestSuite) TestWatcherRejectsLoadConfigurationAfterStart() {
	re := suite.Require()
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		"TestWatcherRejectsLoadConfigurationAfterStart/",
		func([]*clientv3.Event) error { return nil },
		func(*mvccpb.KeyValue) error { return nil },
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		true,
	)
	watcher.StartWatchLoop()
	re.Panics(func() { watcher.SetLoadHooks(func() {}, func(error) error { return nil }) })
	re.Panics(watcher.SetReconcileDeletedKeys)
	re.NoError(watcher.WaitLoad())
}

func (suite *loopWatcherTestSuite) TestWatcherRetriesReconciliationAfterPostCallbackFailure() {
	re := suite.Require()
	const prefix = "TestWatcherRetriesReconciliationAfterPostCallbackFailure/"
	key := prefix + "deleted"
	suite.put(re, key, "value")

	cache := make(map[string]string)
	var pending map[string]string
	postErr := errors.New("post callback failed")
	failPost := false
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		prefix,
		func([]*clientv3.Event) error {
			pending = maps.Clone(cache)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			pending[string(kv.Key)] = string(kv.Value)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			delete(pending, string(kv.Key))
			return nil
		},
		func([]*clientv3.Event) error {
			if failPost {
				return postErr
			}
			cache = pending
			return nil
		},
		true, /* withPrefix */
	)
	watcher.SetReconcileDeletedKeys()

	_, err := watcher.load(suite.ctx)
	re.NoError(err)
	re.Equal(map[string]string{key: "value"}, cache)

	_, err = suite.client.Delete(suite.ctx, key)
	re.NoError(err)
	failPost = true
	_, err = watcher.load(suite.ctx)
	re.ErrorIs(err, postErr)
	re.Equal(map[string]string{key: "value"}, cache)

	failPost = false
	_, err = watcher.load(suite.ctx)
	re.NoError(err)
	re.Empty(cache)
}

func (suite *loopWatcherTestSuite) TestWatcherCommitsPartialReconciliationProgress() {
	re := suite.Require()
	const prefix = "TestWatcherCommitsPartialReconciliationProgress/"
	firstKey := prefix + "first"
	secondKey := prefix + "second"
	suite.put(re, firstKey, "first")
	suite.put(re, secondKey, "second")

	cache := make(map[string]string)
	transientErr := errors.New("transient delete failure")
	duplicateDeleteErr := errors.New("delete callback called twice")
	failSecondDelete := true
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		prefix,
		func([]*clientv3.Event) error { return nil },
		func(kv *mvccpb.KeyValue) error {
			cache[string(kv.Key)] = string(kv.Value)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			key := string(kv.Key)
			if key == secondKey && failSecondDelete {
				failSecondDelete = false
				return transientErr
			}
			if _, ok := cache[key]; !ok {
				return duplicateDeleteErr
			}
			delete(cache, key)
			return nil
		},
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	watcher.SetReconcileDeletedKeys()

	_, err := watcher.load(suite.ctx)
	re.NoError(err)
	re.Len(cache, 2)

	_, err = suite.client.Delete(suite.ctx, prefix, clientv3.WithPrefix())
	re.NoError(err)
	_, err = watcher.load(suite.ctx)
	re.ErrorIs(err, transientErr)
	re.Equal(map[string]string{secondKey: "second"}, cache)

	_, err = watcher.load(suite.ctx)
	re.NoError(err)
	re.Empty(cache)
}

func (suite *loopWatcherTestSuite) TestWatcherLoadHooksObserveFinalResult() {
	re := suite.Require()
	const key = "TestWatcherLoadHooksObserveFinalResult"
	suite.put(re, key, "value")

	callbackErr := errors.New("callback failed")
	failPut := false
	preLoadCalls := 0
	postLoadResults := make([]error, 0, 3)
	watcher := NewLoopWatcher(
		suite.ctx,
		&suite.wg,
		suite.client,
		"test",
		key,
		func([]*clientv3.Event) error { return nil },
		func(*mvccpb.KeyValue) error {
			if failPut {
				return callbackErr
			}
			return nil
		},
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		false, /* withPrefix */
	)
	watcher.SetLoadHooks(
		func() { preLoadCalls++ },
		func(loadErr error) error {
			postLoadResults = append(postLoadResults, loadErr)
			return nil
		},
	)

	_, err := watcher.load(suite.ctx)
	re.NoError(err)
	failPut = true
	_, err = watcher.load(suite.ctx)
	re.ErrorIs(err, callbackErr)
	canceledCtx, cancel := context.WithCancel(suite.ctx)
	cancel()
	_, err = watcher.load(canceledCtx)
	re.NoError(err)

	re.Equal(3, preLoadCalls)
	re.Len(postLoadResults, 3)
	re.NoError(postLoadResults[0])
	re.ErrorIs(postLoadResults[1], callbackErr)
	re.ErrorIs(postLoadResults[2], context.Canceled)
}

func (suite *loopWatcherTestSuite) TestWatcherRetriesWatchDeleteAfterPostCallbackFailure() {
	re := suite.Require()
	const prefix = "TestWatcherRetriesWatchDeleteAfterPostCallbackFailure/"
	key := prefix + "deleted"
	suite.put(re, key, "value")

	ctx, cancel := context.WithCancel(suite.ctx)
	cache := make(map[string]string)
	var pending map[string]string
	postErr := errors.New("post callback failed")
	failPost := false
	watcher := NewLoopWatcher(
		ctx,
		&suite.wg,
		suite.client,
		"test",
		prefix,
		func([]*clientv3.Event) error {
			pending = maps.Clone(cache)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			pending[string(kv.Key)] = string(kv.Value)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			delete(pending, string(kv.Key))
			return nil
		},
		func([]*clientv3.Event) error {
			if failPost {
				cancel()
				return postErr
			}
			cache = pending
			return nil
		},
		true, /* withPrefix */
	)
	watcher.SetReconcileDeletedKeys()

	revision, err := watcher.load(ctx)
	re.NoError(err)
	re.Equal(map[string]string{key: "value"}, cache)

	failPost = true
	_, err = suite.client.Delete(suite.ctx, key)
	re.NoError(err)
	_, err = watcher.watch(ctx, revision)
	re.NoError(err)
	re.Equal(map[string]string{key: "value"}, cache)

	failPost = false
	_, err = watcher.load(suite.ctx)
	re.NoError(err)
	re.Empty(cache)
}

func (suite *loopWatcherTestSuite) TestWatcherBacksOffFailedCompactionReload() {
	re := suite.Require()
	ctx, cancel := context.WithTimeout(suite.ctx, 3*time.Second)
	defer cancel()

	const key = "TestWatcherBacksOffFailedCompactionReload"
	suite.put(re, key, "invalid")
	resp, err := EtcdKVGet(suite.client, key)
	re.NoError(err)
	watchRevision := resp.Header.Revision
	advanceResp, err := suite.client.Put(ctx, key+"-advance-revision", "")
	re.NoError(err)
	_, err = suite.etcd.Server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: advanceResp.Header.Revision})
	re.NoError(err)

	callbackErr := errors.New("callback failed")
	watcher := NewLoopWatcher(
		ctx,
		&sync.WaitGroup{},
		suite.client,
		"test",
		key,
		func([]*clientv3.Event) error { return nil },
		func(*mvccpb.KeyValue) error { return callbackErr },
		func(*mvccpb.KeyValue) error { return nil },
		func([]*clientv3.Event) error { return nil },
		false, /* withPrefix */
	)
	watcher.watchChangeRetryInterval = 50 * time.Millisecond

	_, err = watcher.watch(ctx, watchRevision)
	re.ErrorIs(err, callbackErr)
	re.Equal(50*time.Millisecond, watcher.compactionReloadRetryInterval)

	retryStart := time.Now()
	_, err = watcher.watch(ctx, watchRevision)
	re.ErrorIs(err, callbackErr)
	re.GreaterOrEqual(time.Since(retryStart), 45*time.Millisecond)
	re.Equal(100*time.Millisecond, watcher.compactionReloadRetryInterval)
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
			_, err := watcher.watch(suite.ctx, 0)
			re.NoError(err)
		}()

		if injectWatchChanBlock {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
			testutil.Eventually(re, func() bool {
				b, err := os.ReadFile(fname)
				re.NoError(err)
				l := string(b)
				return strings.Contains(l, "watch channel is blocked for a long time")
			})
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
		} else {
			testutil.Eventually(re, func() bool {
				b, err := os.ReadFile(fname)
				re.NoError(err)
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

func TestWriteKeyToFile(t *testing.T) {
	re := require.New(t)
	tempFile, err := os.CreateTemp("", "testfile")
	re.NoError(err)
	defer os.Remove(tempFile.Name())

	key := "test/key123"
	op := "get"
	err = writeKeyToFile(tempFile.Name(), key, op)
	re.NoError(err)

	content, err := os.ReadFile(tempFile.Name())
	re.NoError(err)
	expectedContent := "test/key get\n"
	re.Equal(expectedContent, string(content))
}

func TestWriteKeyToFileMultipleKeys(t *testing.T) {
	re := require.New(t)
	tempFile, err := os.CreateTemp("", "testfile")
	re.NoError(err)
	defer os.Remove(tempFile.Name())

	keys := []string{"test/key123", "another/key456", "key789"}
	op := "put"
	for _, key := range keys {
		err = writeKeyToFile(tempFile.Name(), key, op)
		re.NoError(err)
	}

	content, err := os.ReadFile(tempFile.Name())
	re.NoError(err)
	expectedContent := "test/key put\nanother/key put\nkey put\n"
	re.Equal(expectedContent, string(content))
}

func TestWriteKeyToFileError(t *testing.T) {
	re := require.New(t)
	invalidFilePath := "/invalid/path/testfile"
	key := "test/key123"
	op := "get"
	err := writeKeyToFile(invalidFilePath, key, op)
	re.Error(err)
}
