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
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/types"
)

func TestMemberHelpers(t *testing.T) {
	re := require.New(t)
	cfg1 := NewTestSingleConfig(t)
	etcd1, err := embed.StartEtcd(cfg1)
	defer func() {
		etcd1.Close()
	}()
	re.NoError(err)

	ep1 := cfg1.LCUrls[0].String()
	client1, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep1},
	})
	re.NoError(err)

	<-etcd1.Server.ReadyNotify()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp1.Members, 1)
	// types.ID is an alias of uint64.
	re.Equal(uint64(etcd1.Server.ID()), listResp1.Members[0].ID)

	// Test AddEtcdMember
	etcd2 := checkAddEtcdMember(t, cfg1, client1)
	cfg2 := etcd2.Config()
	defer etcd2.Close()
	ep2 := cfg2.LCUrls[0].String()
	client2, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep2},
	})
	re.NoError(err)
	checkMembers(re, client2, []*embed.Etcd{etcd1, etcd2})

	// Test CheckClusterID
	urlsMap, err := types.NewURLsMap(cfg2.InitialCluster)
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
	cfg := NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	kv := clientv3.NewKV(client)
	for i := range keys {
		_, err = kv.Put(context.TODO(), keys[i], vals[i])
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
	cfg := NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl1", "val1", 2)
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
	cfg := NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

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
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/autoSyncInterval", "return(true)"))

	// Start a etcd server.
	cfg1 := NewTestSingleConfig(t)
	etcd1, err := embed.StartEtcd(cfg1)
	re.NoError(err)

	// Create a etcd client with etcd1 as endpoint.
	ep1 := cfg1.LCUrls[0].String()
	urls, err := types.NewURLs([]string{ep1})
	re.NoError(err)
	client1, err := createEtcdClientWithMultiEndpoint(nil, urls)
	re.NoError(err)
	<-etcd1.Server.ReadyNotify()

	// Add a new member.
	etcd2 := checkAddEtcdMember(t, cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})

	// Remove the first member and close the etcd1.
	_, err = RemoveEtcdMember(client1, uint64(etcd1.Server.ID()))
	re.NoError(err)
	time.Sleep(20 * time.Millisecond) // wait for etcd client sync endpoints and client will be connected to etcd2
	etcd1.Close()

	// Check the client can get the new member with the new endpoints.
	listResp3, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp3.Members, 1)
	re.Equal(uint64(etcd2.Server.ID()), listResp3.Members[0].ID)

	require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/autoSyncInterval"))
}

func TestEtcdWithHangLeaderEnableCheck(t *testing.T) {
	re := require.New(t)
	var err error
	// Test with enable check.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/autoSyncInterval", "return(true)"))
	err = checkEtcdWithHangLeader(t)
	re.NoError(err)
	require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/autoSyncInterval"))

	// Test with disable check.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/closeKeepAliveCheck", "return(true)"))
	err = checkEtcdWithHangLeader(t)
	re.Error(err)
	require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/closeKeepAliveCheck"))
}

func checkEtcdWithHangLeader(t *testing.T) error {
	re := require.New(t)
	// Start a etcd server.
	cfg1 := NewTestSingleConfig(t)
	etcd1, err := embed.StartEtcd(cfg1)
	re.NoError(err)
	ep1 := cfg1.LCUrls[0].String()
	<-etcd1.Server.ReadyNotify()

	// Create a proxy to etcd1.
	proxyAddr := tempurl.Alloc()
	var enableDiscard atomic.Bool
	go proxyWithDiscard(re, ep1, proxyAddr, &enableDiscard)

	// Create a etcd client with etcd1 as endpoint.
	urls, err := types.NewURLs([]string{proxyAddr})
	re.NoError(err)
	client1, err := createEtcdClientWithMultiEndpoint(nil, urls)
	re.NoError(err)

	// Add a new member and set the client endpoints to etcd1 and etcd2.
	etcd2 := checkAddEtcdMember(t, cfg1, client1)
	defer etcd2.Close()
	checkMembers(re, client1, []*embed.Etcd{etcd1, etcd2})
	time.Sleep(1 * time.Second) // wait for etcd client sync endpoints

	// Hang the etcd1 and wait for the client to connect to etcd2.
	enableDiscard.Store(true)
	time.Sleep(defaultDialKeepAliveTime + defaultDialKeepAliveTimeout*2)
	_, err = EtcdKVGet(client1, "test/key1")
	return err
}

func checkAddEtcdMember(t *testing.T, cfg1 *embed.Config, client *clientv3.Client) *embed.Etcd {
	re := require.New(t)
	cfg2 := NewTestSingleConfig(t)
	cfg2.Name = genRandName()
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.LPUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	peerURL := cfg2.LPUrls[0].String()
	addResp, err := AddEtcdMember(client, []string{peerURL})
	re.NoError(err)
	etcd2, err := embed.StartEtcd(cfg2)
	re.NoError(err)
	re.Equal(uint64(etcd2.Server.ID()), addResp.Member.ID)
	<-etcd2.Server.ReadyNotify()
	return etcd2
}

func checkMembers(re *require.Assertions, client *clientv3.Client, etcds []*embed.Etcd) {
	// Check the client can get the new member.
	listResp, err := ListEtcdMembers(client)
	re.NoError(err)
	re.Len(listResp.Members, len(etcds))
	inList := func(m *etcdserverpb.Member) bool {
		for _, etcd := range etcds {
			if m.ID == uint64(etcd.Server.ID()) {
				return true
			}
		}
		return false
	}
	for _, m := range listResp.Members {
		re.True(inList(m))
	}
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
