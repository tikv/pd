// Copyright 2023 TiKV Project Authors.
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

package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestRegister(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	ep := cfg.LCUrls[0].String()
	client, err := clientv3.NewFromURL(ep)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	// Test register with http prefix.
	sr := NewServiceRegister(context.Background(), client, "12345", "test_service", "http://127.0.0.1:1", "http://127.0.0.1:1", 10)
	re.NoError(err)
	err = sr.Register()
	re.NoError(err)
	re.Equal("/ms/12345/test_service/registry/http://127.0.0.1:1", sr.key)
	resp, err := client.Get(context.Background(), sr.key)
	re.NoError(err)
	re.Equal("http://127.0.0.1:1", string(resp.Kvs[0].Value))

	// Test deregister.
	err = sr.Deregister()
	re.NoError(err)
	resp, err = client.Get(context.Background(), sr.key)
	re.NoError(err)
	re.Empty(resp.Kvs)

	// Test the case that ctx is canceled.
	sr = NewServiceRegister(context.Background(), client, "12345", "test_service", "127.0.0.1:2", "127.0.0.1:2", 1)
	err = sr.Register()
	re.NoError(err)
	sr.cancel()
	re.Empty(getKeyAfterLeaseExpired(re, client, sr.key))

	// Test the case that keepalive is failed when the etcd is restarted.
	sr = NewServiceRegister(context.Background(), client, "12345", "test_service", "127.0.0.1:2", "127.0.0.1:2", 1)
	err = sr.Register()
	re.NoError(err)
	for i := 0; i < 3; i++ {
		re.Equal("127.0.0.1:2", getKeyAfterLeaseExpired(re, client, sr.key))
		etcd.Server.HardStop()                  // close the etcd to make the keepalive failed
		time.Sleep(etcdutil.DefaultDialTimeout) // ensure that the request is timeout
		etcd.Close()
		etcd, err = embed.StartEtcd(cfg)
		re.NoError(err)
		<-etcd.Server.ReadyNotify()
		testutil.Eventually(re, func() bool {
			return getKeyAfterLeaseExpired(re, client, sr.key) == "127.0.0.1:2"
		})
	}
	etcd.Close()
}

func getKeyAfterLeaseExpired(re *require.Assertions, client *clientv3.Client, key string) string {
	time.Sleep(3 * time.Second) // ensure that the lease is expired
	resp, err := client.Get(context.Background(), key)
	re.NoError(err)
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}
