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
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRegister(t *testing.T) {
	re := require.New(t)
	servers, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	etcd, cfg := servers[0], servers[0].Config()

	// Test register with http prefix.
	sr := NewServiceRegister(context.Background(), client, "test_service", "http://127.0.0.1:1", "http://127.0.0.1:1", 10)
	err := sr.Register()
	re.NoError(err)
	re.Equal("/ms/0/test_service/registry/http://127.0.0.1:1", sr.key)
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
	sr = NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:2", "127.0.0.1:2", 1)
	err = sr.Register()
	re.NoError(err)
	sr.cancel()
	re.Empty(getKeyAfterLeaseExpired(re, client, sr.key))

	// Test the case that keepalive is failed when the etcd is restarted.
	sr = NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:2", "127.0.0.1:2", 1)
	err = sr.Register()
	re.NoError(err)
	fname := testutil.InitTempFileLogger("info")
	defer os.Remove(fname)
	for i := range 3 {
		re.Equal("127.0.0.1:2", getKeyAfterLeaseExpired(re, client, sr.key))
		etcd.Server.HardStop() // close the etcd to make the keepalive failed
		// ensure that the request is timeout
		testutil.Eventually(re, func() bool {
			content, _ := os.ReadFile(fname)
			// check log in function `ServiceRegister.Register`
			// ref https://github.com/tikv/pd/blob/6377b26e4e879e7623fbc1d0b7f1be863dea88ad/pkg/mcs/discovery/register.go#L77
			// need to both contain `register.go` and `keep alive failed`
			pattern := regexp.MustCompile(`register.go.*keep alive failed`)
			matches := pattern.FindAll(content, -1)
			return len(matches) >= i+1
		})
		etcd.Close()
		etcd, err = embed.StartEtcd(&cfg)
		re.NoError(err)
		<-etcd.Server.ReadyNotify()
		testutil.Eventually(re, func() bool {
			return getKeyAfterLeaseExpired(re, client, sr.key) == "127.0.0.1:2"
		})
	}
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
