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
	"go.etcd.io/etcd/embed"
)

func TestDiscover(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	re.NoError(err)

	<-etcd.Server.ReadyNotify()
	sr1, err := NewServiceRegister(context.Background(), []string{ep}, "test_service", "127.0.0.1:1", "127.0.0.1:1", 1)
	re.NoError(err)
	err = sr1.Register()
	re.NoError(err)
	sr2, err := NewServiceRegister(context.Background(), []string{ep}, "test_service", "127.0.0.1:2", "127.0.0.1:2", 1)
	re.NoError(err)
	err = sr2.Register()
	re.NoError(err)

	endpoints, err := Discover([]string{ep}, "test_service")
	re.NoError(err)
	re.Len(endpoints, 2)
	re.Equal("127.0.0.1:1", endpoints[0])
	re.Equal("127.0.0.1:2", endpoints[1])

	sr1.cancel()
	sr2.cancel()
	time.Sleep(3 * time.Second)
	endpoints, err = Discover([]string{ep}, "test_service")
	re.NoError(err)
	re.Empty(endpoints)
}
