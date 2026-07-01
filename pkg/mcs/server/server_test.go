// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestInitListenerWithKernelSelectedPort(t *testing.T) {
	re := require.New(t)
	svr := NewBaseServer(context.Background())
	re.NoError(svr.InitListener(&grpcutil.TLSConfig{}, "http://127.0.0.1:0"))
	defer svr.GetListener().Close()

	actual := svr.GetActualListenAddr()
	u, err := url.Parse(actual)
	re.NoError(err)
	re.Equal("http", u.Scheme)
	re.Equal("127.0.0.1", u.Hostname())
	re.NotEmpty(u.Port())
	re.NotEqual("0", u.Port())
}

func TestResolveAdvertiseListenAddr(t *testing.T) {
	actualAddr := "http://127.0.0.1:12345"
	require.Equal(t, actualAddr, ResolveListenAddr("http://127.0.0.1:0", actualAddr))
	require.Equal(t, "http://127.0.0.1:23456", ResolveListenAddr("http://127.0.0.1:23456", actualAddr))

	require.Equal(t, actualAddr, ResolveAdvertiseListenAddr("", actualAddr))
	require.Equal(t, actualAddr, ResolveAdvertiseListenAddr("http://127.0.0.1:0", actualAddr))
	require.Equal(t, actualAddr, ResolveAdvertiseListenAddr("127.0.0.1:0", actualAddr))
	require.Equal(t, "http://127.0.0.1:23456", ResolveAdvertiseListenAddr("http://127.0.0.1:23456", actualAddr))
}
