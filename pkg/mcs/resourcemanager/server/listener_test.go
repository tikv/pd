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
)

func TestInitListenerAndUpdateConfigWithKernelSelectedPort(t *testing.T) {
	re := require.New(t)
	cfg := NewConfig()
	cfg.BackendEndpoints = "http://127.0.0.1:2379"
	cfg.ListenAddr = "http://127.0.0.1:0"
	cfg.AdvertiseListenAddr = "http://10.0.0.5:0"
	cfg.Name = cfg.ListenAddr

	cfg, err := GenerateConfig(cfg)
	re.NoError(err)
	svr := CreateServer(context.Background(), cfg)
	re.NoError(svr.initListenerAndUpdateConfig())
	defer func() {
		re.NoError(svr.GetListener().Close())
	}()

	actualURL, err := url.Parse(svr.GetActualListenAddr())
	re.NoError(err)
	re.Equal("127.0.0.1", actualURL.Hostname())
	re.NotEmpty(actualURL.Port())
	re.NotEqual("0", actualURL.Port())
	re.Equal(svr.GetActualListenAddr(), cfg.ListenAddr)

	advertiseURL, err := url.Parse(cfg.AdvertiseListenAddr)
	re.NoError(err)
	re.Equal("10.0.0.5", advertiseURL.Hostname())
	re.Equal(actualURL.Port(), advertiseURL.Port())
	re.Equal(cfg.AdvertiseListenAddr, cfg.Name)
}
