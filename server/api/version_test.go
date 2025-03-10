// Copyright 2019 TiKV Project Authors.
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

package api

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

func TestGetVersion(t *testing.T) {
	// TODO: enable it.
	t.Skip("Temporary disable. See issue: https://github.com/tikv/pd/issues/1893")
	re := require.New(t)

	fname := filepath.Join(os.TempDir(), "stdout")
	old := os.Stdout
	temp, _ := os.Create(fname)
	os.Stdout = temp

	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	reqCh := make(chan struct{})
	go func() {
		<-reqCh
		time.Sleep(200 * time.Millisecond)
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/version"
		resp, err := testDialClient.Get(addr)
		re.NoError(err)
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
		re.NoError(err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *server.Server)
	go func(cfg *config.Config) {
		s, err := server.CreateServer(ctx, cfg, nil, NewHandler)
		re.NoError(err)
		re.NoError(failpoint.Enable("github.com/tikv/pd/server/memberNil", `return(true)`))
		reqCh <- struct{}{}
		err = s.Run()
		re.NoError(err)
		ch <- s
	}(cfg)

	svr := <-ch
	close(ch)
	out, _ := os.ReadFile(fname)
	re.NotContains(string(out), "PANIC")

	// clean up
	func() {
		temp.Close()
		os.Stdout = old
		os.RemoveAll(fname)
		svr.Close()
		cancel()
		testutil.CleanServer(cfg.DataDir)
	}()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/memberNil"))
}
