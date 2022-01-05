// Copyright 2021 TiKV Project Authors.
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

package pdctl

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	cmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"go.uber.org/zap"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&globalTestSuite{})

type globalTestSuite struct{}

func (s *globalTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *globalTestSuite) TestSendAndGetComponent(c *C) {
	handler := func(ctx context.Context, s *server.Server) (http.Handler, server.ServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
			component := apiutil.GetComponentNameOnHTTP(r)
			for k := range r.Header {
				log.Info("header", zap.String("key", k))
			}
			log.Info("component", zap.String("component", component))
			c.Assert(component, Equals, "pdctl")
			fmt.Fprint(w, component)
		})
		info := server.ServiceGroup{
			IsCore: true,
		}
		return mux, info, nil
	}
	cfg := server.NewTestSingleConfig(checkerWithNilAssert(c))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, handler)
	c.Assert(err, IsNil)
	err = svr.Run()
	c.Assert(err, IsNil)
	pdAddr := svr.GetAddr()
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()

	cmd := cmd.GetRootCmd()
	args := []string{"-u", pdAddr, "health"}
	output, err := ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(string(output), Equals, "pdctl\n")
}
