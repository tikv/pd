// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

var _ = Suite(&testLogSuite{})

type testLogSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testLogSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testLogSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testLogSuite) TestSetLogLevel(c *C) {
	level := "error"
	data, err := json.Marshal(level)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/log", data)
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel().String(), Equals, level)
}

func (s *testLogSuite) TestGetLog(c *C) {
	pl := logutil.GetPluggableLogger("api", true)
	answerCh := make(chan int64, 1)
	go func() {
		defer close(answerCh)
		resp := make(map[string]interface{})
		err := readJSON(testDialClient, s.urlPrefix+"/log?name=api&second=5", resp)
		if err != nil {
			return
		}
		if answer, ok := resp["answer"].(json.Number); ok {
			if answer, err := answer.Int64(); err == nil {
				answerCh <- answer
			}
		}
	}()
	time.Sleep(1 * time.Second)
	pl.Info("world", zap.Int("answer", 42))
	answer := <-answerCh
	c.Assert(answer, Equals, int64(42))
}
