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

package logutil

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = Suite(&testPluggableLoggerSuite{})

type testPluggableLoggerSuite struct {
	testLoggerManager loggerManager
}

func (s *testPluggableLoggerSuite) SetUpSuite(c *C) {
	s.testLoggerManager.Init()
}

func (s *testPluggableLoggerSuite) TestLoggerManager(c *C) {
	// Get a logger that does not exist.
	c.Assert(s.testLoggerManager.GetPluggableLogger("test", false), IsNil)
	// Create PluggableLogger.
	logger := s.testLoggerManager.GetPluggableLogger("test", true)
	c.Assert(logger.GetLogger(), IsNil)
	c.Assert(logger.GetName(), Equals, "test")
	// Plug logger.
	nop := zap.NewNop()
	s.testLoggerManager.GetPluggableLogger("test", false).PlugLogger(nop)
	c.Assert(logger.GetLogger(), NotNil)
	logger.Debug("noop")
	// Unplug logger.
	s.testLoggerManager.GetPluggableLogger("test", false).UnplugLogger(nop)
	c.Assert(logger.GetLogger(), IsNil)
	logger.Debug("noop")
}

func (s *testPluggableLoggerSuite) TestPluggableLogger(c *C) {
	pl := s.testLoggerManager.GetPluggableLogger("foo", true)
	buffer := new(bytes.Buffer)
	lg, _, err := log.InitLoggerWithWriteSyncer(&log.Config{
		Level:               "info",
		DisableTimestamp:    true,
		DisableCaller:       true,
		DisableStacktrace:   true,
		DisableErrorVerbose: true,
	}, zapcore.AddSync(buffer))
	c.Assert(err, IsNil)
	pl.PlugLogger(lg)
	pl.Info("world", zap.Int64("answer", 42))
	c.Assert(lg.Sync(), IsNil)
	c.Assert(buffer.String(), Equals, "[INFO] [world] [answer=42]\n")
	pl.UnplugLogger(lg)
}
