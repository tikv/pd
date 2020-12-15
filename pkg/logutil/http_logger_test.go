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
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var _ = Suite(&testHTTPLoggerSuite{})

type testHTTPLoggerSuite struct {
	writer *testResponseWriter
	logger *HTTPLogger
}

func (s *testHTTPLoggerSuite) SetUpTest(c *C) {
	writer := newTestResponseWriter()
	lg, err := NewHTTPLogger(&log.Config{
		Level:               "info",
		DisableTimestamp:    true,
		DisableCaller:       true,
		DisableStacktrace:   true,
		DisableErrorVerbose: true,
	}, writer)
	c.Assert(err, IsNil)

	s.writer = writer
	s.logger = lg
}

func (s *testHTTPLoggerSuite) TestClose(c *C) {
	closed := false
	s.logger.AddCloseCallback(func() {
		closed = true
	})
	s.logger.Close()
	c.Assert(closed, IsTrue)
}

func (s *testHTTPLoggerSuite) TestNotExist(c *C) {
	err := s.logger.Plug("not_exist_1", "not_exist_2")
	c.Assert(err, ErrorMatches, "these names do not exist: not_exist_1,not_exist_2")
}

func (s *testHTTPLoggerSuite) TestLogger(c *C) {
	pl := GetPluggableLogger("http", true)
	c.Assert(s.logger.Plug("http"), IsNil)
	pl.Info("world", zap.Int64("answer", 42))
	s.logger.Close()
	c.Assert(s.writer.GetCode(), Equals, http.StatusOK)
	c.Assert(s.writer.String(), Equals, "[INFO] [world] [answer=42]\n")
}

type testResponseWriter struct {
	*bytes.Buffer
	Code int
}

func newTestResponseWriter() *testResponseWriter {
	return &testResponseWriter{
		Buffer: new(bytes.Buffer),
		Code:   0,
	}
}

func (*testResponseWriter) Header() http.Header {
	return nil
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	w.Code = statusCode
}

func (w *testResponseWriter) GetCode() int {
	return w.Code
}

func (w *testResponseWriter) String() string {
	return w.Buffer.String()
}
