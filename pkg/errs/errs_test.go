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

package errs

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// testingWriter is a WriteSyncer that writes to the the messages.
type testingWriter struct {
	messages []string
}

func newTestingWriter() *testingWriter {
	return &testingWriter{}
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	p = bytes.TrimRight(p, "\n")
	m := fmt.Sprintf("%s", p)
	w.messages = append(w.messages, m)
	return n, nil
}

func (w *testingWriter) Sync() error {
	return nil
}

type verifyLogger struct {
	*zap.Logger
	w *testingWriter
}

func (logger *verifyLogger) Message() string {
	if logger.w.messages == nil {
		return ""
	}
	return logger.w.messages[len(logger.w.messages)-1]
}

func newZapTestLogger(cfg *log.Config, opts ...zap.Option) verifyLogger {
	// TestingWriter is used to write to memory.
	// Used in the verify logger.
	writer := newTestingWriter()
	lg, _, _ := log.InitLoggerWithWriteSyncer(cfg, writer, opts...)

	return verifyLogger{
		Logger: lg,
		w:      writer,
	}
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testErrorSuite{})

type testErrorSuite struct{}

func (s *testErrorSuite) TestError(c *C) {
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf)
	log.ReplaceGlobals(lg.Logger, nil)

	rfc := `[error="[PD:tso:ErrInvalidTimestamp]invalid timestamp"]`
	log.Error("test", zap.Error(ErrInvalidTimestamp.FastGenByArgs()))
	c.Assert(strings.Contains(lg.Message(), rfc), IsTrue)
	err := errors.New("test error")
	log.Error("test", ZapError(ErrInvalidTimestamp, err))
	rfc = `[error="[PD:tso:ErrInvalidTimestamp]test error"]`
	c.Assert(strings.Contains(lg.Message(), rfc), IsTrue)
}

func (s *testErrorSuite) TestErrorEqual(c *C) {
	err1 := ErrSchedulerNotFound.FastGenByArgs()
	err2 := ErrSchedulerNotFound.FastGenByArgs()
	c.Assert(errors.ErrorEqual(err1, err2), IsTrue)

	err := errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err).FastGenWithCause()
	err2 = ErrSchedulerNotFound.Wrap(err).FastGenWithCause()
	c.Assert(errors.ErrorEqual(err1, err2), IsTrue)

	err1 = ErrSchedulerNotFound.FastGenByArgs()
	err2 = ErrSchedulerNotFound.Wrap(err).FastGenWithCause()
	c.Assert(errors.ErrorEqual(err1, err2), IsFalse)

	err3 := errors.New("test")
	err4 := errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err3).FastGenWithCause()
	err2 = ErrSchedulerNotFound.Wrap(err4).FastGenWithCause()
	c.Assert(errors.ErrorEqual(err1, err2), IsTrue)

	err3 = errors.New("test1")
	err4 = errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err3).FastGenWithCause()
	err2 = ErrSchedulerNotFound.Wrap(err4).FastGenWithCause()
	c.Assert(errors.ErrorEqual(err1, err2), IsFalse)
}

func (s *testErrorSuite) TestZapError(c *C) {
	err := errors.New("test")
	log.Info("test", ZapError(err))
	err1 := ErrSchedulerNotFound
	log.Info("test", ZapError(err1))
	log.Info("test", ZapError(err1, err))
}

func (s *testErrorSuite) TestErrorWithStack(c *C) {
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf)
	log.ReplaceGlobals(lg.Logger, nil)

	_, err := strconv.ParseUint("-42", 10, 64)
	log.Error("test", ZapError(ErrStrconvParseInt.Wrap(err).GenWithStackByCause()))
	m1 := lg.Message()
	log.Error("test", zap.Error(errors.WithStack(err)))
	m2 := lg.Message()
	// This test is based on line number and the first log is in line 141, the second is in line 142.
	// So they have the same length stack. Move this test to another place need to change the corresponding length.
	c.Assert(len(m1[strings.Index(m1, "[stack="):]), Equals, len(m2[strings.Index(m2, "[stack="):]))
}
