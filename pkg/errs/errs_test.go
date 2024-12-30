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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errs

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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
	m := string(p)
	w.messages = append(w.messages, m)
	return n, nil
}

func (*testingWriter) Sync() error {
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
	lg, _, _ := log.InitLoggerWithWriteSyncer(cfg, writer, writer, opts...)

	return verifyLogger{
		Logger: lg,
		w:      writer,
	}
}

func TestError(t *testing.T) {
	re := require.New(t)
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf)
	log.ReplaceGlobals(lg.Logger, nil)

	rfc := `[error="[PD:member:ErrEtcdLeaderNotFound]etcd leader not found`
	log.Error("test", zap.Error(ErrEtcdLeaderNotFound.FastGenByArgs()))
	re.Contains(lg.Message(), rfc)
	err := errors.New("test error")
	// use Info() because of no stack for comparing.
	log.Info("test", ZapError(ErrEtcdLeaderNotFound, err))
	rfc = `[error="[PD:member:ErrEtcdLeaderNotFound]etcd leader not found: test error`
	m1 := lg.Message()
	re.Contains(m1, rfc)
	log.Info("test", zap.Error(ErrEtcdLeaderNotFound.Wrap(err)))
	m2 := lg.Message()
	idx1 := strings.Index(m1, "[error")
	idx2 := strings.Index(m2, "[error")
	re.Equal(m1[idx1:], m2[idx2:])
	log.Info("test", zap.Error(ErrEtcdLeaderNotFound.Wrap(err).FastGenWithCause()))
	m3 := lg.Message()
	re.NotContains(m3, rfc)
}

func TestErrorEqual(t *testing.T) {
	re := require.New(t)
	err1 := ErrSchedulerNotFound.FastGenByArgs()
	err2 := ErrSchedulerNotFound.FastGenByArgs()
	re.True(errors.ErrorEqual(err1, err2))

	err := errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err)
	err2 = ErrSchedulerNotFound.Wrap(err)
	re.True(errors.ErrorEqual(err1, err2))

	err1 = ErrSchedulerNotFound.FastGenByArgs()
	err2 = ErrSchedulerNotFound.Wrap(err)
	re.False(errors.ErrorEqual(err1, err2))

	err3 := errors.New("test")
	err4 := errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err3)
	err2 = ErrSchedulerNotFound.Wrap(err4)
	re.True(errors.ErrorEqual(err1, err2))

	err3 = errors.New("test1")
	err4 = errors.New("test")
	err1 = ErrSchedulerNotFound.Wrap(err3)
	err2 = ErrSchedulerNotFound.Wrap(err4)
	re.False(errors.ErrorEqual(err1, err2))
}

func TestZapError(_ *testing.T) {
	err := errors.New("test")
	log.Info("test", ZapError(err))
	err1 := ErrSchedulerNotFound
	log.Info("test", ZapError(err1))
	log.Info("test", ZapError(err1, err))
}

func TestErrorWithStack(t *testing.T) {
	re := require.New(t)
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf)
	log.ReplaceGlobals(lg.Logger, nil)

	_, err := strconv.ParseUint("-42", 10, 64)
	log.Error("test", ZapError(ErrStrconvParseInt.Wrap(err).GenWithStackByCause()))
	m1 := lg.Message()
	log.Error("test", zap.Error(errors.WithStack(err)))
	m2 := lg.Message()
	log.Error("test", ZapError(ErrStrconvParseInt.GenWithStackByCause(), err))
	m3 := lg.Message()
	// This test is based on line number and the first log is in line 141, the second is in line 142.
	// So they have the same length stack. Move this test to another place need to change the corresponding length.
	idx1 := strings.Index(m1, "[stack=")
	re.GreaterOrEqual(idx1, -1)
	idx2 := strings.Index(m2, "[stack=")
	re.GreaterOrEqual(idx2, -1)
	idx3 := strings.Index(m3, "[stack=")
	re.GreaterOrEqual(idx3, -1)
	re.Len(m2[idx2:], len(m1[idx1:]))
	re.Len(m3[idx3:], len(m1[idx1:]))
}
