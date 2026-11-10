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
	stderrors "errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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

func newZapTestLogger(cfg *log.Config, opts ...zap.Option) (verifyLogger, error) {
	// TestingWriter is used to write to memory.
	// Used in the verify logger.
	writer := newTestingWriter()
	lg, _, err := log.InitLoggerWithWriteSyncer(cfg, writer, writer, opts...)

	return verifyLogger{
		Logger: lg,
		w:      writer,
	}, err
}

func TestError(t *testing.T) {
	re := require.New(t)
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg, err := newZapTestLogger(conf)
	re.NoError(err)
	log.ReplaceGlobals(lg.Logger, nil)

	rfc := `[error="[PD:member:ErrEtcdLeaderNotFound]etcd leader not found`
	log.Error("test", zap.Error(ErrEtcdLeaderNotFound.FastGenByArgs()))
	re.Contains(lg.Message(), rfc)
	err = errors.New("test error")
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
	lg, err := newZapTestLogger(conf)
	re.NoError(err)
	log.ReplaceGlobals(lg.Logger, nil)

	_, err = strconv.ParseUint("-42", 10, 64)
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

type causeOnlyError struct{ error }

// Cause exposes the legacy error chain without implementing Unwrap.
func (e causeOnlyError) Cause() error { return e.error }

func TestHTTPError(t *testing.T) {
	original := stderrors.New("invalid request")
	for _, tc := range []struct {
		name     string
		err      error
		code     string
		status   int
		hasStack bool
	}{
		{"input", NewInvalidInputErr(original), "input", http.StatusBadRequest, false},
		{"repeated input", NewInvalidInputErr(NewInvalidInputErr(original)), "input", http.StatusBadRequest, false},
		{"internal", NewInternalErr(original), "internal", http.StatusInternalServerError, true},
		{"repeated internal", NewInternalErr(NewInternalErr(original)), "internal", http.StatusInternalServerError, true},
		{"preserve internal code", NewInvalidInputErr(NewInternalErr(original)), "internal", http.StatusInternalServerError, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			httpErr := FindHTTPError(tc.err)
			re.NotNil(httpErr)
			re.Equal(tc.code, httpErr.Code())
			re.Equal(tc.status, httpErr.HTTPCode())
			re.Equal(original, httpErr.GetClientData())
			re.ErrorIs(tc.err, original)
			re.Equal(original, errors.Cause(tc.err))
			re.Error(stderrors.Unwrap(tc.err))
			re.Equal(tc.hasStack, errors.HasStack(tc.err))
			if tc.hasStack {
				re.NotEmpty(GetStackTracer(tc.err).StackTrace())
			}
		})
	}
	re := require.New(t)
	re.NoError(NewInvalidInputErr(nil))
	re.NoError(NewInternalErr(nil))
	re.Nil(FindHTTPError(nil))
	re.Nil(GetStackTracer(nil))
}

func TestHTTPErrorChains(t *testing.T) {
	original := errors.New("storage failed")
	for _, tc := range []struct {
		name string
		wrap func(error) error
	}{
		{"cause", func(err error) error { return causeOnlyError{err} }},
		{"unwrap", func(err error) error { return fmt.Errorf("request: %w", err) }},
		{"annotated", func(err error) error { return errors.Annotate(err, "request") }},
		{"cause then unwrap", func(err error) error { return causeOnlyError{fmt.Errorf("request: %w", err)} }},
		{"unwrap then cause", func(err error) error { return fmt.Errorf("request: %w", causeOnlyError{err}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			for _, internal := range []bool{false, true} {
				marked := NewInvalidInputErr(original)
				if internal {
					marked = NewInternalErr(original)
				}
				expected := FindHTTPError(marked)
				found := FindHTTPError(tc.wrap(marked))
				re.NotNil(found)
				re.Equal(expected.Code(), found.Code())
				re.Equal(expected.HTTPCode(), found.HTTPCode())
				re.Equal(original, found.GetClientData())
			}
			wrapped := NewInternalErr(tc.wrap(original))
			re.Implements((*errors.StackTracer)(nil), wrapped)
			re.Equal(errors.GetStackTracer(original).StackTrace(), wrapped.(errors.StackTracer).StackTrace())
			re.Equal(errors.GetStackTracer(original).StackTrace(), GetStackTracer(tc.wrap(wrapped)).StackTrace())
		})
	}
}
