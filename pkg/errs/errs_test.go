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
	"errors"
	"fmt"
	"strings"
	"testing"

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

func (logger *verifyLogger) Contain(t *testing.T, s string) {
	if logger.w.messages == nil {
		t.Error()
	}
	msg := logger.w.messages[len(logger.w.messages)-1]
	IsContain(t, msg, s)
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

func IsContain(t *testing.T, s1 string, s2 string) {
	if !strings.Contains(s1, s2) {
		t.Error()
	}
}

func TestError(t *testing.T) {
	conf := &log.Config{Level: "debug", File: log.FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf)
	log.ReplaceGlobals(lg.Logger, nil)

	rfc := `[error="[PD:tso:ErrInvalidTimestamp] invalid timestamp"]`
	log.Error("test", zap.Error(ErrInvalidTimestamp.FastGenByArgs()))
	lg.Contain(t, rfc)
	cause := `[cause="test err"]`
	log.Error("test", zap.Error(ErrInvalidTimestamp.FastGenByArgs()), zap.NamedError("cause", errors.New("test err")))
	lg.Contain(t, rfc)
	lg.Contain(t, cause)
}
