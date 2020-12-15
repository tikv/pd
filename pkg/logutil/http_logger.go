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
	"net/http"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// HTTPLogger is a Logger that outputs logs to ResponseWriter.
type HTTPLogger struct {
	httpWriteSyncer
	logger         *zap.Logger
	closeCallbacks []func()
}

// NewHTTPLogger returns a HTTPLogger.
func NewHTTPLogger(conf *log.Config, w http.ResponseWriter) (*HTTPLogger, error) {
	hl := new(HTTPLogger)
	hl.httpWriteSyncer.Init(w)

	logger, _, err := log.InitLoggerWithWriteSyncer(conf, &hl.httpWriteSyncer, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return nil, err
	}

	hl.logger = logger
	hl.closeCallbacks = make([]func(), 0)
	return hl, nil
}

// AddCloseCallback adds some callbacks when closing.
func (l *HTTPLogger) AddCloseCallback(fs ...func()) {
	l.closeCallbacks = append(l.closeCallbacks, fs...)
}

// Plug will plug the HTTPLogger into PluggableLogger.
func (l *HTTPLogger) Plug(names ...string) error {
	l.AddCloseCallback(func() {
		for _, name := range names {
			pl := GetPluggableLogger(name, false)
			if pl != nil {
				pl.UnplugLogger(l.logger)
			}
		}
	})

	var notExist []string
	for _, name := range names {
		if pl := GetPluggableLogger(name, false); pl != nil {
			pl.PlugLogger(l.logger)
		} else {
			notExist = append(notExist, name)
		}
	}

	if len(notExist) > 0 {
		return errors.Errorf("these names do not exist: %s", strings.Join(notExist, ","))
	}

	return nil
}

// Close will call close callbacks and close all output.
func (l *HTTPLogger) Close() {
	defer l.logger.Sync()

	for _, f := range l.closeCallbacks {
		f()
	}

	l.httpWriteSyncer.Close()
}

// httpWriteSyncer is an implementation similar to zapcore.lockedWriteSyncer.
type httpWriteSyncer struct {
	sync.Mutex
	Writer http.ResponseWriter
	Closed bool
}

func (s *httpWriteSyncer) Init(writer http.ResponseWriter) {
	s.Lock()
	defer s.Unlock()
	writer.WriteHeader(http.StatusOK)
	s.Writer = writer
	s.Closed = false
}

func (s *httpWriteSyncer) Close() {
	s.Lock()
	defer s.Unlock()
	s.Closed = true
}

func (s *httpWriteSyncer) Write(bs []byte) (int, error) {
	s.Lock()
	defer s.Unlock()

	if s.Closed {
		return len(bs), nil
	}

	return s.Writer.Write(bs)
}

func (s *httpWriteSyncer) Sync() error {
	return nil
}
