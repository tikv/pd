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

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// HTTPLogger is a Logger that outputs logs to ResponseWriter.
type HTTPLogger struct {
	writer         http.ResponseWriter
	logger         *zap.Logger
	closeCallbacks []func()
}

// NewHTTPLogger returns a HTTPLogger.
func NewHTTPLogger(conf *log.Config, w http.ResponseWriter) (*HTTPLogger, error) {
	syncer := zapcore.AddSync(w)
	logger, props, err := log.InitLoggerWithWriteSyncer(conf, syncer, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return nil, err
	}
	props.Level.SetLevel(zapcore.DebugLevel)
	return &HTTPLogger{
		writer:         w,
		logger:         logger,
		closeCallbacks: make([]func(), 0),
	}, nil
}

// AddCloseCallback adds some callbacks when closing.
func (l *HTTPLogger) AddCloseCallback(fs ...func()) {
	l.closeCallbacks = append(l.closeCallbacks, fs...)
}

// Plug will plug the HTTPLogger into PluggableLogger.
func (l *HTTPLogger) Plug(names ...string) {
	l.AddCloseCallback(func() {
		for _, name := range names {
			pl := GetOrCreatePluggableLogger(name)
			pl.SetLogger(nil)
		}
	})
	for _, name := range names {
		pl := GetOrCreatePluggableLogger(name)
		pl.SetLogger(l.logger)
	}
}

// Close will call close callbacks and close all output.
func (l *HTTPLogger) Close() {
	defer l.writer.WriteHeader(http.StatusOK)
	defer l.logger.Sync()
	for _, f := range l.closeCallbacks {
		f()
	}
}
