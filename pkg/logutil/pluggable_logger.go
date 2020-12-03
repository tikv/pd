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
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// PluggableLogger is a pluggable zap.Logger.
type PluggableLogger struct {
	name   string
	logger atomic.Value // zap.Logger
}

// GetName gets the pluggable logger's name.
func (l PluggableLogger) GetName() string {
	return l.name
}

// GetLogger gets the logger. If it is nil,
// it means that the current related log does not need to be output.
func (l PluggableLogger) GetLogger() *zap.Logger {
	logger := l.logger.Load().(*zap.Logger)
	if logger == nil {
		return nil
	}
	return logger.WithOptions(zap.AddCallerSkip(1))
}

// SetLogger sets the logger. If it is nil,
// it means that the current related log does not need to be output.
func (l *PluggableLogger) SetLogger(logger *zap.Logger) {
	l.logger.Store(logger)
}

func (l PluggableLogger) Debug(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Debug(msg, fields...)
	}
}

func (l PluggableLogger) Info(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Info(msg, fields...)
	}
}

func (l PluggableLogger) Warn(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Warn(msg, fields...)
	}
}

func (l PluggableLogger) Error(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Error(msg, fields...)
	}
}

func (l PluggableLogger) Panic(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Panic(msg, fields...)
	}
}

func (l PluggableLogger) Fatal(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Fatal(msg, fields...)
	}
}

type loggerManager struct {
	sync.Mutex
	Loggers map[string]*PluggableLogger
}

func (m *loggerManager) Init() {
	m.Lock()
	defer m.Unlock()
	m.Loggers = make(map[string]*PluggableLogger)
}

// GetPluggableLogger gets a pluggable zap.Logger.
func (m *loggerManager) GetPluggableLogger(name string, createIfNotExist bool) *PluggableLogger {
	m.Lock()
	defer m.Unlock()
	l, ok := m.Loggers[name]
	if !ok {
		if !createIfNotExist {
			return nil
		}
		l = new(PluggableLogger)
		l.name = name
		l.logger.Store((*zap.Logger)(nil))
		m.Loggers[name] = l
	}
	return l
}

var globalLoggerManager loggerManager

func init() {
	globalLoggerManager.Init()
}

// GetPluggableLogger gets a pluggable zap.Logger.
func GetPluggableLogger(name string, createIfNotExist bool) *PluggableLogger {
	return globalLoggerManager.GetPluggableLogger(name, createIfNotExist)
}
