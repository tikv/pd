// Copyright 2017 TiKV Project Authors.
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

package simutil

import (
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

// Logger is the global logger used for simulator.
var Logger *zap.Logger

// InitLogger initializes the Logger with -log level.
func InitLogger(l, file string) {
	conf := &log.Config{Level: l, File: log.FileLogConfig{Filename: file}}
	lg, _, _ := log.InitLogger(conf)
	Logger = lg
}
