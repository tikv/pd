// Copyright 2019 PingCAP, Inc.
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

package schedule

import (
	"path/filepath"
	"plugin"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var PluginMapLock = sync.RWMutex{}
var PluginMap = make(map[string]*plugin.Plugin)

// GetFunction gets func by funcName from plugin(.so)
func GetFunction(path string, funcName string) (plugin.Symbol, error) {
	PluginMapLock.Lock()
	if _, ok := PluginMap[path]; !ok {
		//open plugin
		filePath, err := filepath.Abs(path)
		if err != nil {
			PluginMapLock.Unlock()
			return nil, err
		}
		log.Info("open plugin file", zap.String("file-path", filePath))
		p, err := plugin.Open(filePath)
		if err != nil {
			PluginMapLock.Unlock()
			return nil, err
		}
		PluginMap[path] = p
	}
	PluginMapLock.Unlock()
	//get func from plugin
	f, err := PluginMap[path].Lookup(funcName)
	if err != nil {
		log.Error("Lookup func error!")
		return nil, err
	}
	return f, nil
}
