// Copyright 2019 TiKV Project Authors.
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

package schedule

import (
	"path/filepath"
	"plugin"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// PluginInterface is used to manage all plugin.
type PluginInterface struct {
	pluginMap     map[string]*plugin.Plugin
	pluginMapLock syncutil.RWMutex
}

// NewPluginInterface create a plugin interface
func NewPluginInterface() *PluginInterface {
	return &PluginInterface{
		pluginMap:     make(map[string]*plugin.Plugin),
		pluginMapLock: syncutil.RWMutex{},
	}
}

// GetFunction gets func by funcName from plugin(.so)
func (p *PluginInterface) GetFunction(path string, funcName string) (plugin.Symbol, error) {
	p.pluginMapLock.Lock()
	defer p.pluginMapLock.Unlock()
	if _, ok := p.pluginMap[path]; !ok {
		// open plugin
		filePath, err := filepath.Abs(path)
		if err != nil {
			return nil, errs.ErrFilePathAbs.Wrap(err)
		}
		log.Info("open plugin file", zap.String("file-path", filePath))
		plugin, err := plugin.Open(filePath)
		if err != nil {
			return nil, errs.ErrLoadPlugin.Wrap(err)
		}
		p.pluginMap[path] = plugin
	}
	// get func from plugin
	f, err := p.pluginMap[path].Lookup(funcName)
	if err != nil {
		return nil, errs.ErrLookupPluginFunc.Wrap(err)
	}
	return f, nil
}
