package schedule

import (
	"fmt"
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
	if PluginMap[path] == nil {
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
	PluginMapLock.RLock()
	defer PluginMapLock.RUnlock()
	//get func from plugin
	f, err := PluginMap[path].Lookup(funcName)
	if err != nil {
		fmt.Println("Lookup func error!")
		return nil, err
	}
	return f, nil
}
