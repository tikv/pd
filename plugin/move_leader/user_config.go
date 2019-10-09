package main

import (
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/pd/server/schedule"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type userConfig struct {
	cfgLock sync.RWMutex
	version uint64
	cfg     *dispatchConfig
}

type dispatchConfig struct {
	Leaders leaders
}

type leaders struct {
	Leader []moveLeader
}

type moveLeader struct {
	KeyStart  string
	KeyEnd    string
	Stores    []schedule.StoreLabels
	StartTime time.Time
	EndTime   time.Time
}

func NewUserConfig() schedule.Config {
	ret := &userConfig{
		cfgLock: sync.RWMutex{},
		version: 1,
		cfg:     nil,
	}
	return ret
}

// Load and decode config file
// if conflict, return false
// if not conflict, reset pluginInfoMap
func (uc *userConfig) LoadConfig(path string, maxReplicas int) bool {
	filePath, err := filepath.Abs(path)
	if err != nil {
		log.Error("open file failed", zap.Error(err))
		return false
	}
	log.Info("parse toml file once. ", zap.String("filePath", filePath))
	cfg := new(dispatchConfig)
	if _, err := toml.DecodeFile(filePath, cfg); err != nil {
		log.Error("parse user config failed", zap.Error(err))
		return false
	}
	uc.cfgLock.Lock()
	defer uc.cfgLock.Unlock()
	schedule.PluginsInfoMapLock.Lock()
	defer schedule.PluginsInfoMapLock.Unlock()
	uc.cfg = cfg
	if uc.cfg != nil && uc.IfConflict(maxReplicas) {
		return false
	}
	schedule.PluginsInfoMap = make(map[string]*schedule.PluginInfo)
	for i, info := range uc.cfg.Leaders.Leader {
		pi := &schedule.PluginInfo{
			KeyStart: info.KeyStart,
			KeyEnd:   info.KeyEnd,
			Interval: &schedule.TimeInterval{Begin: info.StartTime, End: info.EndTime},
			Stores:   info.Stores,
			StoreIDs: []uint64{},
		}
		str := "Leader-" + strconv.Itoa(i)
		schedule.PluginsInfoMap[str] = pi
	}
	uc.version++
	return true
}

func (uc *userConfig) GetStoreId(cluster schedule.Cluster) map[string][]uint64 {
	ret := make(map[string][]uint64)
	for i, Leader := range uc.cfg.Leaders.Leader {
		for _, s := range Leader.Stores {
			if store := schedule.GetStoreByLabel(cluster, s.StoreLabel); store != nil {
				str := "Leader-" + strconv.Itoa(i)
				log.Info(str, zap.Uint64("store-id", store.GetID()))
				ret[str] = append(ret[str], store.GetID())
			}
		}
	}
	return ret
}

func (uc *userConfig) GetInterval() map[string]*schedule.TimeInterval {
	ret := make(map[string]*schedule.TimeInterval)
	for i, Leader := range uc.cfg.Leaders.Leader {
		str := "Leader-" + strconv.Itoa(i)
		interval := &schedule.TimeInterval{
			Begin: Leader.StartTime,
			End:   Leader.EndTime,
		}
		ret[str] = interval
	}
	return ret
}

// Check if there are conflicts in similar type of rules
// eg. move-leader&move-leader or move-region&move-region
func (uc *userConfig) IfConflict(maxReplicas int) bool {
	ret := false
	// move_leaders
	for i, l1 := range uc.cfg.Leaders.Leader {
		for j, l2 := range uc.cfg.Leaders.Leader {
			if i < j {
				if (l1.KeyStart <= l2.KeyStart && l1.KeyEnd > l2.KeyStart) ||
					(l2.KeyStart <= l1.KeyStart && l2.KeyEnd > l1.KeyStart) {
					if ((l1.StartTime.Before(l2.StartTime) || l1.StartTime.Equal(l2.StartTime)) &&
						l1.EndTime.After(l2.StartTime)) ||
						((l2.StartTime.Before(l1.StartTime) || l2.StartTime.Equal(l1.StartTime)) &&
							l2.EndTime.After(l1.StartTime)) {
						log.Error("Key Range Conflict", zap.Ints("Config Move-Leader Nums", []int{i, j}))
						ret = true
					}

				}
			}
		}
	}
	return ret
}
