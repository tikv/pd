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
	Regions regions
}

type regions struct {
	Region []moveRegion
}

type moveRegion struct {
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
// if not conflict, reset pluginMap
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

	for i, info := range uc.cfg.Regions.Region {
		pi := &schedule.PluginInfo{
			KeyStart: info.KeyStart,
			KeyEnd:   info.KeyEnd,
			Interval: &schedule.TimeInterval{Begin: info.StartTime, End: info.EndTime},
			Stores:   info.Stores,
			StoreIDs: []uint64{},
		}
		str := "Region-" + strconv.Itoa(i)
		schedule.PluginsInfoMap[str] = pi
	}

	uc.version++
	return true
}

func (uc *userConfig) GetStoreId(cluster schedule.Cluster) map[string][]uint64 {
	ret := make(map[string][]uint64)
	for i, Region := range uc.cfg.Regions.Region {
		for _, s := range Region.Stores {
			if store := schedule.GetStoreByLabel(cluster, s.StoreLabel); store != nil {
				str := "Region-" + strconv.Itoa(i)
				log.Info(str, zap.Uint64("store-id", store.GetID()))
				ret[str] = append(ret[str], store.GetID())
			}
		}
	}
	return ret
}

func (uc *userConfig) GetInterval() map[string]*schedule.TimeInterval {
	ret := make(map[string]*schedule.TimeInterval)
	for i, Region := range uc.cfg.Regions.Region {
		str := "Region-" + strconv.Itoa(i)
		interval := &schedule.TimeInterval{
			Begin: Region.StartTime,
			End:   Region.EndTime,
		}
		ret[str] = interval
	}
	return ret
}

// Check if there are conflicts in similar type of rules
func (uc *userConfig) IfConflict(maxReplicas int) bool {
	ret := false
	// move_regions
	for i, r1 := range uc.cfg.Regions.Region {
		for j, r2 := range uc.cfg.Regions.Region {
			if i < j {
				if (r1.KeyStart <= r2.KeyStart && r1.KeyEnd > r2.KeyStart) ||
					(r2.KeyStart <= r1.KeyStart && r2.KeyEnd > r1.KeyStart) {
					if ((r1.StartTime.Before(r2.StartTime) || r1.StartTime.Equal(r2.StartTime)) &&
						r1.EndTime.After(r2.StartTime)) ||
						((r2.StartTime.Before(r1.StartTime) || r2.StartTime.Equal(r1.StartTime)) &&
							r2.EndTime.After(r1.StartTime)) {
						log.Error("Key Range Conflict", zap.Ints("Config Move-Region Nums", []int{i, j}))
						ret = true
					}
				}
			}
		}
	}
	// store nums > max replicas
	for i, r := range uc.cfg.Regions.Region {
		if len(r.Stores) > maxReplicas {
			log.Error("the number of stores is beyond the max replicas", zap.Int("Config Move-Region Nums", i))
			ret = true
		}
	}
	return ret
}
