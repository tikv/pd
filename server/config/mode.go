package config

import (
	"github.com/pingcap/errors"
	"github.com/tikv/pd/server/storage/endpoint"
)

// Scheduling mode
const (
	Normal  = "normal"
	Suspend = "suspend"
	Scaling = "scaling"
)

func isValidMode(mode string) bool {
	switch mode {
	case Normal, Suspend, Scaling:
		return true
	default:
		return false
	}
}

// SwitchMode switches the scheduling mode.
func (o *PersistOptions) SwitchMode(storage endpoint.ConfigStorage, oldCfg *ScheduleConfig, mode string) (ScheduleConfig, error) {
	// save the current mode setting
	if err := storage.SaveScheduleMode(oldCfg.Mode, oldCfg); err != nil {
		return ScheduleConfig{}, err
	}
	if !isValidMode(mode) {
		return ScheduleConfig{}, errors.Errorf("mode %v is invalid", mode)
	}
	newCfg := &ScheduleConfig{}
	existed, err := storage.LoadScheduleMode(mode, newCfg)
	if err != nil {
		return ScheduleConfig{}, err
	}
	if !existed {
		newCfg = oldCfg.Clone()
		getDefaultModeConfig(newCfg, mode)
	}
	return *newCfg, nil
}

func getDefaultModeConfig(newCfg *ScheduleConfig, mode string) {
	switch mode {
	case Suspend:
		newCfg.MergeScheduleLimit = 0
		newCfg.LeaderScheduleLimit = 0
		newCfg.RegionScheduleLimit = 0
		newCfg.HotRegionScheduleLimit = 0
		newCfg.ReplicaScheduleLimit = 0
	case Scaling:
		for storeID := range newCfg.StoreLimit {
			newCfg.StoreLimit[storeID] = StoreLimitConfig{AddPeer: 200, RemovePeer: 200}
		}
	}
	newCfg.Mode = mode
}
