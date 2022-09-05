package config

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.uber.org/zap"
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
func (o *PersistOptions) SwitchMode(storage endpoint.ConfigStorage, oldMode, newMode *ScheduleModeConfig) error {
	if !isValidMode(newMode.Mode) {
		return errors.Errorf("mode %v is invalid", newMode)
	}
	// save the current mode setting
	oldCfg := o.GetScheduleConfig()
	if err := storage.SaveScheduleMode(oldMode.Mode, oldCfg); err != nil {
		return err
	}
	// load the new mode setting
	newCfg := &ScheduleConfig{}
	existed, err := storage.LoadScheduleMode(newMode.Mode, newCfg)
	if err != nil {
		return err
	}
	// if the new mode setting doesn't exist, init a new one.
	if !existed {
		// We must have the Normal config.
		_, err := storage.LoadScheduleMode(Normal, newCfg)
		if err != nil {
			return err
		}
		updateScheduleConfig(newCfg, newMode.Mode)
	}
	if newMode.Mode == Scaling { // reset the store limit for all store to avoid new added store using default limit in scaling mode.
		for storeID := range oldCfg.StoreLimit {
			newCfg.StoreLimit[storeID] = StoreLimitConfig{AddPeer: 200, RemovePeer: 200}
		}
	}

	newCfg.SchedulersPayload = nil
	o.SetScheduleConfig(newCfg)
	o.SetScheduleModeConfig(newMode)
	if err := o.Persist(storage); err != nil {
		o.SetScheduleConfig(oldCfg)
		o.SetScheduleModeConfig(oldMode)
		log.Error("failed to switch schedule mode",
			zap.Reflect("new-mode", newCfg),
			zap.Reflect("old-mode", oldMode),
			zap.Reflect("new", newCfg),
			zap.Reflect("old", oldCfg),
			errs.ZapError(err))
		return err
	}
	log.Info("schedule mode is switched", zap.Reflect("old-mode", oldMode), zap.Reflect("new-mode", newMode))
	return nil
}

func updateScheduleConfig(newCfg *ScheduleConfig, mode string) {
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
}
