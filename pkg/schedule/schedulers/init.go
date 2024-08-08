// Copyright 2023 TiKV Project Authors.
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

package schedulers

import (
	"strconv"
	"strings"
	"sync"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/operator"
	types "github.com/tikv/pd/pkg/schedule/type"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

var registerOnce sync.Once

// Register registers schedulers.
func Register() {
	registerOnce.Do(func() {
		schedulersRegister()
	})
}

func schedulersRegister() {
	// balance leader
	RegisterSliceDecoderBuilder(types.BalanceLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Batch = BalanceLeaderBatchSize
			return nil
		}
	})

	RegisterScheduler(types.BalanceLeaderScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = BalanceLeaderBatchSize
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})

	// balance region
	RegisterSliceDecoderBuilder(types.BalanceRegionScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			return nil
		}
	})

	RegisterScheduler(types.BalanceRegionScheduler, func(opController *operator.Controller,
		_ endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceRegionScheduler(opController, conf), nil
	})

	// balance witness
	RegisterSliceDecoderBuilder(types.BalanceWitnessScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*balanceWitnessSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Batch = balanceWitnessBatchSize
			return nil
		}
	})

	RegisterScheduler(types.BalanceWitnessScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &balanceWitnessSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = balanceWitnessBatchSize
		}
		return newBalanceWitnessScheduler(opController, conf), nil
	})

	// evict leader
	RegisterSliceDecoderBuilder(types.EvictLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err)
			}

			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			conf.Batch = EvictLeaderBatchSize
			return nil
		}
	})

	RegisterScheduler(types.EvictLeaderScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		return newEvictLeaderScheduler(opController, conf), nil
	})

	// evict slow store
	RegisterSliceDecoderBuilder(types.EvictSlowStoreScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.EvictSlowStoreScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowStoreSchedulerConfig(storage)
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictSlowStoreScheduler(opController, conf), nil
	})

	// grant hot region
	RegisterSliceDecoderBuilder(types.GrantHotRegionScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) != 2 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}

			conf, ok := v.(*grantHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			leaderID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err)
			}

			storeIDs := make([]uint64, 0)
			for _, id := range strings.Split(args[1], ",") {
				storeID, err := strconv.ParseUint(id, 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err)
				}
				storeIDs = append(storeIDs, storeID)
			}
			if !conf.setStore(leaderID, storeIDs) {
				return errs.ErrSchedulerConfig
			}
			return nil
		}
	})

	RegisterScheduler(types.GrantHotRegionScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &grantHotRegionSchedulerConfig{StoreIDs: make([]uint64, 0), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantHotRegionScheduler(opController, conf), nil
	})

	// hot region
	RegisterSliceDecoderBuilder(types.BalanceHotRegionScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.BalanceHotRegionScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initHotRegionScheduleConfig()
		var data map[string]any
		if err := decoder(&data); err != nil {
			return nil, err
		}
		if len(data) != 0 {
			// After upgrading, use compatible config.
			// For clusters with the initial version >= v5.2, it will be overwritten by the default config.
			conf.applyPrioritiesConfig(compatiblePrioritiesConfig)
			// For clusters with the initial version >= v6.4, it will be overwritten by the default config.
			conf.setRankFormulaVersion("")
			if err := decoder(conf); err != nil {
				return nil, err
			}
		}
		conf.storage = storage
		return newHotScheduler(opController, conf), nil
	})

	// grant leader
	RegisterSliceDecoderBuilder(types.GrantLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}

			conf, ok := v.(*grantLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err)
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil
		}
	})

	RegisterScheduler(types.GrantLeaderScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &grantLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})

	// label
	RegisterSliceDecoderBuilder(types.LabelScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*labelSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			return nil
		}
	})

	RegisterScheduler(types.LabelScheduler, func(opController *operator.Controller,
		_ endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &labelSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newLabelScheduler(opController, conf), nil
	})

	// random merge
	RegisterSliceDecoderBuilder(types.RandomMergeScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			return nil
		}
	})

	RegisterScheduler(types.RandomMergeScheduler, func(opController *operator.Controller,
		_ endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomMergeScheduler(opController, conf), nil
	})

	// scatter range
	// args: [start-key, end-key, range-name].
	RegisterSliceDecoderBuilder(types.ScatterRangeScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) != 3 {
				return errs.ErrSchedulerConfig.FastGenByArgs("ranges and name")
			}
			if len(args[2]) == 0 {
				return errs.ErrSchedulerConfig.FastGenByArgs("range name")
			}
			conf, ok := v.(*scatterRangeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.StartKey = args[0]
			conf.EndKey = args[1]
			conf.RangeName = args[2]
			return nil
		}
	})

	RegisterScheduler(types.ScatterRangeScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &scatterRangeSchedulerConfig{
			storage: storage,
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		rangeName := conf.RangeName
		if len(rangeName) == 0 {
			return nil, errs.ErrSchedulerConfig.FastGenByArgs("range name")
		}
		return newScatterRangeScheduler(opController, conf), nil
	})

	// shuffle hot region
	RegisterSliceDecoderBuilder(types.ShuffleHotRegionScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err)
				}
				conf.Limit = limit
			}
			return nil
		}
	})

	RegisterScheduler(types.ShuffleHotRegionScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newShuffleHotRegionScheduler(opController, conf), nil
	})

	// shuffle leader
	RegisterSliceDecoderBuilder(types.ShuffleLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*shuffleLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = ShuffleLeaderName
			return nil
		}
	})

	RegisterScheduler(types.ShuffleLeaderScheduler, func(opController *operator.Controller,
		_ endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleLeaderScheduler(opController, conf), nil
	})

	// shuffle region
	RegisterSliceDecoderBuilder(types.ShuffleRegionScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*shuffleRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Roles = allRoles
			return nil
		}
	})

	RegisterScheduler(types.ShuffleRegionScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &shuffleRegionSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleRegionScheduler(opController, conf), nil
	})

	// split bucket
	RegisterSliceDecoderBuilder(types.SplitBucketScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.SplitBucketScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initSplitBucketConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newSplitBucketScheduler(opController, conf), nil
	})

	// transfer witness leader
	RegisterSliceDecoderBuilder(types.TransferWitnessLeaderScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.TransferWitnessLeaderScheduler, func(opController *operator.Controller,
		_ endpoint.ConfigStorage, _ ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		return newTransferWitnessLeaderScheduler(opController), nil
	})

	// evict slow store by trend
	RegisterSliceDecoderBuilder(types.EvictSlowTrendScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.EvictSlowTrendScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowTrendSchedulerConfig(storage)
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowTrendScheduler(opController, conf), nil
	})
}
