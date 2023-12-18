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
	RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(BalanceLeaderType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
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
	RegisterSliceDecoderBuilder(BalanceRegionType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceRegionName
			return nil
		}
	})

	RegisterScheduler(BalanceRegionType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceRegionScheduler(opController, conf), nil
	})

	// balance witness
	RegisterSliceDecoderBuilder(BalanceWitnessType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(BalanceWitnessType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
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
	RegisterSliceDecoderBuilder(EvictLeaderType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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
			return nil
		}
	})

	RegisterScheduler(EvictLeaderType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		return newEvictLeaderScheduler(opController, conf), nil
	})

	// evict slow store
	RegisterSliceDecoderBuilder(EvictSlowStoreType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	RegisterScheduler(EvictSlowStoreType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowStoreSchedulerConfig(storage)
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictSlowStoreScheduler(opController, conf), nil
	})

	// grant hot region
	RegisterSliceDecoderBuilder(GrantHotRegionType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(GrantHotRegionType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &grantHotRegionSchedulerConfig{StoreIDs: make([]uint64, 0), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantHotRegionScheduler(opController, conf), nil
	})

	// hot region
	RegisterSliceDecoderBuilder(HotRegionType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	RegisterScheduler(HotRegionType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := initHotRegionScheduleConfig()
		var data map[string]interface{}
		if err := decoder(&data); err != nil {
			return nil, err
		}
		if len(data) != 0 {
			// After upgrading, use compatible config.
			// For clusters with the initial version >= v5.2, it will be overwritten by the default config.
			conf.applyPrioritiesConfig(compatiblePrioritiesConfig)
			// For clusters with the initial version >= v6.4, it will be overwritten by the default config.
			conf.SetRankFormulaVersion("")
			if err := decoder(conf); err != nil {
				return nil, err
			}
		}
		conf.storage = storage
		return newHotScheduler(opController, conf), nil
	})

	// grant leader
	RegisterSliceDecoderBuilder(GrantLeaderType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(GrantLeaderType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &grantLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})

	// label
	RegisterSliceDecoderBuilder(LabelType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*labelSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = LabelName
			return nil
		}
	})

	RegisterScheduler(LabelType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &labelSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newLabelScheduler(opController, conf), nil
	})

	// random merge
	RegisterSliceDecoderBuilder(RandomMergeType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = RandomMergeName
			return nil
		}
	})

	RegisterScheduler(RandomMergeType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomMergeScheduler(opController, conf), nil
	})

	// scatter range
	// args: [start-key, end-key, range-name].
	RegisterSliceDecoderBuilder(ScatterRangeType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(ScatterRangeType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
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
	RegisterSliceDecoderBuilder(ShuffleHotRegionType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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
			conf.Name = ShuffleHotRegionName
			return nil
		}
	})

	RegisterScheduler(ShuffleHotRegionType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newShuffleHotRegionScheduler(opController, conf), nil
	})

	// shuffle leader
	RegisterSliceDecoderBuilder(ShuffleLeaderType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(ShuffleLeaderType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleLeaderScheduler(opController, conf), nil
	})

	// shuffle region
	RegisterSliceDecoderBuilder(ShuffleRegionType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
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

	RegisterScheduler(ShuffleRegionType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := &shuffleRegionSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleRegionScheduler(opController, conf), nil
	})

	// split bucket
	RegisterSliceDecoderBuilder(SplitBucketType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	RegisterScheduler(SplitBucketType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := initSplitBucketConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newSplitBucketScheduler(opController, conf), nil
	})

	// transfer witness leader
	RegisterSliceDecoderBuilder(TransferWitnessLeaderType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	RegisterScheduler(TransferWitnessLeaderType, func(opController *operator.Controller, _ endpoint.ConfigStorage, _ ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		return newTransferWitnessLeaderScheduler(opController), nil
	})

	// evict slow store by trend
	RegisterSliceDecoderBuilder(EvictSlowTrendType, func(args []string) ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	RegisterScheduler(EvictSlowTrendType, func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowTrendSchedulerConfig(storage)
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowTrendScheduler(opController, conf), nil
	})
}
