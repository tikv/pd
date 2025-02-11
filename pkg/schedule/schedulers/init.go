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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
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
		conf := &balanceLeaderSchedulerConfig{
			baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = BalanceLeaderBatchSize
		}
		sche := newBalanceLeaderScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{
			baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newBalanceRegionScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		conf := &balanceWitnessSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = balanceWitnessBatchSize
		}
		sche := newBalanceWitnessScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// evict leader
	RegisterSliceDecoderBuilder(types.EvictLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) < 1 {
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
		conf := &evictLeaderSchedulerConfig{
			schedulerConfig:   &baseSchedulerConfig{},
			StoreIDWithRanges: make(map[uint64][]core.KeyRange),
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		sche := newEvictLeaderScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// evict slow store
	RegisterSliceDecoderBuilder(types.EvictSlowStoreScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.EvictSlowStoreScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowStoreSchedulerConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		sche := newEvictSlowStoreScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
			conf.setStore(leaderID, storeIDs)
			return nil
		}
	})

	RegisterScheduler(types.GrantHotRegionScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &grantHotRegionSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
			StoreIDs:        make([]uint64, 0),
		}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newGrantHotRegionScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		sche := newHotScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// grant leader
	RegisterSliceDecoderBuilder(types.GrantLeaderScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			if len(args) < 1 {
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
		conf := &grantLeaderSchedulerConfig{
			schedulerConfig:   &baseSchedulerConfig{},
			StoreIDWithRanges: make(map[uint64][]core.KeyRange),
		}
		conf.cluster = opController.GetCluster()
		conf.removeSchedulerCb = removeSchedulerCb[0]
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newGrantLeaderScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &labelSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newLabelScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &randomMergeSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newRandomMergeScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		rangeName := conf.RangeName
		if len(rangeName) == 0 {
			return nil, errs.ErrSchedulerConfig.FastGenByArgs("range name")
		}
		sche := newScatterRangeScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		conf := &shuffleHotRegionSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
			Limit:           uint64(1),
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newShuffleHotRegionScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
			return nil
		}
	})

	RegisterScheduler(types.ShuffleLeaderScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newShuffleLeaderScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		conf := &shuffleRegionSchedulerConfig{
			schedulerConfig: &baseSchedulerConfig{},
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newShuffleRegionScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
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
		sche := newSplitBucketScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// transfer witness leader
	RegisterSliceDecoderBuilder(types.TransferWitnessLeaderScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.TransferWitnessLeaderScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, _ ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &baseSchedulerConfig{}
		sche := newTransferWitnessLeaderScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// evict slow store by trend
	RegisterSliceDecoderBuilder(types.EvictSlowTrendScheduler, func([]string) ConfigDecoder {
		return func(any) error {
			return nil
		}
	})

	RegisterScheduler(types.EvictSlowTrendScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := initEvictSlowTrendSchedulerConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}

		sche := newEvictSlowTrendScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})

	// balance key range scheduler
	// args: [role, engine, timeout, alias, range1, range2, ...]
	RegisterSliceDecoderBuilder(types.BalanceRangeScheduler, func(args []string) ConfigDecoder {
		return func(v any) error {
			conf, ok := v.(*balanceRangeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			if len(args) < 5 {
				return errs.ErrSchedulerConfig.FastGenByArgs("args length must be greater than 4")
			}
			role, err := url.QueryUnescape(args[0])
			if err != nil {
				return errs.ErrQueryUnescape.Wrap(err)
			}
			jobRole := NewRole(role)
			if jobRole == unknown {
				return errs.ErrQueryUnescape.FastGenByArgs("role")
			}
			engine, err := url.QueryUnescape(args[1])
			if err != nil {
				return errs.ErrQueryUnescape.Wrap(err)
			}
			timeout, err := url.QueryUnescape(args[2])
			if err != nil {
				return errs.ErrQueryUnescape.Wrap(err)
			}
			duration, err := time.ParseDuration(timeout)
			if err != nil {
				return errs.ErrURLParse.Wrap(err)
			}
			alias, err := url.QueryUnescape(args[3])
			if err != nil {
				return errs.ErrURLParse.Wrap(err)
			}
			ranges, err := getKeyRanges(args[4:])
			if err != nil {
				return err
			}

			id := uint64(0)
			if len(conf.jobs) > 0 {
				id = conf.jobs[len(conf.jobs)-1].JobID + 1
			}

			job := &balanceRangeSchedulerJob{
				Role:    jobRole,
				Engine:  engine,
				Timeout: duration,
				Alias:   alias,
				Ranges:  ranges,
				Status:  pending,
				JobID:   id,
			}
			conf.jobs = append(conf.jobs, job)
			return nil
		}
	})

	RegisterScheduler(types.BalanceRangeScheduler, func(opController *operator.Controller,
		storage endpoint.ConfigStorage, decoder ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		conf := &balanceRangeSchedulerConfig{
			schedulerConfig: newBaseDefaultSchedulerConfig(),
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		sche := newBalanceRangeScheduler(opController, conf)
		conf.init(sche.GetName(), storage, conf)
		return sche, nil
	})
}
