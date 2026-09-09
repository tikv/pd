// Copyright 2026 TiKV Project Authors.
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

package filter_test

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	mcsconfig "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
)

// BenchmarkStoreLimitFilter measures candidate checks with shared stores and
// per-worker filters, as independent schedulers do. Setup is outside the timer.
func BenchmarkStoreLimitFilter(b *testing.B) {
	for _, deployment := range []string{"PD", "MCS"} {
		for _, version := range []string{storelimit.VersionV1, storelimit.VersionV2} {
			for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer} {
				b.Run(deployment+"/"+version+"/"+typ.String(), func(b *testing.B) {
					opt := mockconfig.NewTestOptions()
					cfg := opt.GetScheduleConfig().Clone()
					stores := make([]*core.StoreInfo, 64)
					for i := range stores {
						id := uint64(i + 1)
						cfg.StoreLimit[id] = config.StoreLimitConfig{AddPeer: 60, RemovePeer: 60}
						limit := storelimit.NewStoreRateLimit(1)
						if version == storelimit.VersionV2 {
							limit = storelimit.NewSlidingWindows()
						}
						stores[i] = core.NewStoreInfo(&metapb.Store{Id: id}, core.SetLastHeartbeatTS(time.Now()), core.SetStoreLimit(limit))
					}
					opt.SetScheduleConfig(cfg)
					var conf config.SharedConfigProvider = opt
					if deployment == "MCS" {
						mcs := mcsconfig.NewPersistConfig(mcsconfig.NewConfig(), nil)
						mcs.SetScheduleConfig(cfg)
						conf = mcs
					}
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						f := &filter.StoreStateFilter{MoveRegion: true, OperatorLevel: constant.Medium}
						i := 0
						for pb.Next() {
							store := stores[i%len(stores)]
							if typ == storelimit.RemovePeer {
								f.Source(conf, store)
							} else {
								f.Target(conf, store)
							}
							i++
						}
					})
				})
			}
		}
	}
}
