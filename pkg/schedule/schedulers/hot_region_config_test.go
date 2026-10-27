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

package schedulers

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
)

func TestHotRegionConfigRejectsUnsafeUpdates(t *testing.T) {
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	maxZombieRounds := int(math.MaxInt64 / (utils.RegionHeartBeatReportInterval * time.Second))
	cases := []struct {
		body  string
		field string
	}{
		{`{"history-sample-duration":"-1m"}`, "history-sample-duration"},
		{`{"history-sample-interval":"-1s"}`, "history-sample-interval"},
		{`{"history-sample-interval":"0s"}`, "history-sample-interval"},
		{`{"history-sample-duration":"1s","history-sample-interval":"2s"}`, "history-sample-duration"},
		{`{"history-sample-duration":"2562047h47m16.854775807s","history-sample-interval":"1ns"}`, "history sample count"},
		{fmt.Sprintf(`{"history-sample-duration":"%dns","history-sample-interval":"1ns"}`, statistics.MaxHistorySampleCount+1), "history sample count"},
		{`{"max-peer-number":-1}`, "max-peer-number"},
		{`{"max-peer-number":0}`, "max-peer-number"},
		{`{"max-zombie-rounds":-1}`, "max-zombie-rounds"},
		{fmt.Sprintf(`{"max-zombie-rounds":%d}`, maxZombieRounds+1), "max-zombie-rounds"},
		{`{"byte-rate-rank-step-ratio":0}`, "byte-rate-rank-step-ratio"},
		{`{"key-rate-rank-step-ratio":-0.01}`, "key-rate-rank-step-ratio"},
		{`{"query-rate-rank-step-ratio":0}`, "query-rate-rank-step-ratio"},
		{`{"cpu-rate-rank-step-ratio":0}`, "cpu-rate-rank-step-ratio"},
		{`{"count-rank-step-ratio":1.01}`, "count-rank-step-ratio"},
		{`{"great-dec-ratio":0}`, "great-dec-ratio"},
		{`{"minor-dec-ratio":1.01}`, "minor-dec-ratio"},
		{`{"great-dec-ratio":0.99,"minor-dec-ratio":0.95}`, "great-dec-ratio"},
		{`{"min-hot-byte-rate":-1}`, "min-hot-byte-rate"},
		{`{"min-hot-key-rate":-1}`, "min-hot-key-rate"},
		{`{"min-hot-query-rate":-1}`, "min-hot-query-rate"},
		{`{"min-hot-cpu-rate":-1}`, "min-hot-cpu-rate"},
		{`{"src-tolerance-ratio":-1}`, "src-tolerance-ratio"},
		{`{"dst-tolerance-ratio":-1}`, "dst-tolerance-ratio"},
	}
	for _, tc := range cases {
		t.Run(tc.body, func(t *testing.T) {
			re := require.New(t)
			store := storage.NewStorageWithMemoryBackend()
			_, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, store, ConfigJSONDecoder([]byte(tc.body)))
			re.ErrorContains(err, tc.field)

			scheduler, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, store, ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
			re.NoError(err)
			before, err := scheduler.EncodeConfig()
			re.NoError(err)
			re.NoError(store.SaveSchedulerConfig(scheduler.GetName(), before))

			// Include a valid change to verify that rejection preserves the entire config.
			var update map[string]any
			re.NoError(json.Unmarshal([]byte(tc.body), &update))
			update["read-priorities"] = []string{"key", "byte"}
			payload, err := json.Marshal(update)
			re.NoError(err)
			response := httptest.NewRecorder()
			scheduler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/config", strings.NewReader(string(payload))))
			re.Equal(http.StatusBadRequest, response.Code)
			re.Contains(response.Body.String(), tc.field)
			after, err := scheduler.EncodeConfig()
			re.NoError(err)
			re.JSONEq(string(before), string(after))
			persisted, err := store.LoadSchedulerConfig(scheduler.GetName())
			re.NoError(err)
			re.JSONEq(string(before), persisted)

			re.NoError(store.SaveSchedulerConfig(scheduler.GetName(), payload))
			re.ErrorContains(scheduler.ReloadConfig(), tc.field)
			after, err = scheduler.EncodeConfig()
			re.NoError(err)
			re.JSONEq(string(before), string(after))
		})
	}
}

func TestHotRegionConfigNumericBoundaries(t *testing.T) {
	maxZombieRounds := int(math.MaxInt64 / (utils.RegionHeartBeatReportInterval * time.Second))
	for _, body := range []string{
		`{}`,
		`{"history-sample-duration":"0s"}`,
		`{"history-sample-duration":"1ns","history-sample-interval":"1ns"}`,
		fmt.Sprintf(`{"history-sample-duration":"%dns","history-sample-interval":"1ns"}`, statistics.MaxHistorySampleCount),
		`{"max-peer-number":1,"max-zombie-rounds":0}`,
		fmt.Sprintf(`{"max-zombie-rounds":%d}`, maxZombieRounds),
		`{"byte-rate-rank-step-ratio":1,"key-rate-rank-step-ratio":1,"query-rate-rank-step-ratio":1,"cpu-rate-rank-step-ratio":1,"count-rank-step-ratio":1}`,
		`{"great-dec-ratio":1,"minor-dec-ratio":1}`,
		`{"great-dec-ratio":0.01,"minor-dec-ratio":0.01}`,
		`{"min-hot-byte-rate":0,"min-hot-key-rate":0,"min-hot-query-rate":0,"min-hot-cpu-rate":0,"src-tolerance-ratio":0,"dst-tolerance-ratio":0}`,
	} {
		t.Run(body, func(t *testing.T) {
			re := require.New(t)
			conf := initHotRegionScheduleConfig()
			re.NoError(json.Unmarshal([]byte(body), conf))
			re.NoError(conf.validateLocked())
			re.GreaterOrEqual(conf.getStoreStatZombieDuration(), time.Duration(0))
			re.GreaterOrEqual(conf.getRegionsStatZombieDuration(), time.Duration(0))
		})
	}
}

func TestHotRegionConfigRejectsNonFiniteNumbers(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		conf := initHotRegionScheduleConfig()
		for _, field := range []struct {
			name  string
			value *float64
		}{
			{"min-hot-byte-rate", &conf.MinHotByteRate},
			{"min-hot-key-rate", &conf.MinHotKeyRate},
			{"min-hot-query-rate", &conf.MinHotQueryRate},
			{"min-hot-cpu-rate", &conf.MinHotCPURate},
			{"src-tolerance-ratio", &conf.SrcToleranceRatio},
			{"dst-tolerance-ratio", &conf.DstToleranceRatio},
			{"byte-rate-rank-step-ratio", &conf.ByteRateRankStepRatio},
			{"key-rate-rank-step-ratio", &conf.KeyRateRankStepRatio},
			{"query-rate-rank-step-ratio", &conf.QueryRateRankStepRatio},
			{"cpu-rate-rank-step-ratio", &conf.CPURateRankStepRatio},
			{"count-rank-step-ratio", &conf.CountRankStepRatio},
			{"great-dec-ratio", &conf.GreatDecRatio},
			{"minor-dec-ratio", &conf.MinorDecRatio},
			{"pending-weight", &conf.PendingWeight},
			{"split-thresholds", &conf.SplitThresholds},
		} {
			t.Run(fmt.Sprintf("%s/%v", field.name, value), func(t *testing.T) {
				previous := *field.value
				*field.value = value
				require.ErrorContains(t, conf.validateLocked(), field.name)
				*field.value = previous
			})
		}
	}
}

func TestAdjustPrioritiesConfigCPUFallback(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		name         string
		querySupport bool
		cpuSupport   bool
		origins      []string
		expect       []string
	}{
		{
			name:         "cpu-supported-keep-origins",
			querySupport: true,
			cpuSupport:   true,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&defaultPrioritiesConfig),
		},
		{
			name:         "cpu-unsupported-fallback-to-query",
			querySupport: true,
			cpuSupport:   false,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&queryPrioritiesConfig),
		},
		{
			name:         "cpu-unsupported-keep-query-origins",
			querySupport: true,
			cpuSupport:   false,
			origins:      []string{utils.QueryPriority, utils.BytePriority},
			expect:       []string{utils.QueryPriority, utils.BytePriority},
		},
		{
			name:         "query-unsupported-fallback-to-compatible",
			querySupport: false,
			cpuSupport:   true,
			origins:      []string{utils.CPUPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
		{
			name:         "query-unsupported-with-query-origins",
			querySupport: false,
			cpuSupport:   false,
			origins:      []string{utils.QueryPriority, utils.BytePriority},
			expect:       getReadPriorities(&compatiblePrioritiesConfig),
		},
	}

	for _, test := range tests {
		got := adjustPrioritiesConfig(test.querySupport, test.cpuSupport, test.origins, getReadPriorities)
		re.Equal(test.expect, got, test.name)
	}
}
