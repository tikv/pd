// Copyright 2024 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// captureLogs replaces the global logger with an observer and returns it.
func captureLogs(t *testing.T) *observer.ObservedLogs {
	core, logs := observer.New(zap.InfoLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(restore)
	return logs
}

// setRedactLog turns the log redaction on or off and restores it after the test.
func setRedactLog(t *testing.T, redact logutil.RedactInfoLogType) {
	var (
		logger *zap.Logger
		props  *log.ZapProperties
	)
	require.NoError(t, logutil.SetupLogger(&log.Config{Level: "info"}, &logger, &props, redact))
	t.Cleanup(func() {
		require.NoError(t, logutil.SetupLogger(&log.Config{Level: "info"}, &logger, &props, logutil.RedactInfoLogOFF))
	})
}

func postConfig(re *require.Assertions, h http.Handler, body string) {
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader([]byte(body))))
	re.Equal(http.StatusOK, resp.Code)
}

func TestScatterRangeConfigLogRedactsKeys(t *testing.T) {
	re := require.New(t)
	setRedactLog(t, logutil.RedactInfoLogON)
	logs := captureLogs(t)

	conf := &scatterRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		RangeName:       "example",
		StartKey:        "customer-start",
		EndKey:          "customer-end",
	}
	conf.init("scatter-test", storage.NewStorageWithMemoryBackend(), conf)
	// An empty request logs the keys that are already stored.
	postConfig(re, newScatterRangeHandler(conf), `{}`)

	entries := logs.FilterMessage("scatter-range-scheduler config is updated").All()
	re.Len(entries, 1)
	fields := entries[0].ContextMap()
	re.Equal("?", fields["start-key"])
	re.Equal("?", fields["end-key"])
}

func TestScatterRangeConfigLogKeepsBinaryKeys(t *testing.T) {
	re := require.New(t)
	logs := captureLogs(t)

	conf := &scatterRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		RangeName:       "example",
		StartKey:        string([]byte{0x80}),
		EndKey:          string([]byte{0xff}),
	}
	conf.init("scatter-binary-test", storage.NewStorageWithMemoryBackend(), conf)
	postConfig(re, newScatterRangeHandler(conf), `{}`)

	entries := logs.FilterMessage("scatter-range-scheduler config is updated").All()
	re.Len(entries, 1)
	fields := entries[0].ContextMap()
	re.Equal("80", fields["start-key"])
	re.Equal("ff", fields["end-key"])
}

func TestEvictLeaderConfigLogShowsAppliedRanges(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.AddLeaderStore(1, 0)
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	logs := captureLogs(t)

	for _, body := range []map[string]any{
		{"store_id": 1, "ranges": []string{"a", "b"}},
		{"store_id": 1, "ranges": []string{"c", "d"}},
	} {
		data, err := json.Marshal(body)
		re.NoError(err)
		postConfig(re, sl, string(data))
	}
	entries := logs.FilterMessage("evict-leader-scheduler config is updated").All()
	re.Len(entries, 2)
	re.Equal("61-62", entries[0].ContextMap()["ranges"])
	re.Equal("63-64", entries[1].ContextMap()["ranges"])

	// Without explicit ranges nothing is logged for them.
	postConfig(re, sl, `{"store_id":1}`)
	entries = logs.FilterMessage("evict-leader-scheduler config is updated").All()
	re.Len(entries, 3)
	re.NotContains(entries[2].ContextMap(), "ranges")
}

func TestEvictLeaderConfigLogRedactsRanges(t *testing.T) {
	re := require.New(t)
	setRedactLog(t, logutil.RedactInfoLogON)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.AddLeaderStore(1, 0)
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	logs := captureLogs(t)

	postConfig(re, sl, `{"store_id":1,"ranges":["a","b"]}`)
	entries := logs.FilterMessage("evict-leader-scheduler config is updated").All()
	re.Len(entries, 1)
	re.Equal("?", entries[0].ContextMap()["ranges"])
}

func TestGrantHotRegionConfigLogShowsEffectiveStores(t *testing.T) {
	re := require.New(t)
	logs := captureLogs(t)

	conf := &grantHotRegionSchedulerConfig{schedulerConfig: &baseSchedulerConfig{}}
	conf.init("grant-test", storage.NewStorageWithMemoryBackend(), conf)
	postConfig(re, newGrantHotRegionHandler(conf), `{"store-leader-id":"2","store-id":"1"}`)

	entries := logs.FilterMessage("grant-hot-region-scheduler config is updated").All()
	re.Len(entries, 1)
	fields := entries[0].ContextMap()
	re.EqualValues(2, fields["store-leader-id"])
	// The leader is added to the configured stores when it is missing.
	re.ElementsMatch([]any{uint64(1), uint64(2)}, fields["store-ids"])
}
