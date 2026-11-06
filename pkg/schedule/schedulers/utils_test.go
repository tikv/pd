// Copyright 2021 TiKV Project Authors.
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
	"os"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.uber.org/zap"
)

func TestCalculateRangeCountLogRedact(t *testing.T) {
	for _, mode := range []struct {
		name string
		typ  logutil.RedactInfoLogType
	}{
		{"off", logutil.RedactInfoLogOFF},
		{"on", logutil.RedactInfoLogON},
		{"marker", logutil.RedactInfoLogMarker},
	} {
		t.Run(mode.name, func(t *testing.T) {
			re := require.New(t)
			cancel, _, cluster, _ := prepareSchedulersTest()
			defer cancel()
			cluster.AddRegionStore(1, 1)
			rule := &placement.Rule{
				GroupID: placement.DefaultGroupID, ID: "range", Index: 1, Override: true,
				StartKeyHex: "6161", EndKeyHex: "7a7a", Role: placement.Voter, Count: 1,
			}
			re.NoError(cluster.SetRule(rule))
			// Use a subrange so unredacted rule boundaries cannot hide behind range fields.
			cluster.AddLeaderRegionWithRange(1, "ab", "zy", 1)

			fname := testutil.InitTempFileLogger("debug")
			var (
				lg    *zap.Logger
				props *log.ZapProperties
			)
			re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, mode.typ))
			t.Cleanup(func() {
				re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogOFF))
			})
			store := cluster.GetStore(1)
			re.Equal(1.0, calculateRangeCount(cluster, []*core.StoreInfo{store}, store, []byte("ab"), []byte("zy"), core.PeerScatter))

			output, err := os.ReadFile(fname)
			re.NoError(err)
			re.Contains(string(output), "calculate range result")
			if mode.typ == logutil.RedactInfoLogON {
				re.Contains(string(output), "[rule=?]")
				re.NotContains(string(output), rule.StartKeyHex)
				re.NotContains(string(output), rule.EndKeyHex)
			} else {
				re.Contains(string(output), rule.StartKeyHex)
				re.Contains(string(output), rule.EndKeyHex)
				if mode.typ == logutil.RedactInfoLogMarker {
					re.Contains(string(output), `[rule="‹{`)
				}
			}
			re.Equal("6161", rule.StartKeyHex)
			re.Equal("7a7a", rule.EndKeyHex)
		})
	}
}

func TestRetryQuota(t *testing.T) {
	re := require.New(t)

	q := newRetryQuota()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2})
	keepStores := []*core.StoreInfo{store1}

	// test getLimit
	re.Equal(10, q.getLimit(store1))

	// test attenuate
	for _, expected := range []int{5, 2, 1, 1, 1} {
		q.attenuate(store1)
		re.Equal(expected, q.getLimit(store1))
	}

	// test GC
	re.Equal(10, q.getLimit(store2))
	q.attenuate(store2)
	re.Equal(5, q.getLimit(store2))
	q.gc(keepStores)
	re.Equal(1, q.getLimit(store1))
	re.Equal(10, q.getLimit(store2))

	// test resetLimit
	q.resetLimit(store1)
	re.Equal(10, q.getLimit(store1))
}
