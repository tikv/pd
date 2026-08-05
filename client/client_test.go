// Copyright 2016 TiKV Project Authors.
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

package pd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	"github.com/tikv/pd/client/pkg/utils/tsoutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestTSLessEqual(t *testing.T) {
	re := require.New(t)
	re.True(tsoutil.TSLessEqual(9, 9, 9, 9))
	re.True(tsoutil.TSLessEqual(8, 9, 9, 8))
	re.False(tsoutil.TSLessEqual(9, 8, 8, 9))
	re.False(tsoutil.TSLessEqual(9, 8, 9, 6))
	re.True(tsoutil.TSLessEqual(9, 6, 9, 8))
}

const testClientURL = "tmp://test.url:5255"

func TestClientCtx(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	cli, err := NewClientWithContext(ctx, caller.TestComponent,
		[]string{testClientURL}, SecurityOption{})
	re.Error(err)
	defer cli.Close()
	re.Less(time.Since(start), time.Second*5)
}

func TestClientWithRetry(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	cli, err := NewClientWithContext(context.TODO(), caller.TestComponent,
		[]string{testClientURL}, SecurityOption{}, opt.WithMaxErrorRetry(5))
	re.Error(err)
	defer cli.Close()
	re.Less(time.Since(start), time.Second*10)
}

func TestIsRetryableGetTSError(t *testing.T) {
	re := require.New(t)

	re.True(isRetryableGetTSError(fmt.Errorf("%s", errs.NotLeaderErr)))
	re.True(isRetryableGetTSError(fmt.Errorf("%s", errs.MismatchCalleeIDErr)))
	re.False(isRetryableGetTSError(errors.New("other error")))
}

func TestRoundUpDurationToSeconds(t *testing.T) {
	re := require.New(t)
	re.Equal(int64(0), roundUpDurationToSeconds(0))
	re.Equal(int64(1), roundUpDurationToSeconds(time.Millisecond))
	re.Equal(int64(1), roundUpDurationToSeconds(time.Second))
	re.Equal(int64(3600), roundUpDurationToSeconds(time.Hour))
	re.Equal(int64(3601), roundUpDurationToSeconds(time.Hour+1))
	// time.Duration(9223372036854775807) -> 9223372036.854... secs -(round up)-> 9223372037
	re.Equal(int64(9223372037), roundUpDurationToSeconds(math.MaxInt64-1))
	re.Equal(int64(math.MaxInt64), roundUpDurationToSeconds(math.MaxInt64))
}

func TestSaturatingStdDurationFromSeconds(t *testing.T) {
	re := require.New(t)

	re.Equal(time.Second*2, saturatingStdDurationFromSeconds(2))
	re.Equal(time.Duration(0), saturatingStdDurationFromSeconds(-2))
	re.Equal(time.Hour, saturatingStdDurationFromSeconds(3600))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(1<<34))
	re.Equal((1<<33)*time.Second, saturatingStdDurationFromSeconds(1<<33))
	re.Equal(9223372036*time.Second, saturatingStdDurationFromSeconds(9223372036))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(9223372037))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(math.MaxInt64))
}

func TestIsKeyspaceUsingKeyspaceLevelGC(t *testing.T) {
	tests := []struct {
		name string
		meta *keyspacepb.KeyspaceMeta
		want bool
	}{
		{name: "nil metadata", meta: nil, want: false},
		{name: "nil config", meta: &keyspacepb.KeyspaceMeta{}, want: false},
		{name: "empty config", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{}}, want: false},
		{name: "native keyspace level GC", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "keyspace_level"}}, want: true},
		{name: "unified GC", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "unified"}}, want: false},
		{name: "unified GC overrides safe point version v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "unified", "safe_point_version": "v2"}}, want: false},
		{name: "invalid GC management type", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "111111"}}, want: false},
		{name: "safe point version v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "v2"}}, want: true},
		{name: "uppercase safe point version", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "V2"}}, want: false},
		{name: "padded safe point version", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": " v2 "}}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			meta := test.meta

			// When
			got := IsKeyspaceUsingKeyspaceLevelGC(meta)

			// Then
			require.Equal(t, test.want, got)
		})
	}
}
