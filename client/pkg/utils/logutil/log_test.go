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

package logutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestSampleLoggerFactory(t *testing.T) {
	factory := SampleLoggerFactory(time.Minute, 5)

	firstCore, firstObserved := observer.New(zapcore.InfoLevel)
	restoreFirst := log.ReplaceGlobals(zap.New(firstCore), nil)
	t.Cleanup(restoreFirst)
	logger := factory()
	require.Same(t, logger, factory())

	secondCore, secondObserved := observer.New(zapcore.InfoLevel)
	restoreSecond := log.ReplaceGlobals(zap.New(secondCore), nil)
	t.Cleanup(restoreSecond)

	const sampledMessage = "sampled member update"
	urls := []string{"http://pd-1:2379", "http://pd-2:2379"}
	for i := range 10 {
		logger.Info(sampledMessage, zap.String("url", urls[i%len(urls)]))
	}

	sampledEntries := firstObserved.FilterMessage(sampledMessage)
	require.Equal(t, 5, sampledEntries.Len())
	require.Equal(t, 5, sampledEntries.FilterField(zap.String("sampled", "")).Len())
	require.Equal(t, 3, sampledEntries.FilterField(zap.String("url", urls[0])).Len())
	require.Equal(t, 2, sampledEntries.FilterField(zap.String("url", urls[1])).Len())

	const otherMessage = "another sampled message"
	for range 10 {
		logger.Info(otherMessage)
	}
	require.Equal(t, 5, firstObserved.FilterMessage(otherMessage).Len())
	require.Equal(t, 0, secondObserved.Len())
}
