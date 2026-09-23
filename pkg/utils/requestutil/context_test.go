// Copyright 2022 TiKV Project Authors.
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

package requestutil

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRequestInfo(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, ok := RequestInfoFrom(ctx)
	re.False(ok)
	timeNow := time.Now().Unix()
	ctx = WithRequestInfo(ctx,
		RequestInfo{
			ServiceLabel:   "test label",
			Method:         http.MethodPost,
			CallerID:       "pdctl",
			IP:             "localhost",
			URLParam:       "{\"id\"=1}",
			BodyParam:      "{\"state\"=\"Up\"}",
			StartTimeStamp: timeNow,
		})
	result, ok := RequestInfoFrom(ctx)
	re.NotNil(result)
	re.True(ok)
	re.Equal("test label", result.ServiceLabel)
	re.Equal(http.MethodPost, result.Method)
	re.Equal("pdctl", result.CallerID)
	re.Equal("localhost", result.IP)
	re.Equal("{\"id\"=1}", result.URLParam)
	re.Equal("{\"state\"=\"Up\"}", result.BodyParam)
	re.Equal(timeNow, result.StartTimeStamp)
}

func TestEndTime(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, ok := EndTimeFrom(ctx)
	re.False(ok)
	timeNow := time.Now().Unix()
	ctx = WithEndTime(ctx, timeNow)
	result, ok := EndTimeFrom(ctx)
	re.NotNil(result)
	re.True(ok)
	re.Equal(timeNow, result)
}

func setRedactInfoLogForTest(t *testing.T, redactInfoLog logutil.RedactInfoLogType) {
	re := require.New(t)
	var (
		lg    *zap.Logger
		props *log.ZapProperties
	)
	re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, redactInfoLog))
	t.Cleanup(func() {
		re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogOFF))
	})
}

func TestRequestInfoStringRedact(t *testing.T) {
	re := require.New(t)
	info := RequestInfo{
		ServiceLabel:   "SetRegionLabelRule",
		Method:         "HTTP/1.1/POST:/pd/api/v1/config/region-label/rule",
		CallerID:       "pdctl",
		IP:             "localhost",
		Port:           "2379",
		URLParam:       `{"key":["7480000000000000ff"]}`,
		BodyParam:      `{"id":"test","data":[{"start_key":"7480000000000000ff","end_key":"7480000000000001ff"}]}`,
		StartTimeStamp: time.Now().Unix(),
	}
	originalURLParam, originalBodyParam := info.URLParam, info.BodyParam
	prefix := "{ServiceLabel:SetRegionLabelRule, Method:HTTP/1.1/POST:/pd/api/v1/config/region-label/rule, " +
		"CallerID:pdctl, IP:localhost, Port:2379, StartTime:" + time.Unix(info.StartTimeStamp, 0).String()

	// Redaction off: identical to the raw values.
	re.Equal(prefix+", URLParam:"+info.URLParam+", BodyParam:"+info.BodyParam+"}", info.String())

	setRedactInfoLogForTest(t, logutil.RedactInfoLogON)
	s := info.String()
	re.Equal(prefix+", URLParam:?, BodyParam:?}", s)
	re.NotContains(s, "7480000000000000ff")

	setRedactInfoLogForTest(t, logutil.RedactInfoLogMarker)
	re.Equal(prefix+", URLParam:‹"+info.URLParam+"›, BodyParam:‹"+info.BodyParam+"›}", info.String())

	// The raw fields themselves are left untouched.
	re.Equal(originalURLParam, info.URLParam)
	re.Equal(originalBodyParam, info.BodyParam)
}
