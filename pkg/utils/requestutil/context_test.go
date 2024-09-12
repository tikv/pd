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
)

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
