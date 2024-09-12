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

package pd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/testutil"
)

func TestDynamicOptionChange(t *testing.T) {
	re := require.New(t)
	o := newOption()
	// Check the default value setting.
	re.Equal(defaultMaxTSOBatchWaitInterval, o.getMaxTSOBatchWaitInterval())
	re.Equal(defaultEnableTSOFollowerProxy, o.getEnableTSOFollowerProxy())
	re.Equal(defaultEnableFollowerHandle, o.getEnableFollowerHandle())

	// Check the invalid value setting.
	re.Error(o.setMaxTSOBatchWaitInterval(time.Second))
	re.Equal(defaultMaxTSOBatchWaitInterval, o.getMaxTSOBatchWaitInterval())
	expectInterval := time.Millisecond
	o.setMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.getMaxTSOBatchWaitInterval())
	expectInterval = time.Duration(float64(time.Millisecond) * 0.5)
	o.setMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.getMaxTSOBatchWaitInterval())
	expectInterval = time.Duration(float64(time.Millisecond) * 1.5)
	o.setMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.getMaxTSOBatchWaitInterval())

	expectBool := true
	o.setEnableTSOFollowerProxy(expectBool)
	// Check the value changing notification.
	testutil.Eventually(re, func() bool {
		<-o.enableTSOFollowerProxyCh
		return true
	})
	re.Equal(expectBool, o.getEnableTSOFollowerProxy())
	// Check whether any data will be sent to the channel.
	// It will panic if the test fails.
	close(o.enableTSOFollowerProxyCh)
	// Setting the same value should not notify the channel.
	o.setEnableTSOFollowerProxy(expectBool)

	expectBool = true
	o.setEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.getEnableFollowerHandle())
	expectBool = false
	o.setEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.getEnableFollowerHandle())
}
