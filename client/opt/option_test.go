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

package opt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestDynamicOptionChange(t *testing.T) {
	re := require.New(t)
	o := NewOption()
	// Check the default value setting.
	re.Equal(defaultMaxTSOBatchWaitInterval, o.GetMaxTSOBatchWaitInterval())
	re.Equal(defaultEnableTSOFollowerProxy, o.GetEnableTSOFollowerProxy())
	re.Equal(defaultEnableFollowerHandle, o.GetEnableFollowerHandle())

	// Check the invalid value setting.
	re.Error(o.SetMaxTSOBatchWaitInterval(time.Second))
	re.Equal(defaultMaxTSOBatchWaitInterval, o.GetMaxTSOBatchWaitInterval())
	expectInterval := time.Millisecond
	o.SetMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.GetMaxTSOBatchWaitInterval())
	expectInterval = time.Duration(float64(time.Millisecond) * 0.5)
	o.SetMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.GetMaxTSOBatchWaitInterval())
	expectInterval = time.Duration(float64(time.Millisecond) * 1.5)
	o.SetMaxTSOBatchWaitInterval(expectInterval)
	re.Equal(expectInterval, o.GetMaxTSOBatchWaitInterval())

	expectBool := true
	o.SetEnableTSOFollowerProxy(expectBool)
	// Check the value changing notification.
	testutil.Eventually(re, func() bool {
		<-o.EnableTSOFollowerProxyCh
		return true
	})
	re.Equal(expectBool, o.GetEnableTSOFollowerProxy())
	// Check whether any data will be sent to the channel.
	// It will panic if the test fails.
	close(o.EnableTSOFollowerProxyCh)
	// Setting the same value should not notify the channel.
	o.SetEnableTSOFollowerProxy(expectBool)

	expectBool = true
	o.SetEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.GetEnableFollowerHandle())
	expectBool = false
	o.SetEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.GetEnableFollowerHandle())
}
