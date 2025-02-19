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
<<<<<<< HEAD:client/option_test.go
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
=======
	o := NewOption()

	// Test default values.
	re.Equal(defaultMaxTSOBatchWaitInterval, o.GetMaxTSOBatchWaitInterval(), "default max TSO batch wait interval")
	re.Equal(defaultEnableTSOFollowerProxy, o.GetEnableTSOFollowerProxy(), "default enable TSO follower proxy")
	re.Equal(defaultEnableFollowerHandle, o.GetEnableFollowerHandle(), "default enable follower handle")
	re.Equal(defaultTSOClientRPCConcurrency, o.GetTSOClientRPCConcurrency(), "default TSO client RPC concurrency")
	re.Equal(defaultEnableRouterClient, o.GetEnableRouterClient(), "default enable router client")

	// Test invalid setting.
	err := o.SetMaxTSOBatchWaitInterval(time.Second)
	re.Error(err, "expect error for invalid high interval")
	// Value remains unchanged.
	re.Equal(defaultMaxTSOBatchWaitInterval, o.GetMaxTSOBatchWaitInterval(), "max TSO batch wait interval should not change to an invalid value")

	// Define a list of valid intervals.
	validIntervals := []time.Duration{
		time.Millisecond,
		time.Duration(float64(time.Millisecond) * 0.5),
		time.Duration(float64(time.Millisecond) * 1.5),
		10 * time.Millisecond,
		0,
	}
	for _, interval := range validIntervals {
		// Use a subtest for each valid interval.
		err := o.SetMaxTSOBatchWaitInterval(interval)
		re.NoError(err, "expected interval %v to be set without error", interval)
		re.Equal(interval, o.GetMaxTSOBatchWaitInterval(), "max TSO batch wait interval should be updated to %v", interval)
	}

	clearChannel(o.EnableTSOFollowerProxyCh)

	// Testing that the setting is effective and a notification is sent.
	var expectBool bool
	for _, expectBool = range []bool{true, false} {
		o.SetEnableTSOFollowerProxy(expectBool)
		testutil.Eventually(re, func() bool {
			select {
			case <-o.EnableTSOFollowerProxyCh:
			default:
				return false
			}
			return o.GetEnableTSOFollowerProxy() == expectBool
		})
	}

	// Testing that setting the same value should not trigger a notification.
	o.SetEnableTSOFollowerProxy(expectBool)
	ensureNoNotification(t, o.EnableTSOFollowerProxyCh)
>>>>>>> 2bbeb9c971 (client: support dynamic start/stop of the router client (#9082)):client/opt/option_test.go

	// This option does not use a notification channel.
	expectBool = true
<<<<<<< HEAD:client/option_test.go
	o.setEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.getEnableFollowerHandle())
	expectBool = false
	o.setEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.getEnableFollowerHandle())
=======
	o.SetEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.GetEnableFollowerHandle(), "EnableFollowerHandle should be set to true")
	expectBool = false
	o.SetEnableFollowerHandle(expectBool)
	re.Equal(expectBool, o.GetEnableFollowerHandle(), "EnableFollowerHandle should be set to false")

	expectInt := 10
	o.SetTSOClientRPCConcurrency(expectInt)
	re.Equal(expectInt, o.GetTSOClientRPCConcurrency(), "TSOClientRPCConcurrency should update accordingly")

	clearChannel(o.EnableRouterClientCh)

	// Testing that the setting is effective and a notification is sent.
	for _, expectBool = range []bool{true, false} {
		o.SetEnableRouterClient(expectBool)
		testutil.Eventually(re, func() bool {
			select {
			case <-o.EnableRouterClientCh:
			default:
				return false
			}
			return o.GetEnableRouterClient() == expectBool
		})
	}

	// Testing that setting the same value should not trigger a notification.
	o.SetEnableRouterClient(expectBool)
	ensureNoNotification(t, o.EnableRouterClientCh)
}

// clearChannel drains any pending events from the channel.
func clearChannel(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

// ensureNoNotification checks that no notification is sent on the channel within a short timeout.
func ensureNoNotification(t *testing.T, ch chan struct{}) {
	select {
	case v := <-ch:
		t.Fatalf("unexpected notification received: %v", v)
	case <-time.After(100 * time.Millisecond):
		// No notification received as expected.
	}
>>>>>>> 2bbeb9c971 (client: support dynamic start/stop of the router client (#9082)):client/opt/option_test.go
}
