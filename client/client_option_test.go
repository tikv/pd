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
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/testutil"
)

var _ = Suite(&testClientOptionSuite{})

type testClientOptionSuite struct{}

func (s *testClientSuite) TestDynamicOptionChange(c *C) {
	co := NewClientOption()
	// Check the default value setting.
	c.Assert(co.GetMaxTSOBatchWaitInterval(), Equals, time.Duration(defaultMaxTSOBatchWaitInterval))
	c.Assert(co.GetTSOFollowerProxyOption(), Equals, defaultEnableTSOFollowerProxy)

	// Check the invalid value setting.
	co.SetMaxTSOBatchWaitInterval(time.Second)
	c.Assert(co.GetMaxTSOBatchWaitInterval(), Equals, time.Duration(defaultMaxTSOBatchWaitInterval))
	expectInterval := time.Millisecond
	co.SetMaxTSOBatchWaitInterval(expectInterval)
	c.Assert(co.GetMaxTSOBatchWaitInterval(), Equals, expectInterval)
	co.SetMaxTSOBatchWaitInterval(expectInterval)
	c.Assert(co.GetMaxTSOBatchWaitInterval(), Equals, expectInterval)

	expectBool := true
	co.SetTSOFollowerProxyOption(expectBool)
	// Check the value changing notification.
	testutil.WaitUntil(c, func(c *C) bool {
		<-co.enableTSOFollowerProxyCh
		return true
	})
	c.Assert(co.GetTSOFollowerProxyOption(), Equals, expectBool)
	// Check whether any data will be sent to the channel.
	// It will panic if the test fails.
	close(co.enableTSOFollowerProxyCh)
	// Setting the same value should not notify the channel.
	co.SetTSOFollowerProxyOption(expectBool)
}
