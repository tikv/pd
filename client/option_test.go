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
	o := newClientOption()
	// Check the default value setting.
	c.Assert(o.getMaxTSOBatchWaitInterval(), Equals, time.Duration(defaultMaxTSOBatchWaitInterval))
	c.Assert(o.getTSOFollowerProxyOption(), Equals, defaultEnableTSOFollowerProxy)

	// Check the invalid value setting.
	c.Assert(o.setMaxTSOBatchWaitInterval(time.Second), NotNil)
	c.Assert(o.getMaxTSOBatchWaitInterval(), Equals, time.Duration(defaultMaxTSOBatchWaitInterval))
	expectInterval := time.Millisecond
	o.setMaxTSOBatchWaitInterval(expectInterval)
	c.Assert(o.getMaxTSOBatchWaitInterval(), Equals, expectInterval)
	o.setMaxTSOBatchWaitInterval(expectInterval)
	c.Assert(o.getMaxTSOBatchWaitInterval(), Equals, expectInterval)

	expectBool := true
	o.setTSOFollowerProxyOption(expectBool)
	// Check the value changing notification.
	testutil.WaitUntil(c, func(c *C) bool {
		<-o.enableTSOFollowerProxyCh
		return true
	})
	c.Assert(o.getTSOFollowerProxyOption(), Equals, expectBool)
	// Check whether any data will be sent to the channel.
	// It will panic if the test fails.
	close(o.enableTSOFollowerProxyCh)
	// Setting the same value should not notify the channel.
	o.setTSOFollowerProxyOption(expectBool)
}
