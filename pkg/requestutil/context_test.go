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
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRequestContextSuite{})

type testRequestContextSuite struct {
}

func (s *testRequestContextSuite) TestRequestInfo(c *C) {
	ctx := context.Background()
	_, ok := RequestInfoFrom(ctx)
	c.Assert(ok, Equals, false)
	timeNow := time.Now().Unix()
	ctx = WithRequestInfo(ctx,
		RequestInfo{
			ServiceLabel:   "test label",
			Method:         "POST",
			Component:      "pdctl",
			IP:             "localhost",
			URLParam:       "{\"id\"=1}",
			BodyParam:      "{\"state\"=\"Up\"}",
			StartTimeStamp: timeNow,
		})
	result, ok := RequestInfoFrom(ctx)
	c.Assert(result, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(result.ServiceLabel, Equals, "test label")
	c.Assert(result.Method, Equals, "POST")
	c.Assert(result.Component, Equals, "pdctl")
	c.Assert(result.IP, Equals, "localhost")
	c.Assert(result.URLParam, Equals, "{\"id\"=1}")
	c.Assert(result.BodyParam, Equals, "{\"state\"=\"Up\"}")
	c.Assert(result.StartTimeStamp, Equals, timeNow)
}

func (s *testRequestContextSuite) TestEndTime(c *C) {
	ctx := context.Background()
	_, ok := EndTimeFrom(ctx)
	c.Assert(ok, Equals, false)
	timeNow := time.Now().Unix()
	ctx = WithEndTime(ctx, timeNow)
	result, ok := EndTimeFrom(ctx)
	c.Assert(result, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(result, Equals, timeNow)
}
