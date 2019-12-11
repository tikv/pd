// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct{}

func (s *testClientSuite) TestTsLessEqual(c *C) {
	c.Assert(tsLessEqual(9, 9, 9, 9), IsTrue)
	c.Assert(tsLessEqual(8, 9, 9, 8), IsTrue)
	c.Assert(tsLessEqual(9, 8, 8, 9), IsFalse)
	c.Assert(tsLessEqual(9, 8, 9, 6), IsFalse)
	c.Assert(tsLessEqual(9, 6, 9, 8), IsTrue)
}

var _ = Suite(&testClientCtxSuite{})

type testClientCtxSuite struct{}

func (s *testClientCtxSuite) TestClientCtx(c *C) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	_, err := NewClientWithContext(ctx, []string{"localhost:8080"}, SecurityOption{})
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), Less, time.Second*4)
}
