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

package assertutil

import (
	"errors"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testAssertUtilSuite{})

type testAssertUtilSuite struct{}

func (s *testAssertUtilSuite) TestNilFail(c *check.C) {
	var failErr error
	checker := NewChecker(func() {
		failErr = errors.New("called assert func not exist")
	})
	c.Assert(checker.IsNil, check.IsNil)
	checker.AssertNil(nil)
	c.Assert(failErr, check.NotNil)
}
