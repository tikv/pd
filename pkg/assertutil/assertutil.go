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

// Checker accepts the injection of check functions and context from test files.
// Any check function should be set before usage unless the test will fail.
type Checker struct {
	IsNil   func(obtained interface{})
	FailNow func()
}

// NewChecker creates Checker with FailNow function.
func NewChecker(failNow func()) *Checker {
	return &Checker{
		FailNow: failNow,
	}
}

func (c *Checker) failNow() {
	c.FailNow()
}

// AssertNil calls the injected IsNil assertion.
func (c *Checker) AssertNil(obtained interface{}) {
	if c.IsNil == nil {
		c.failNow()
		return
	}
	c.IsNil(obtained)
}
