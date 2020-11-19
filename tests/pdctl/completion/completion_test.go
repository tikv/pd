// Copyright 2020 TiKV Project Authors.
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

package completion_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&completionTestSuite{})

type completionTestSuite struct{}

func (s *completionTestSuite) TestCompletion(c *C) {
	cmd := pdctl.InitCommand()

	// completion command
	args := []string{"completion", "bash"}
	_, _, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// completion command
	args = []string{"completion", "zsh"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
}
