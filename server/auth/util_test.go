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

package auth

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestGenerateHash(c *C) {
	hash := GenerateHash("ItsaCrazilysEcurepass")
	c.Assert(hash, Equals, "a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037")
}

func (s *testUtilSuite) TestCompareHashAndPasswordSuite(c *C) {
	err := compareHashAndPassword("a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037", "ItsaCrazilysEcurepass")
	c.Assert(err, IsNil)

	err = compareHashAndPassword("a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037", "blablabla")
	c.Assert(err, NotNil)
}

func (s *testUtilSuite) TestValidateName(c *C) {
	ok := validateName("123abc")
	c.Assert(ok, IsFalse)

	ok = validateName("fo1-/23")
	c.Assert(ok, IsFalse)

	ok = validateName("fo_o123")
	c.Assert(ok, IsTrue)
}

func (s *testUtilSuite) TestGetUserPath(c *C) {
	c.Assert(GetUserPath(""), Equals, "users/")
	c.Assert(GetUserPath("john"), Equals, "users/john")
}

func (s *testUtilSuite) TestGetRolePath(c *C) {
	c.Assert(GetRolePath(""), Equals, "roles/")
	c.Assert(GetRolePath("john"), Equals, "roles/john")
}
