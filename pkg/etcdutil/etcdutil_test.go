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

package etcdutil

import (
	"testing"

	"net/url"

	"github.com/coreos/etcd/pkg/types"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testEtcdutilSuite{})

type testEtcdutilSuite struct {
}

func (s *testEtcdutilSuite) TestExtractURLsExcept(c *C) {
	var us []url.URL
	a, _ := url.Parse("http://a.a")
	us = append(us, *a)
	b, _ := url.Parse("http://b.a")
	us = append(us, *b)

	var um0 types.URLsMap
	um1 := types.URLsMap{"1": types.URLs(us[:1])}
	um2 := types.URLsMap{"1": types.URLs(us[:1]), "2": types.URLs(us[:])}

	c.Assert(len(ExtractURLsExcept(um0, "1", "2")), Equals, 0)
	c.Assert(ExtractURLsExcept(um1, "nonexistence"), DeepEquals, types.URLs(us[:1]).StringSlice())
	c.Assert(ExtractURLsExcept(um2, "2"), DeepEquals, types.URLs(us[:1]).StringSlice())
}
