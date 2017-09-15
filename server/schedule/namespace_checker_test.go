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

package schedule

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/namespace"
)

var cluster = newMockClusterInfo()

var namespaceChecker = NewNamespaceChecker(nil, cluster, namespace.DefaultClassifier)

var region = newMockRegionInfo(1)

type testNameSpaceCheckerSuite struct{}

func (s *testNameSpaceCheckerSuite) TestChecker(c *C) {
	o := namespaceChecker.Check(region)
	c.Assert(o, IsNil)

}
