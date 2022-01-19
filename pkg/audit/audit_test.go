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

package audit

import (
	"net/http"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/requestutil"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuditSuite{})

type testAuditSuite struct {
}

func (s *testAuditSuite) TestLabelMatcher(c *C) {
	var matcher BackendMatcher = &LabelMatcher{"testSuccess"}
	labels1 := &BackendLabels{Labels: []string{"testFail", "testSuccess"}}
	c.Assert(matcher.Match(labels1), Equals, true)

	labels2 := &BackendLabels{Labels: []string{"testFail"}}
	c.Assert(matcher.Match(labels2), Equals, false)
}

func (s *testAuditSuite) TestLocalLogBackend(c *C) {
	backend := NewLocalLogBackend()
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	info := requestutil.GetRequestInfo(req)
	c.Assert(backend.ProcessHTTPRequest(&info), Equals, true)
}
