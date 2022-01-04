// Copyright 2017 TiKV Project Authors.
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

package logutil

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"go.uber.org/zap/zapcore"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLogSuite{})

type testLogSuite struct{}

func (s *testLogSuite) TestStringToZapLogLevel(c *C) {
	c.Assert(StringToZapLogLevel("fatal"), Equals, zapcore.FatalLevel)
	c.Assert(StringToZapLogLevel("ERROR"), Equals, zapcore.ErrorLevel)
	c.Assert(StringToZapLogLevel("warn"), Equals, zapcore.WarnLevel)
	c.Assert(StringToZapLogLevel("warning"), Equals, zapcore.WarnLevel)
	c.Assert(StringToZapLogLevel("debug"), Equals, zapcore.DebugLevel)
	c.Assert(StringToZapLogLevel("info"), Equals, zapcore.InfoLevel)
	c.Assert(StringToZapLogLevel("whatever"), Equals, zapcore.InfoLevel)
}

func (s *testLogSuite) TestRedactLog(c *C) {
	testcases := []struct {
		name            string
		arg             interface{}
		enableRedactLog bool
		expect          interface{}
	}{
		{
			name:            "string arg, enable redact",
			arg:             "foo",
			enableRedactLog: true,
			expect:          "?",
		},
		{
			name:            "string arg",
			arg:             "foo",
			enableRedactLog: false,
			expect:          "foo",
		},
		{
			name:            "[]byte arg, enable redact",
			arg:             []byte("foo"),
			enableRedactLog: true,
			expect:          []byte("?"),
		},
		{
			name:            "[]byte arg",
			arg:             []byte("foo"),
			enableRedactLog: false,
			expect:          []byte("foo"),
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		SetRedactLog(testcase.enableRedactLog)
		switch r := testcase.arg.(type) {
		case []byte:
			c.Assert(RedactBytes(r), DeepEquals, testcase.expect)
		case string:
			c.Assert(RedactString(r), DeepEquals, testcase.expect)
		case fmt.Stringer:
			c.Assert(RedactStringer(r), DeepEquals, testcase.expect)
		default:
			panic("unmatched case")
		}
	}
}
