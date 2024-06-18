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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestStringToZapLogLevel(t *testing.T) {
	re := require.New(t)
	re.Equal(zapcore.FatalLevel, StringToZapLogLevel("fatal"))
	re.Equal(zapcore.ErrorLevel, StringToZapLogLevel("ERROR"))
	re.Equal(zapcore.WarnLevel, StringToZapLogLevel("warn"))
	re.Equal(zapcore.WarnLevel, StringToZapLogLevel("warning"))
	re.Equal(zapcore.DebugLevel, StringToZapLogLevel("debug"))
	re.Equal(zapcore.InfoLevel, StringToZapLogLevel("info"))
	re.Equal(zapcore.InfoLevel, StringToZapLogLevel("whatever"))
}

func TestRedactLog(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name            string
		arg             any
		enableRedactLog bool
		redactLogMark   string
		expect          any
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
		{
			name:            "string arg, enable redact mark",
			arg:             "foo",
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          "<foo>",
		},
		{
			name:            "string arg contains left mark, enable redact mark",
			arg:             "f<oo",
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          "<f<<oo>",
		},
		{
			name:            "string arg contains right mark, enable redact mark",
			arg:             "foo>",
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          "<foo>>>",
		},
		{
			name:            "string arg contains mark, enable redact mark",
			arg:             "f<oo>",
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          "<f<<oo>>>",
		},
		{
			name:            "[]byte arg, enable redact mark",
			arg:             []byte("foo"),
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          []byte("<foo>"),
		},
		{
			name:            "[]byte arg contains left mark, enable redact mark",
			arg:             []byte("foo<"),
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          []byte("<foo<<>"),
		},
		{
			name:            "[]byte arg contains right mark, enable redact mark",
			arg:             []byte(">foo"),
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          []byte("<>>foo>"),
		},
		{
			name:            "[]byte arg contains mark, enable redact mark",
			arg:             []byte("f>o<o"),
			enableRedactLog: true,
			redactLogMark:   "<>",
			expect:          []byte("<f>>o<<o>"),
		},
	}

	for _, testCase := range testCases {
		setRedactType(testCase.enableRedactLog, testCase.redactLogMark)
		// Create `fmt.Stringer`s to test `RedactStringer` later.
		var argStringer, expectStringer = &strings.Builder{}, &strings.Builder{}
		switch r := testCase.arg.(type) {
		case []byte:
			re.Equal(testCase.expect, RedactBytes(r), testCase.name)
			argStringer.Write((testCase.arg).([]byte))
			expectStringer.Write((testCase.expect).([]byte))
		case string:
			re.Equal(testCase.expect, RedactString(r), testCase.name)
			argStringer.WriteString((testCase.arg).(string))
			expectStringer.WriteString((testCase.expect).(string))
		default:
			re.FailNow("unmatched case", testCase.name)
		}
		re.Equal(expectStringer.String(), RedactStringer(argStringer).String(), testCase.name)
	}
}
