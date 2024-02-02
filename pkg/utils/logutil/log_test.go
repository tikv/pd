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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestStringToZapLogLevel(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	re := require.New(t)
	testCases := []struct {
		name            string
		arg             any
		enableRedactLog bool
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
	}

	for _, testCase := range testCases {
		t.Log(testCase.name)
		SetRedactLog(testCase.enableRedactLog)
		switch r := testCase.arg.(type) {
		case []byte:
			re.Equal(testCase.expect, RedactBytes(r))
		case string:
			re.Equal(testCase.expect, RedactString(r))
		case fmt.Stringer:
			re.Equal(testCase.expect, RedactStringer(r))
		default:
			panic("unmatched case")
		}
	}
}
