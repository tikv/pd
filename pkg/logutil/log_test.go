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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestStringToZapLogLevel(t *testing.T) {
	require.Equal(t, zapcore.FatalLevel, StringToZapLogLevel("fatal"))
	require.Equal(t, zapcore.ErrorLevel, StringToZapLogLevel("ERROR"))
	require.Equal(t, zapcore.WarnLevel, StringToZapLogLevel("warn"))
	require.Equal(t, zapcore.WarnLevel, StringToZapLogLevel("warning"))
	require.Equal(t, zapcore.DebugLevel, StringToZapLogLevel("debug"))
	require.Equal(t, zapcore.InfoLevel, StringToZapLogLevel("info"))
	require.Equal(t, zapcore.InfoLevel, StringToZapLogLevel("whatever"))
}

func TestRedactLog(t *testing.T) {
	testCases := []struct {
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

	for _, testCase := range testCases {
		t.Log(testCase.name)
		SetRedactLog(testCase.enableRedactLog)
		switch r := testCase.arg.(type) {
		case []byte:
			require.True(t, reflect.DeepEqual(testCase.expect, RedactBytes(r)))
		case string:
			require.True(t, reflect.DeepEqual(testCase.expect, RedactString(r)))
		case fmt.Stringer:
			require.True(t, reflect.DeepEqual(testCase.expect, RedactStringer(r)))
		default:
			panic("unmatched case")
		}
	}
}
