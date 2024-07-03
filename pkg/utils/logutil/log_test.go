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
	"encoding/json"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
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

func TestRedactInfoLogType(t *testing.T) {
	re := require.New(t)
	// JSON unmarshal.
	jsonUnmarshalTestCases := []struct {
		jsonStr   string
		expect    RedactInfoLogType
		expectErr bool
	}{
		{`false`, RedactInfoLogOFF, false},
		{`true`, RedactInfoLogON, false},
		{`"MARKER"`, RedactInfoLogMarker, false},
		{`"marker"`, RedactInfoLogMarker, false},
		{`"OTHER"`, RedactInfoLogOFF, true},
		{`"OFF"`, RedactInfoLogOFF, true},
		{`"ON"`, RedactInfoLogOFF, true},
		{`"off"`, RedactInfoLogOFF, true},
		{`"on"`, RedactInfoLogOFF, true},
		{`""`, RedactInfoLogOFF, true},
		{`"fALSe"`, RedactInfoLogOFF, true},
		{`"trUE"`, RedactInfoLogOFF, true},
	}
	var redactType RedactInfoLogType
	for idx, tc := range jsonUnmarshalTestCases {
		t.Logf("test case %d: %s", idx, tc.jsonStr)
		err := json.Unmarshal([]byte(tc.jsonStr), &redactType)
		if tc.expectErr {
			re.Error(err)
			re.ErrorContains(err, invalidRedactInfoLogTypeErrMsg)
		} else {
			re.NoError(err)
			re.Equal(tc.expect, redactType)
		}
	}
	// JSON marshal.
	jsonMarshalTestCases := []struct {
		typ    RedactInfoLogType
		expect string
	}{
		{RedactInfoLogOFF, `false`},
		{RedactInfoLogON, `true`},
		{RedactInfoLogMarker, `"MARKER"`},
	}
	for _, tc := range jsonMarshalTestCases {
		b, err := json.Marshal(tc.typ)
		re.NoError(err)
		re.Equal(tc.expect, string(b))
	}
	// TOML unmarshal.
	tomlTestCases := []struct {
		tomlStr   string
		expect    RedactInfoLogType
		expectErr bool
	}{
		{`redact-info-log = false`, RedactInfoLogOFF, false},
		{`redact-info-log = true`, RedactInfoLogON, false},
		{`redact-info-log = "MARKER"`, RedactInfoLogMarker, false},
		{`redact-info-log = "marker"`, RedactInfoLogMarker, false},
		{`redact-info-log = "OTHER"`, RedactInfoLogOFF, true},
		{`redact-info-log = "OFF"`, RedactInfoLogOFF, true},
		{`redact-info-log = "ON"`, RedactInfoLogOFF, true},
		{`redact-info-log = "off"`, RedactInfoLogOFF, true},
		{`redact-info-log = "on"`, RedactInfoLogOFF, true},
		{`redact-info-log = ""`, RedactInfoLogOFF, true},
		{`redact-info-log = "fALSe"`, RedactInfoLogOFF, true},
		{`redact-info-log = "trUE"`, RedactInfoLogOFF, true},
	}
	var config struct {
		RedactInfoLog RedactInfoLogType `toml:"redact-info-log"`
	}
	for _, tc := range tomlTestCases {
		_, err := toml.Decode(tc.tomlStr, &config)
		if tc.expectErr {
			re.Error(err)
			re.ErrorContains(err, invalidRedactInfoLogTypeErrMsg)
		} else {
			re.NoError(err)
			re.Equal(tc.expect, config.RedactInfoLog)
		}
	}
}

func TestRedactLog(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name              string
		arg               any
		redactInfoLogType RedactInfoLogType
		expect            any
	}{
		{
			name:              "string arg, enable redact",
			arg:               "foo",
			redactInfoLogType: RedactInfoLogON,
			expect:            "?",
		},
		{
			name:              "string arg",
			arg:               "foo",
			redactInfoLogType: RedactInfoLogOFF,
			expect:            "foo",
		},
		{
			name:              "[]byte arg, enable redact",
			arg:               []byte("foo"),
			redactInfoLogType: RedactInfoLogON,
			expect:            []byte("?"),
		},
		{
			name:              "[]byte arg",
			arg:               []byte("foo"),
			redactInfoLogType: RedactInfoLogOFF,
			expect:            []byte("foo"),
		},
		{
			name:              "string arg, enable redact marker",
			arg:               "foo",
			redactInfoLogType: RedactInfoLogMarker,
			expect:            "‹foo›",
		},
		{
			name:              "string arg contains left marker, enable redact marker",
			arg:               "f‹oo",
			redactInfoLogType: RedactInfoLogMarker,
			expect:            "‹f‹‹oo›",
		},
		{
			name:              "string arg contains right marker, enable redact marker",
			arg:               "foo›",
			redactInfoLogType: RedactInfoLogMarker,
			expect:            "‹foo›››",
		},
		{
			name:              "string arg contains marker, enable redact marker",
			arg:               "f‹oo›",
			redactInfoLogType: RedactInfoLogMarker,
			expect:            "‹f‹‹oo›››",
		},
		{
			name:              "[]byte arg, enable redact marker",
			arg:               []byte("foo"),
			redactInfoLogType: RedactInfoLogMarker,
			expect:            []byte("‹foo›"),
		},
		{
			name:              "[]byte arg contains left marker, enable redact marker",
			arg:               []byte("foo‹"),
			redactInfoLogType: RedactInfoLogMarker,
			expect:            []byte("‹foo‹‹›"),
		},
		{
			name:              "[]byte arg contains right marker, enable redact marker",
			arg:               []byte("›foo"),
			redactInfoLogType: RedactInfoLogMarker,
			expect:            []byte("‹››foo›"),
		},
		{
			name:              "[]byte arg contains marker, enable redact marker",
			arg:               []byte("f›o‹o"),
			redactInfoLogType: RedactInfoLogMarker,
			expect:            []byte("‹f››o‹‹o›"),
		},
	}

	for _, testCase := range testCases {
		setRedactType(testCase.redactInfoLogType)
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
