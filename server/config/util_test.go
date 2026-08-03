// Copyright 2018 TiKV Project Authors.
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateURLWithScheme(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		addr   string
		hasErr bool
	}{
		{"", true},
		{"foo", true},
		{"/foo", true},
		{"http", true},
		{"http://", true},
		{"http://foo", false},
		{"https://foo", false},
		{"http://127.0.0.1", false},
		{"http://127.0.0.1/", false},
		{"https://foo.com/bar", false},
		{"https://foo.com/bar/", false},
	}
	for _, test := range tests {
		re.Equal(test.hasErr, ValidateURLWithScheme(test.addr) != nil)
	}
}

func TestValidateMetricStorageURL(t *testing.T) {
	for _, rawURL := range []string{
		"",
		"http://metrics.example",
		"HTTPS://metrics.example:9090/prometheus",
		"http://192.168.0.1:9090",
		"http://[fc00::1]:9090",
	} {
		require.NoError(t, ValidateMetricStorageURL(rawURL), rawURL)
	}

	for _, rawURL := range []string{
		"file:///tmp/metrics",
		"http://user:pass@metrics.example",
		"http://metrics.example:",
		"http://metrics.example:70000",
		"http://metrics.example/#secret",
		"http://localhost:9090",
		"http://metrics.localhost:9090",
		"http://127.1.2.3:9090",
		"http://[::1]:9090",
		"http://[::ffff:127.0.0.1]:9090",
		"http://0.0.0.0:9090",
		"http://169.254.169.254:9090",
		"http://100.100.100.200:9090",
		"http://[fd00:ec2::254]:9090",
	} {
		require.Error(t, ValidateMetricStorageURL(rawURL), rawURL)
	}
}
