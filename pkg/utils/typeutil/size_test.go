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

package typeutil

import (
	"encoding/json"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestSizeJSON(t *testing.T) {
	re := require.New(t)
	b := ByteSize(265421587)
	o, err := json.Marshal(b)
	re.NoError(err)

	var nb ByteSize
	err = json.Unmarshal(o, &nb)
	re.NoError(err)

	b = ByteSize(1756821276000)
	o, err = json.Marshal(b)
	re.NoError(err)
	re.Equal(`"1.598TiB"`, string(o))
}

func TestParseMbFromText(t *testing.T) {
	const defaultValue = 2

	re := require.New(t)
	testCases := []struct {
		body []string
		size uint64
	}{{
		body: []string{"10Mib", "10MiB", "10M", "10MB"},
		size: 10,
	}, {
		body: []string{"10GiB", "10Gib", "10G", "10GB"},
		size: 10 * units.GiB / units.MiB,
	}, {
		body: []string{"1024KiB", "1048576"},
		size: 1,
	}, {
		body: []string{"100KiB", "1023KiB", "1048575", "0"},
		size: 0,
	}, {
		body: []string{"10yiB", "10aib"},
		size: defaultValue,
	}}

	for _, testCase := range testCases {
		for _, b := range testCase.body {
			re.Equal(testCase.size, ParseMBFromText(b, defaultValue))
		}
	}
}
