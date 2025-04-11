// Copyright 2016 TiKV Project Authors.
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
	"math"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

type example struct {
	Interval Duration `json:"interval" toml:"interval"`
}

func TestDurationJSON(t *testing.T) {
	re := require.New(t)
	example := &example{}

	text := []byte(`{"interval":"1h1m1s"}`)
	re.NoError(json.Unmarshal(text, example))
	re.Equal(float64(60*60+60+1), example.Interval.Seconds())

	b, err := json.Marshal(example)
	re.NoError(err)
	re.Equal(string(text), string(b))
}

func TestDurationTOML(t *testing.T) {
	re := require.New(t)
	example := &example{}

	text := []byte(`interval = "1h1m1s"`)
	re.NoError(toml.Unmarshal(text, example))
	re.Equal(float64(60*60+60+1), example.Interval.Seconds())
}

func TestSaturatingStdDurationFromSeconds(t *testing.T) {
	re := require.New(t)

	re.Equal(time.Second*2, SaturatingStdDurationFromSeconds(2))
	re.Equal(time.Duration(0), SaturatingStdDurationFromSeconds(-2))
	re.Equal(time.Hour, SaturatingStdDurationFromSeconds(3600))
	re.Equal(time.Duration(math.MaxInt64), SaturatingStdDurationFromSeconds(1<<34))
	re.Equal((1<<33)*time.Second, SaturatingStdDurationFromSeconds(1<<33))
	re.Equal(9223372036*time.Second, SaturatingStdDurationFromSeconds(9223372036))
	re.Equal(time.Duration(math.MaxInt64), SaturatingStdDurationFromSeconds(9223372037))
	re.Equal(time.Duration(math.MaxInt64), SaturatingStdDurationFromSeconds(math.MaxInt64))
}
