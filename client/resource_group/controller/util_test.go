// Copyright 2023 TiKV Project Authors.
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

package controller

import (
	"encoding/json"
	"testing"

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
