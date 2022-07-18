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

package simutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/codec"
)

func TestGenerateTableKeys(t *testing.T) {
	re := require.New(t)
	tableCount := 3
	size := 10
	keys := GenerateTableKeys(tableCount, size)
	re.Len(keys, size)

	for i := 1; i < len(keys); i++ {
		re.Less(keys[i-1], keys[i])
		s := []byte(keys[i-1])
		e := []byte(keys[i])
		for j := 0; j < 1000; j++ {
			split, err := GenerateTiDBEncodedSplitKey(s, e)
			re.NoError(err)
			re.Less(string(s), string(split))
			re.Less(string(split), string(e))
			e = split
		}
	}
}

func TestGenerateSplitKey(t *testing.T) {
	re := require.New(t)
	s := []byte(codec.EncodeBytes([]byte("a")))
	e := []byte(codec.EncodeBytes([]byte("ab")))
	for i := 0; i <= 1000; i++ {
		cc, err := GenerateTiDBEncodedSplitKey(s, e)
		re.NoError(err)
		re.Less(string(s), string(cc))
		re.Less(string(cc), string(e))
		e = cc
	}

	// empty key
	s = []byte("")
	e = []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 0, 0, 0, 0, 0, 248}
	splitKey, err := GenerateTiDBEncodedSplitKey(s, e)
	re.NoError(err)
	re.Less(string(s), string(splitKey))
	re.Less(string(splitKey), string(e))

	// split equal key
	s = codec.EncodeBytes([]byte{116, 128, 0, 0, 0, 0, 0, 0, 1})
	e = codec.EncodeBytes([]byte{116, 128, 0, 0, 0, 0, 0, 0, 1, 1})
	for i := 0; i <= 1000; i++ {
		re.Less(string(s), string(e))
		splitKey, err = GenerateTiDBEncodedSplitKey(s, e)
		re.NoError(err)
		re.Less(string(s), string(splitKey))
		re.Less(string(splitKey), string(e))
		e = splitKey
	}
}
