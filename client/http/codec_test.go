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

package http

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesCodec(t *testing.T) {
	inputs := []struct {
		enc []byte
		dec []byte
	}{
		{[]byte{}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 247}},
		{[]byte{0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 248}},
		{[]byte{1, 2, 3}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 250}},
		{[]byte{1, 2, 3, 0}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 251}},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{1, 2, 3, 4, 5, 6, 7, 0, 254}},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247}},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247}},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248}},
	}

	for _, input := range inputs {
		b := encodeBytes(input.enc)
		require.Equal(t, input.dec, b)

		d, err := decodeBytes(b)
		require.NoError(t, err)
		require.Equal(t, input.enc, d)
	}

	// Test error decode.
	errInputs := [][]byte{
		{1, 2, 3, 4},
		{0, 0, 0, 0, 0, 0, 0, 247},
		{0, 0, 0, 0, 0, 0, 0, 0, 246},
		{0, 0, 0, 0, 0, 0, 0, 1, 247},
		{1, 2, 3, 4, 5, 6, 7, 8, 0},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0},
	}

	for _, input := range errInputs {
		_, err := decodeBytes(input)
		require.Error(t, err)
	}
}
