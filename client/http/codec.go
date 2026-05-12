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
	"encoding/hex"

	"github.com/pingcap/errors"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var pads = make([]byte, encGroupSize)

// encodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//
//	[group1][marker1]...[groupN][markerN]
//	group is 8 bytes slice which is padding with 0.
//	marker is `0xFF - padding 0 count`
//
// For example:
//
//	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
//
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func encodeBytes(data []byte) []byte {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	result := make([]byte, 0, (dLen/encGroupSize+1)*(encGroupSize+1))
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}
	return result
}

func decodeBytes(b []byte) ([]byte, error) {
	buf := make([]byte, 0, len(b))
	for {
		if len(b) < encGroupSize+1 {
			return nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		padCount := encMarker - marker
		if padCount > encGroupSize {
			return nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		buf = append(buf, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != encPad {
					return nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return buf, nil
}

// rawKeyToKeyHexStr converts a raw key to a hex string after encoding.
func rawKeyToKeyHexStr(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return hex.EncodeToString(encodeBytes(key))
}

// keyHexStrToRawKey converts a hex string to a raw key after decoding.
func keyHexStrToRawKey(hexKey string) ([]byte, error) {
	if len(hexKey) == 0 {
		return make([]byte, 0), nil
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	return decodeBytes(key)
}
