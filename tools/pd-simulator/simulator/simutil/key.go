// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package simutil

import (
	"bytes"
	"math/rand"
	"sort"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/codec"
)

const (
	// 26^10 ~= 1.4e+14, should be enough.
	keyChars = "abcdefghijklmnopqrstuvwxyz"
	keyLen   = 10
)

// GenerateKeys generates ordered, unique strings.
func GenerateKeys(size int) []string {
	m := make(map[string]struct{}, size)
	for len(m) < size {
		k := make([]byte, keyLen)
		for i := range k {
			k[i] = keyChars[rand.Intn(len(keyChars))]
		}
		m[string(k)] = struct{}{}
	}

	v := make([]string, 0, size)
	for k := range m {
		v = append(v, k)
	}
	sort.Strings(v)
	return v
}

// GenerateTableKey generates the table key according to the table ID and row ID.
func GenerateTableKey(tableID, rowID int64) []byte {
	key := codec.GenerateRowKey(tableID, rowID)
	// append 0xFF use to split
	key = append(key, 0xFF)

	return codec.EncodeBytes(key)
}

// GenerateTableKeys generates the table keys according to the table count and size.
func GenerateTableKeys(tableCount, size int) []string {
	v := make([]string, 0, size)
	groupNumber := size / tableCount
	tableID := 0
	var key []byte
	for size > 0 {
		tableID++
		for rowID := 0; rowID < groupNumber && size > 0; rowID++ {
			key = GenerateTableKey(int64(tableID), int64(rowID))
			v = append(v, string(key))
			size--
		}
	}
	return v
}

// GenerateSplitKey generate the split key.
func GenerateSplitKey(start, end []byte) []byte {
	var key []byte
	// lessThanEnd is set as true when the key is already less than end key.
	lessThanEnd := len(end) == 0
	for i, s := range start {
		e := byte('z')
		if !lessThanEnd {
			e = end[i]
		}
		c := (s + e) / 2
		key = append(key, c)
		// case1: s = c < e. Continue with lessThanEnd=true.
		// case2: s < c < e. return key.
		// case3: s = c = e. Continue with lessThanEnd=false.
		lessThanEnd = c < e
		if c > s && c < e {
			return key
		}
	}
	key = append(key, ('a'+'z')/2)
	return key
}

func mustDecodeMvccKey(key []byte) ([]byte, error) {
	// FIXME: seems nil key not encode to order compare key
	if len(key) == 0 {
		return nil, nil
	}

	left, res, err := codec.DecodeBytes(key)
	if len(left) > 0 {
		return nil, errors.Errorf("decode key left some bytes, key: %s", string(key))
	}
	if err != nil {
		return nil, errors.Errorf("decode key meet error: %s, key: %s", err, string(res))
	}
	return res, nil
}

// GenerateTiDBEncodedSplitKey calculates the split key with start and end key,
// the keys are encoded according to the TiDB encoding rules.
func GenerateTiDBEncodedSplitKey(start, end []byte) ([]byte, error) {
	if len(start) == 0 && len(end) == 0 {
		// suppose use table key with table ID 0 and row ID 0.
		return GenerateTableKey(0, 0), nil
	}

	var err error
	start, err = mustDecodeMvccKey(start)
	if err != nil {
		return nil, err
	}
	end, err = mustDecodeMvccKey(end)
	if err != nil {
		return nil, err
	}
	originStartLen := len(start)

	// make the start key and end key in same length.
	if len(end) == 0 {
		_, tableID, err := codec.DecodeInt(start[1:])
		if err != nil {
			return nil, err
		}
		return GenerateTableKey(tableID+1, 0), nil
	} else if len(start) < len(end) {
		pad := make([]byte, len(end)-len(start))
		start = append(start, pad...)
	} else if len(end) < len(start) {
		pad := make([]byte, len(start)-len(end))
		end = append(end, pad...)
	}

	switch bytes.Compare(start, end) {
	case 0, 1:
		return nil, errors.Errorf("invalid key, start key: %s, end key: %s", string(start[:originStartLen]), string(end))
	case -1:
	}
	for i := len(end) - 1; i >= 0; i-- {
		if i == 0 {
			return nil, errors.Errorf("invalid key to split, end key: %s ", string(end))
		}
		if end[i] == 0 {
			end[i] = 0xFF
		} else {
			end[i]--
			break
		}
	}
	// if endKey equal to startKey after reduce 1.
	// we append 0xFF to the split key
	if bytes.Equal(end, start) {
		end = append(end, 0xFF)
	}
	return codec.EncodeBytes(end), nil
}
