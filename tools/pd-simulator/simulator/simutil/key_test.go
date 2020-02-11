// Copyright 2018 PingCAP, Inc.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/codec"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableKeySuite{})

type testTableKeySuite struct{}

func (t *testTableKeySuite) TestGenerateTableKeys(c *C) {
	tableCount := 3
	size := 10
	keys := GenerateTableKeys(tableCount, size)
	c.Assert(len(keys), Equals, size)

	for i := 1; i < len(keys); i++ {
		c.Assert(keys[i-1], Less, keys[i])
		s := []byte(keys[i-1])
		e := []byte(keys[i])
		for j := 0; j < 1000; j++ {
			split, err := GenerateTiDBEncodedSplitKey(s, e)
			c.Assert(err, IsNil)
			c.Assert(s, Less, split)
			c.Assert(split, Less, e)
			e = split
		}
	}

}

func (t *testTableKeySuite) TestGenerateSplitKey(c *C) {
	s := []byte(codec.EncodeBytes([]byte("a")))
	e := []byte(codec.EncodeBytes([]byte("ab")))
	for i := 0; i <= 1000; i++ {
		cc, err := GenerateTiDBEncodedSplitKey(s, e)
		c.Assert(err, IsNil)
		c.Assert(s, Less, cc)
		c.Assert(cc, Less, e)
		e = cc
	}

	// empty key
	s = []byte("")
	e = []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 0, 0, 0, 0, 0, 248}
	splitKey, err := GenerateTiDBEncodedSplitKey(s, e)
	c.Assert(err, IsNil)
	c.Assert(s, Less, splitKey)
	c.Assert(splitKey, Less, e)

	// split equal key
	s = codec.EncodeBytes([]byte{116, 128, 0, 0, 0, 0, 0, 0, 1})
	e = codec.EncodeBytes([]byte{116, 128, 0, 0, 0, 0, 0, 0, 1, 1})
	for i := 0; i <= 1000; i++ {
		c.Assert(s, Less, e)
		splitKey, err = GenerateTiDBEncodedSplitKey(s, e)
		c.Assert(err, IsNil)
		c.Assert(s, Less, splitKey)
		c.Assert(splitKey, Less, e)
		e = splitKey
	}

}
