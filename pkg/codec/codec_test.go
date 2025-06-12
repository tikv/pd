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

package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBytes(t *testing.T) {
	re := require.New(t)
	key := "abcdefghijklmnopqrstuvwxyz"
	for i := range key {
		_, k, err := DecodeBytes(EncodeBytes([]byte(key[:i])))
		re.NoError(err)
		re.Equal(key[:i], string(k))
	}
}

func TestTableID(t *testing.T) {
	re := require.New(t)
	key := EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0xff), key.TableID())

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff_i\x01\x02"))
	re.Equal(int64(0xff), key.TableID())

	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\xff")
	re.Equal(int64(0), key.TableID())

	key = EncodeBytes([]byte("T\x00\x00\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0), key.TableID())

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0), key.TableID())
}
