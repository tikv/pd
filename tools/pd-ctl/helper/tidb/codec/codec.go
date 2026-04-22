// Copyright 2025 TiKV Project Authors.
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

// Notes: it's a copy from tidb

package codec

import (
	"time"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/types"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag           byte = 0
	bytesFlag         byte = 1
	compactBytesFlag  byte = 2
	intFlag           byte = 3
	uintFlag          byte = 4
	floatFlag         byte = 5
	decimalFlag       byte = 6
	durationFlag      byte = 7
	varintFlag        byte = 8
	uvarintFlag       byte = 9
	jsonFlag          byte = 10
	vectorFloat32Flag byte = 20
	maxFlag           byte = 250
)

// DecodeOne decodes on datum from a byte slice generated with EncodeKey or EncodeValue.
func DecodeOne(b []byte) (remain []byte, d types.Datum, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		d.SetInt64(v)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		d.SetUint64(v)
	case varintFlag:
		var v int64
		b, v, err = DecodeVarint(b)
		d.SetInt64(v)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeUvarint(b)
		d.SetUint64(v)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		d.SetFloat64(v)
	case bytesFlag:
		var v []byte
		b, v, err = DecodeBytes(b, nil)
		d.SetBytes(v)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		var (
			dec             *types.MyDecimal
			precision, frac int
		)
		b, dec, precision, frac, err = DecodeDecimal(b)
		if err == nil {
			d.SetMysqlDecimal(dec)
			d.SetLength(precision)
			d.SetFrac(frac)
		}
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetMysqlDuration(v)
		}
	case jsonFlag:
		var size int
		size, err = types.PeekBytesAsJSON(b)
		if err != nil {
			return b, d, err
		}
		j := types.BinaryJSON{TypeCode: b[0], Value: b[1:size]}
		d.SetMysqlJSON(j)
		b = b[size:]
	case vectorFloat32Flag:
		v, remaining, err := types.ZeroCopyDeserializeVectorFloat32(b)
		if err != nil {
			return b, d, errors.Trace(err)
		}
		d.SetVectorFloat32(v)
		b = remaining
	case NilFlag:
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	return b, d, nil
}
