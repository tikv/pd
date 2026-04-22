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

package types

import (
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/charset"
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/hack"
)

// Kind constants.
const (
	KindNull          byte = 0
	KindInt64         byte = 1
	KindUint64        byte = 2
	KindFloat32       byte = 3
	KindFloat64       byte = 4
	KindString        byte = 5
	KindBytes         byte = 6
	KindBinaryLiteral byte = 7 // Used for BIT / HEX literals.
	KindMysqlDecimal  byte = 8
	KindMysqlDuration byte = 9
	KindMysqlEnum     byte = 10
	KindMysqlBit      byte = 11 // Used for BIT table column values.
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindInterface     byte = 14
	KindMinNotNull    byte = 15
	KindMaxValue      byte = 16
	KindRaw           byte = 17
	KindMysqlJSON     byte = 18
	KindVectorFloat32 byte = 19
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte   // datum kind.
	decimal   uint16 // decimal can hold uint16 values.
	length    uint32 // length can hold uint32 values.
	i         int64  // i can hold int64 uint64 float64 values.
	collation string // collation hold the collation information for string value.
	b         []byte // b can hold string or []byte values.
	x         any    // x hold all other types.
}

// GetInt64 gets int64 value.
func (d *Datum) GetInt64() int64 {
	return d.i
}

// SetInt64 sets int64 value.
func (d *Datum) SetInt64(i int64) {
	d.k = KindInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Datum) GetUint64() uint64 {
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Datum) SetUint64(i uint64) {
	d.k = KindUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Datum) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Datum) SetFloat64(f float64) {
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Datum) GetFloat32() float32 {
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
	d.collation = charset.CollationBin
}

// SetMysqlDecimal sets decimal value
func (d *Datum) SetMysqlDecimal(b *MyDecimal) {
	d.k = KindMysqlDecimal
	d.x = b
}

// SetLength sets the length of the datum.
func (d *Datum) SetLength(l int) {
	d.length = uint32(l)
}

// SetFrac sets the frac of the datum.
func (d *Datum) SetFrac(frac int) {
	d.decimal = uint16(frac)
}

// SetMysqlDuration sets Duration value
func (d *Datum) SetMysqlDuration(b Duration) {
	d.k = KindMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// SetMysqlJSON sets json.BinaryJSON value
func (d *Datum) SetMysqlJSON(b BinaryJSON) {
	d.k = KindMysqlJSON
	d.i = int64(b.TypeCode)
	d.b = b.Value
}

// SetVectorFloat32 sets VectorFloat32 value
func (d *Datum) SetVectorFloat32(vec VectorFloat32) {
	d.k = KindVectorFloat32
	d.b = vec.ZeroCopySerialize()
}

// GetString gets string value.
func (d *Datum) GetString() string {
	return string(hack.String(d.b))
}

// GetMysqlTime gets types.Time value
func (d *Datum) GetMysqlTime() Time {
	return d.x.(Time)
}

// GetMysqlDuration gets Duration value
func (d *Datum) GetMysqlDuration() Duration {
	return Duration{Duration: time.Duration(d.i), Fsp: int(int8(d.decimal))}
}

// GetMysqlDecimal gets decimal value
func (d *Datum) GetMysqlDecimal() *MyDecimal {
	return d.x.(*MyDecimal)
}

// GetMysqlEnum gets Enum value
func (d *Datum) GetMysqlEnum() Enum {
	str := string(hack.String(d.b))
	return Enum{Value: uint64(d.i), Name: str}
}

// GetMysqlSet gets Set value
func (d *Datum) GetMysqlSet() Set {
	str := string(hack.String(d.b))
	return Set{Value: uint64(d.i), Name: str}
}

// GetMysqlJSON gets json.BinaryJSON value
func (d *Datum) GetMysqlJSON() BinaryJSON {
	return BinaryJSON{TypeCode: byte(d.i), Value: d.b}
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// GetVectorFloat32 gets VectorFloat32 value
func (d *Datum) GetVectorFloat32() VectorFloat32 {
	v, _, err := ZeroCopyDeserializeVectorFloat32(d.b)
	if err != nil {
		panic(err)
	}
	return v
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	if d.b != nil {
		return d.b
	}
	return []byte{}
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() any {
	return d.x
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	return d.k
}

// ToString gets the string representation of the datum.
func (d *Datum) ToString() (string, error) {
	switch d.Kind() {
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case KindFloat32:
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case KindString:
		return d.GetString(), nil
	case KindBytes:
		return d.GetString(), nil
	case KindMysqlTime:
		return d.GetMysqlTime().String(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().String(), nil
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().String(), nil
	case KindMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().String(), nil
	case KindMysqlJSON:
		return d.GetMysqlJSON().String(), nil
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral().ToString(), nil
	case KindVectorFloat32:
		return d.GetVectorFloat32().String(), nil
	case KindNull:
		return "", nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() any {
	switch d.k {
	case KindInt64:
		return d.GetInt64()
	case KindUint64:
		return d.GetUint64()
	case KindFloat32:
		return d.GetFloat32()
	case KindFloat64:
		return d.GetFloat64()
	case KindString:
		return d.GetString()
	case KindBytes:
		return d.GetBytes()
	case KindMysqlDecimal:
		return d.GetMysqlDecimal()
	case KindMysqlDuration:
		return d.GetMysqlDuration()
	case KindMysqlEnum:
		return d.GetMysqlEnum()
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral()
	case KindMysqlSet:
		return d.GetMysqlSet()
	case KindMysqlJSON:
		return d.GetMysqlJSON()
	case KindMysqlTime:
		return d.GetMysqlTime()
	case KindVectorFloat32:
		return d.GetVectorFloat32()
	default:
		return d.GetInterface()
	}
}
