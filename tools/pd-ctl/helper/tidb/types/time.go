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
	gotime "time"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/mysql"
)

const (
	// Core time bit fields.
	yearBitFieldOffset, yearBitFieldWidth               uint64 = 50, 14
	monthBitFieldOffset, monthBitFieldWidth             uint64 = 46, 4
	dayBitFieldOffset, dayBitFieldWidth                 uint64 = 41, 5
	hourBitFieldOffset, hourBitFieldWidth               uint64 = 36, 5
	minuteBitFieldOffset, minuteBitFieldWidth           uint64 = 30, 6
	secondBitFieldOffset, secondBitFieldWidth           uint64 = 24, 6
	microsecondBitFieldOffset, microsecondBitFieldWidth uint64 = 4, 20

	// fspTt bit field.
	// `fspTt` format:
	// | fsp: 3 bits | type: 1 bit |
	// When `fsp` is valid (in range [0, 6]):
	// 1. `type` bit 0 represent `DateTime`
	// 2. `type` bit 1 represent `Timestamp`
	//
	// Since s`Date` does not require `fsp`, we could use `fspTt` == 0b1110 to represent it.
	fspTtBitFieldOffset, fspTtBitFieldWidth uint64 = 0, 4

	yearBitFieldMask        uint64 = ((1 << yearBitFieldWidth) - 1) << yearBitFieldOffset
	monthBitFieldMask       uint64 = ((1 << monthBitFieldWidth) - 1) << monthBitFieldOffset
	dayBitFieldMask         uint64 = ((1 << dayBitFieldWidth) - 1) << dayBitFieldOffset
	hourBitFieldMask        uint64 = ((1 << hourBitFieldWidth) - 1) << hourBitFieldOffset
	minuteBitFieldMask      uint64 = ((1 << minuteBitFieldWidth) - 1) << minuteBitFieldOffset
	secondBitFieldMask      uint64 = ((1 << secondBitFieldWidth) - 1) << secondBitFieldOffset
	microsecondBitFieldMask uint64 = ((1 << microsecondBitFieldWidth) - 1) << microsecondBitFieldOffset
	fspTtBitFieldMask       uint64 = ((1 << fspTtBitFieldWidth) - 1) << fspTtBitFieldOffset

	fspTtForDate         uint   = 0b1110
	fspBitFieldMask      uint64 = 0b1110
	coreTimeBitFieldMask        = ^fspTtBitFieldMask
)

// Zero values for different types.
var (
	// ZeroCoreTime is the zero value for Time type.
	ZeroTime = Time{}
)

// coreTime is an alias to CoreTime, embedd in Time.
type coreTime = CoreTime

// Time is the struct for handling datetime, timestamp and date.
type Time struct {
	coreTime
}

// NewTime constructs time from core time, type and fsp.
func NewTime(coreTime CoreTime, tp uint8, fsp int) Time {
	t := ZeroTime
	p := (*uint64)(&t.coreTime)
	*p |= uint64(coreTime) & coreTimeBitFieldMask
	if tp == mysql.TypeDate {
		*p |= uint64(fspTtForDate)
		return t
	}
	if fsp == UnspecifiedFsp {
		fsp = DefaultFsp
	}
	*p |= uint64(fsp) << 1
	if tp == mysql.TypeTimestamp {
		*p |= 1
	}
	return t
}

// SetFsp updates the fsp in Time.
func (t *Time) SetFsp(fsp int) {
	if t.getFspTt() == fspTtForDate {
		return
	}
	if fsp == UnspecifiedFsp {
		fsp = DefaultFsp
	}
	*(*uint64)(&t.coreTime) &= ^(fspBitFieldMask)
	*(*uint64)(&t.coreTime) |= uint64(fsp) << 1
}

func (t Time) getFspTt() uint {
	return uint(uint64(t.coreTime) & fspTtBitFieldMask)
}

// Type returns type value.
func (t Time) Type() uint8 {
	if t.getFspTt() == fspTtForDate {
		return mysql.TypeDate
	}
	if uint64(t.coreTime)&1 == 1 {
		return mysql.TypeTimestamp
	}
	return mysql.TypeDatetime
}

// CoreTime returns core time.
func (t Time) CoreTime() CoreTime {
	return CoreTime(uint64(t.coreTime) & coreTimeBitFieldMask)
}

// Duration is the type for MySQL TIME type.
type Duration struct {
	gotime.Duration
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

// Hour returns current hour.
// e.g, hour("11:11:11") -> 11
func (d Duration) Hour() int {
	_, hour, _, _, _ := splitDuration(d.Duration)
	return hour
}

func splitDuration(t gotime.Duration) (sign int, hours int, minutes int, seconds int, fraction int) {
	sign = 1
	if t < 0 {
		t = -t
		sign = -1
	}

	hoursDuration := t / gotime.Hour
	t -= hoursDuration * gotime.Hour //nolint:durationcheck
	minutesDuration := t / gotime.Minute
	t -= minutesDuration * gotime.Minute //nolint:durationcheck
	secondsDuration := t / gotime.Second
	t -= secondsDuration * gotime.Second //nolint:durationcheck
	fractionDuration := t / gotime.Microsecond

	return sign, int(hoursDuration), int(minutesDuration), int(secondsDuration), int(fractionDuration)
}
