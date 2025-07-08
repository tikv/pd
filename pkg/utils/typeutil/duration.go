// Copyright 2016 TiKV Project Authors.
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

package typeutil

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"time"

	"github.com/pingcap/errors"
)

// Duration is a wrapper of time.Duration for TOML and JSON.
type Duration struct {
	time.Duration
}

// NewDuration creates a Duration from time.Duration.
func NewDuration(duration time.Duration) Duration {
	return Duration{Duration: duration}
}

// MarshalJSON returns the duration as a JSON string.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, d.String()), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return errors.WithStack(err)
	}
	d.Duration = duration
	return nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return errors.WithStack(err)
}

// MarshalText returns the duration as a JSON string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// SaturatingStdDurationFromSeconds returns a time.Duration representing the given seconds, truncated within the range
// [0, math.MaxInt64] to avoid overflowing that may happen on plain multiplication.
func SaturatingStdDurationFromSeconds(seconds int64) time.Duration {
	if seconds < 0 {
		return 0
	}
	h, l := bits.Mul64(uint64(seconds), uint64(time.Second))
	if h != 0 || l > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(l)
}
