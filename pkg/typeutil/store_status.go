// Copyright 2017 PingCAP, Inc.
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

package typeutil

import (
	"strconv"

	"github.com/juju/errors"
)

// StoreStatu is a Custome type for describe store status
type StoreStatu bool

// String format the storestatu to a string
func (b *StoreStatu) String() string {
	if *b == true {
		return "Busy"
	}
	return "Free"
}

// MarshalJSON returns the duration as a JSON string.
func (b StoreStatu) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (b *StoreStatu) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	if s == "Busy" {
		*b = true
		return nil
	}
	if s == "Free" {
		*b = false
		return nil
	}
	return errors.New("Unknown type")
}
