// Copyright 2022 TiKV Project Authors.
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
	"reflect"
)

// Codec is the interface representing objects that can marshal and unmarshal themselves.
type Codec interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}

// DeepClone returns the deep copy of the source
func DeepClone[T Codec](src T, factory func() T) T {
	if reflect.ValueOf(src).IsNil() {
		var dst T
		return dst
	}
	b, err := src.Marshal()
	if err != nil {
		var dst T
		return dst
	}
	dst := factory()
	_ = dst.Unmarshal(b)
	return dst
}
