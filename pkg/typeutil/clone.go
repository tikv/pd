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

	"github.com/gogo/protobuf/proto"
)

// Codec is the interface representing objects that can marshal and unmarshal themselves.
type Codec interface {
	proto.Marshaler
	proto.Unmarshaler
}

// DeepClone returns the deep copy of the source
func DeepClone[T Codec](src T) T {
	b, _ := src.Marshal()
	t := reflect.ValueOf(&src).Elem().Type()
	dst := reflect.New(t.Elem()).Interface().(T)
	dst.Unmarshal(b)
	return dst
}
