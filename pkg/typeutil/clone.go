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

// Codec is the interface representing objects that can marshal and unmarshal themselves.
type Codec interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}

// DeepClone returns the deep copy of the source
// notice: src and dst should be not nil.
func DeepClone[T Codec](src, dst T) {
	b, err := src.Marshal()
	if err == nil {
		dst.Unmarshal(b)
	}
}
