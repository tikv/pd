// Copyright 2023 TiKV Project Authors.
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

package balancer

// Policy is the policy of balancer.
type Policy int

const (
	// PolicyRoundRobin is the round robin policy.
	PolicyRoundRobin Policy = iota
	// PolicyLeast is the policy to return the least used node.
	// TODO: move indexed heap to pkg and use it.
	PolicyLeast
)

func (p Policy) String() string {
	switch p {
	case PolicyRoundRobin:
		return "round-robin"
	case PolicyLeast:
		return "least"
	default:
		return "unknown"
	}
}

// Balancer is the interface for balancer.
type Balancer[T uint32 | string] interface {
	// Next returns next one.
	Next() T
	// Put puts one into balancer.
	Put(T)
	// Delete deletes one from balancer.
	Delete(T)
	// GetAll returns all nodes.
	GetAll() []T
	// Len returns the length of nodes.
	Len() int
}

// GenByPolicy generates a balancer by policy.
func GenByPolicy[T uint32 | string](policy Policy) Balancer[T] {
	switch policy {
	case PolicyRoundRobin:
		return NewRoundRobin[T]()
	default: // only round-robin is supported now.
		return NewRoundRobin[T]()
	}
}
