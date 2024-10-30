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

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBalancerPutAndDelete(t *testing.T) {
	re := require.New(t)
	balancers := []Balancer[uint32]{
		NewRoundRobin[uint32](),
	}
	for _, balancer := range balancers {
		re.Equal(uint32(0), balancer.Next())
		// test put
		exists := make(map[uint32]struct{})
		for range 100 {
			num := rand.Uint32()
			balancer.Put(num)
			exists[num] = struct{}{}
			re.Equal(len(balancer.GetAll()), len(exists))
			t := balancer.Next()
			re.Contains(exists, t)
		}
		// test delete
		for num := range exists {
			balancer.Delete(num)
			delete(exists, num)
			re.Equal(len(balancer.GetAll()), len(exists))
			if len(exists) == 0 {
				break
			}
			t := balancer.Next()
			re.NotEqual(t, num)
			re.Contains(exists, t)
		}
		re.Equal(uint32(0), balancer.Next())
	}
}

func TestBalancerDuplicate(t *testing.T) {
	re := require.New(t)
	balancers := []Balancer[uint32]{
		NewRoundRobin[uint32](),
	}
	for _, balancer := range balancers {
		re.Empty(balancer.GetAll())
		// test duplicate put
		balancer.Put(1)
		re.Len(balancer.GetAll(), 1)
		balancer.Put(1)
		re.Len(balancer.GetAll(), 1)
		// test duplicate delete
		balancer.Delete(1)
		re.Empty(balancer.GetAll())
		balancer.Delete(1)
		re.Empty(balancer.GetAll())
	}
}

func TestRoundRobin(t *testing.T) {
	re := require.New(t)
	balancer := NewRoundRobin[uint32]()
	for range 100 {
		num := rand.Uint32()
		balancer.Put(num)
	}
	statistics := make(map[uint32]int)
	for range 1000 {
		statistics[balancer.Next()]++
	}
	min := 1000
	max := 0
	for _, v := range statistics {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	re.LessOrEqual(max-min, 10)
}
