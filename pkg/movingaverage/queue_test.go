// Copyright 2021 TiKV Project Authors.
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

package movingaverage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	sq := NewSafeQueue()
	sq.PushBack(1)
	sq.PushBack(2)
	v1 := sq.PopFront()
	v2 := sq.PopFront()
	re.Equal(1, v1.(int))
	re.Equal(2, v2.(int))
}

func TestClone(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	s1 := NewSafeQueue()
	s1.PushBack(1)
	s1.PushBack(2)
	s2 := s1.Clone()
	s2.PopFront()
	s2.PopFront()
	re.Equal(2, s1.que.Len())
	re.Equal(0, s2.que.Len())
}
