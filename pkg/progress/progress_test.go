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

package progress

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProgress(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	n := "test"
	m := NewManager()
	re.False(m.AddProgress(n, 100, 100, 10*time.Second))
	p, ls, cs, err := m.Status(n)
	re.NoError(err)
	re.Equal(0.0, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)
	time.Sleep(time.Second)
	re.True(m.AddProgress(n, 100, 100, 10*time.Second))

	m.UpdateProgress(n, 30, 30, false)
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.7, p)
	// 30/(70/1s+) > 30/70
	re.Greater(ls, 30.0/70.0)
	// 70/1s+ > 70
	re.Less(cs, 70.0)
	// there is no scheduling
	for i := 0; i < 100; i++ {
		m.UpdateProgress(n, 30, 30, false)
	}
	re.Equal(61, m.progresses[n].history.Len())
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.7, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)

	ps := m.GetProgresses(func(p string) bool {
		return strings.Contains(p, n)
	})
	re.Len(ps, 1)
	re.Equal(n, ps[0])
	ps = m.GetProgresses(func(p string) bool {
		return strings.Contains(p, "a")
	})
	re.Empty(ps)
	re.True(m.RemoveProgress(n))
	re.False(m.RemoveProgress(n))
}

func TestAbnormal(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	n := "test"
	m := NewManager()
	re.False(m.AddProgress(n, 100, 100, 10*time.Second))
	p, ls, cs, err := m.Status(n)
	re.NoError(err)
	re.Equal(0.0, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)
	// When offline a store, but there are still many write operations
	m.UpdateProgress(n, 110, 110, false)
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.0, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)
	// It usually won't happens
	m.UpdateProgressTotal(n, 10)
	p, ls, cs, err = m.Status(n)
	re.Error(err)
	re.Equal(0.0, p)
	re.Equal(0.0, ls)
	re.Equal(0.0, cs)
}
