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
	re.Less(math.Abs(ls-30.0/7.0), 1e-6)
	re.Less(math.Abs(cs-7), 1e-6)
	// there is no scheduling
	for range 1000 {
		m.UpdateProgress(n, 30, 30, false)
	}
	re.Equal(721, m.progresses[n].history.Len())
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

func TestProgressWithDynamicWindow(t *testing.T) {
	// The full capacity of queue is 721.
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

	m.UpdateProgress(n, 31, 31, false)
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.69, p)
	re.Less(math.Abs(ls-31.0/6.9), 1e-6)
	re.Less(math.Abs(cs-6.9), 1e-6)
	re.Equal(2, m.progresses[n].currentWindowLength)
	re.Equal(100.0, m.progresses[n].front.Value.(float64))

	m.UpdateProgress(n, 30, 30, false, WindowDurationOption(time.Minute*20))
	re.Equal(3, m.progresses[n].currentWindowLength)
	re.Equal(100.0, m.progresses[n].front.Value.(float64))
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.7, p)
	re.Less(math.Abs(ls-30.0/(7.0/2)), 1e-6)
	re.Less(math.Abs(cs-3.5), 1e-6)

	for range 1000 {
		m.UpdateProgress(n, 30, 30, false)
	}
	re.Equal(721, m.progresses[n].history.Len())
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.7, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)
	m.UpdateProgress(n, 29, 29, false, WindowDurationOption(time.Minute*20))
	re.Equal(121, m.progresses[n].currentWindowLength)
	re.Equal(30.0, m.progresses[n].front.Value.(float64))
	re.Equal(721, m.progresses[n].history.Len())

	for range 60 {
		m.UpdateProgress(n, 28, 28, false)
	}
	re.Equal(721, m.progresses[n].history.Len())
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.72, p)
	re.Equal(float64(28/(2./120)*10.), ls)
	re.Equal(float64(2./120/10.), cs)

	m.UpdateProgress(n, 28, 28, false, WindowDurationOption(time.Minute*10))
	re.Equal(721, m.progresses[n].history.Len())
	re.Equal(61, m.progresses[n].currentWindowLength)
	re.Equal(28.0, m.progresses[n].front.Value.(float64))
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.72, p)
	re.Equal(math.MaxFloat64, ls)
	re.Equal(0.0, cs)

	m.UpdateProgress(n, 28, 28, false, WindowDurationOption(time.Minute*20))
	re.Equal(121, m.progresses[n].currentWindowLength)
	re.Equal(30.0, m.progresses[n].front.Value.(float64))
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.72, p)
	re.Equal(float64(28/(2./120)*10.), ls)
	re.Equal(float64(2./120/10.), cs)

	m.UpdateProgress(n, 1, 1, false, WindowDurationOption(time.Minute*12))
	re.Equal(73, m.progresses[n].currentWindowLength)
	re.Equal(30.0, m.progresses[n].front.Value.(float64))
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.99, p)
	re.Equal(float64(1/(29./72)*10.), ls)
	re.Equal(float64(29./72/10.), cs)

	m.UpdateProgress(n, 1, 1, false, WindowDurationOption(time.Minute*5))
	re.Equal(61, m.progresses[n].currentWindowLength)
	re.Equal(28.0, m.progresses[n].front.Value.(float64))
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.99, p)
	re.Equal(float64(1/(27./60)*10.), ls)
	re.Equal(float64(27./60/10.), cs)

	m.UpdateProgress(n, 1, 1, false, WindowDurationOption(time.Minute*180))
	p, ls, cs, err = m.Status(n)
	re.Equal(721, m.progresses[n].currentWindowLength)
	re.Equal(30.0, m.progresses[n].front.Value.(float64))
	re.NoError(err)
	re.Equal(0.99, p)
	re.Equal(float64(1/(29./720)*10.), ls)
	re.Equal(float64(29./720/10.), cs)
	for range 2000 {
		m.UpdateProgress(n, 1, 1, false)
	}
	re.Equal(721, m.progresses[n].history.Len())
	p, ls, cs, err = m.Status(n)
	re.NoError(err)
	re.Equal(0.99, p)
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
