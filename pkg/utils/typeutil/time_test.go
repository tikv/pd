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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	re := require.New(t)
	for range 3 {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := Uint64ToBytes(uint64(t.UnixNano()))
		nt, err := ParseTimestamp(data)
		re.NoError(err)
		re.True(nt.Equal(t))
	}
	data := []byte("pd")
	nt, err := ParseTimestamp(data)
	re.Error(err)
	re.True(nt.Equal(ZeroTime))
}

func TestSubTimeByWallClock(t *testing.T) {
	re := require.New(t)
	for range 100 {
		r := rand.Int63n(1000)
		t1 := time.Now()
		// Add r seconds.
		t2 := t1.Add(time.Second * time.Duration(r))
		duration := SubRealTimeByWallClock(t2, t1)
		re.Equal(time.Second*time.Duration(r), duration)
		milliseconds := SubTSOPhysicalByWallClock(t2, t1)
		re.Equal(r*time.Second.Milliseconds(), milliseconds)
		// Add r milliseconds.
		t3 := t1.Add(time.Millisecond * time.Duration(r))
		milliseconds = SubTSOPhysicalByWallClock(t3, t1)
		re.Equal(r, milliseconds)
		// Add r nanoseconds.
		t4 := t1.Add(time.Duration(-r))
		duration = SubRealTimeByWallClock(t4, t1)
		re.Equal(time.Duration(-r), duration)
		// For the millisecond comparison, please see TestSmallTimeDifference.
	}
}

func TestSmallTimeDifference(t *testing.T) {
	re := require.New(t)
	t1, err := time.Parse("2006-01-02 15:04:05.999", "2021-04-26 00:44:25.682")
	re.NoError(err)
	t2, err := time.Parse("2006-01-02 15:04:05.999", "2021-04-26 00:44:25.681918")
	re.NoError(err)
	duration := SubRealTimeByWallClock(t1, t2)
	re.Equal(time.Duration(82)*time.Microsecond, duration)
	duration = SubRealTimeByWallClock(t2, t1)
	re.Equal(time.Duration(-82)*time.Microsecond, duration)
	milliseconds := SubTSOPhysicalByWallClock(t1, t2)
	re.Equal(int64(1), milliseconds)
	milliseconds = SubTSOPhysicalByWallClock(t2, t1)
	re.Equal(int64(-1), milliseconds)
}
