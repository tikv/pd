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

package syncutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFlexibleWaitGroup(t *testing.T) {
	re := require.New(t)
	fwg := NewFlexibleWaitGroup()
	for i := 20; i >= 0; i-- {
		fwg.Add(1)
		go func(i int) {
			defer fwg.Done()
			time.Sleep(time.Millisecond * time.Duration(i*50))
		}(i)
	}
	now := time.Now()
	fwg.Wait()
	re.GreaterOrEqual(time.Since(now).Milliseconds(), int64(1000))
}

// TestAddAfterWait tests the case where Add is called after Wait has started and before Wait has finished.
func TestAddAfterWait(t *testing.T) {
	fwg := NewFlexibleWaitGroup()
	startWait := make(chan struct{})
	addTwice := make(chan struct{})
	done := make(chan struct{})

	// First goroutine: Adds a task, then waits for the second task to be added before finishing.
	go func() {
		defer fwg.Done()
		fwg.Add(1)
		<-addTwice
	}()

	// Second goroutine:  adds a second task after ensure the third goroutine has started to wait
	// and triggers the first goroutine to finish.
	go func() {
		defer fwg.Done()
		<-startWait
		fwg.Add(1)
		addTwice <- struct{}{}
	}()

	// Third goroutine: waits for all tasks to be added, then finishes.
	go func() {
		startWait <- struct{}{}
		fwg.Wait()
		done <- struct{}{}
	}()
	<-done
}

// TestNegativeDelta tests the case where Add is called with a negative delta.
func TestNegativeDelta(t *testing.T) {
	require := require.New(t)
	fwg := NewFlexibleWaitGroup()
	fwg.Add(5)
	go func() {
		fwg.Add(-3)
		fwg.Done()
		fwg.Done()
	}()
	go func() {
		fwg.Add(-2)
		fwg.Done()
	}()
	fwg.Wait()
	require.Equal(0, fwg.count)
}

// TestMultipleWait tests the case where Wait is called multiple times concurrently.
func TestMultipleWait(t *testing.T) {
	require := require.New(t)
	fwg := NewFlexibleWaitGroup()
	fwg.Add(3)
	done := make(chan struct{})
	go func() {
		fwg.Wait()
		done <- struct{}{}
	}()
	go func() {
		fwg.Wait()
		done <- struct{}{}
	}()
	go func() {
		fwg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure that Done is called after the Waits
		fwg.Done()
		fwg.Done()
	}()
	<-done
	<-done
	require.Equal(0, fwg.count)
}

// TestAddAfterWaitFinished tests the case where Add is called after Wait has finished.
func TestAddAfterWaitFinished(t *testing.T) {
	require := require.New(t)
	fwg := NewFlexibleWaitGroup()
	done := make(chan struct{})
	go func() {
		fwg.Add(1)
		fwg.Done()
	}()
	go func() {
		fwg.Wait()
		done <- struct{}{}
	}()
	<-done
	fwg.Add(1)
	require.Equal(1, fwg.count)
	fwg.Done()
	require.Equal(0, fwg.count)
}
