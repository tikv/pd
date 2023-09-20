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
