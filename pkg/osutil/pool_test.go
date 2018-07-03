// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package osutil

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPool{})

type testPool struct{}

func (t *testPool) TestWorkerPoolMaxWorkersCount(c *C) {
	p := NewPool(10)
	p.Start()

	ready := make(chan struct{})
	taskCh := make(chan struct{}, p.MaxWorkersCount)
	task := func() {
		<-ready
		taskCh <- struct{}{}
	}
	for i := 0; i < p.MaxWorkersCount; i++ {
		c.Assert(p.Schedule(Task(task)), IsNil)
	}

	c.Assert(p.Schedule(task), NotNil)
	close(ready)
	for i := 0; i < p.MaxWorkersCount; i++ {
		select {
		case <-taskCh:
		case <-time.After(time.Second):
			c.Fatal("timeout")
		}
	}
	c.Assert(p.Schedule(task), IsNil)
	p.Stop()
}

func (t *testPool) TestWorkerPoolMaxIdleDuration(c *C) {
	p := NewPool(10)
	p.SetMaxIdleWorkerDuration(100 * time.Millisecond)
	p.Start()
	slowChan := make(chan struct{})
	quikeTask := func() {}
	slowTask := func() { <-slowChan }
	c.Assert(p.Schedule(quikeTask), IsNil)
	for i := 0; i < 3; i++ {
		c.Assert(p.Schedule(slowTask), IsNil)
	}
	c.Assert(p.WorkersCount(), Equals, 4)
	time.Sleep(time.Second)
	c.Assert(p.WorkersCount(), Equals, 3)
	close(slowChan)
	time.Sleep(time.Second)
	c.Assert(p.WorkersCount(), Equals, 0)
	p.Stop()
}
