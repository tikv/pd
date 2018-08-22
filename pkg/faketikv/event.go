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

package faketikv

import (
	"github.com/pingcap/pd/pkg/faketikv/cases"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
)

// Event that affect the status of the cluster
type Event interface {
	Run(driver *Driver) bool
}

// EventRunner includes all events
type EventRunner struct {
	events []Event
}

// NewEventRunner news a event runner
func NewEventRunner(events []cases.EventInner) *EventRunner {
	er := &EventRunner{events: make([]Event, 0, len(events))}
	for _, e := range events {
		event := parserEvent(e)
		if event != nil {
			er.events = append(er.events, event)
		}
	}
	return er
}

func parserEvent(e cases.EventInner) Event {
	switch v := e.(type) {
	case *cases.WriteFlowOnSpotInner:
		return &WriteFlowOnSpot{in: v}
	case *cases.WriteFlowOnRegionInner:
		return &WriteFlowOnRegion{in: v}
	case *cases.ReadFlowOnRegionInner:
		return &ReadFlowOnRegion{in: v}
	case *cases.AddNodesDynamicInner:
		return &AddNodesDynamic{in: v}
	case *cases.DeleteNodesInner:
		return &DeleteNodes{in: v}
	}
	return nil
}

// Tick ticks the event run
func (er *EventRunner) Tick(driver *Driver) {
	var finishedIndex int
	for i, e := range er.events {
		isFinished := e.Run(driver)
		if isFinished {
			er.events[i], er.events[finishedIndex] = er.events[finishedIndex], er.events[i]
			finishedIndex++
		}
	}
	er.events = er.events[finishedIndex:]
}

// WriteFlowOnSpot writes bytes in some range
type WriteFlowOnSpot struct {
	in *cases.WriteFlowOnSpotInner
}

// Run implements the event interface
func (w *WriteFlowOnSpot) Run(driver *Driver) bool {
	raft := driver.raftEngine
	res := w.in.Step(driver.tickCount)
	for key, size := range res {
		region := raft.SearchRegion([]byte(key))
		if region == nil {
			simutil.Logger.Errorf("region not found for key %s", key)
			continue
		}
		raft.updateRegionStore(region, size)
	}
	return false
}

// WriteFlowOnRegion writes bytes in some region
type WriteFlowOnRegion struct {
	in *cases.WriteFlowOnRegionInner
}

// Run implements the event interface
func (w *WriteFlowOnRegion) Run(driver *Driver) bool {
	raft := driver.raftEngine
	res := w.in.Step(driver.tickCount)
	for id, bytes := range res {
		region := raft.GetRegion(id)
		if region == nil {
			simutil.Logger.Errorf("region %d not found", id)
			continue
		}
		raft.updateRegionStore(region, bytes)
	}
	return false
}

// ReadFlowOnRegion reads bytes in some region
type ReadFlowOnRegion struct {
	in *cases.ReadFlowOnRegionInner
}

// Run implements the event interface
func (w *ReadFlowOnRegion) Run(driver *Driver) bool {
	res := w.in.Step(driver.tickCount)
	driver.raftEngine.updateRegionReadBytes(res)
	return false
}

// AddNodesDynamic adds nodes dynamically.
type AddNodesDynamic struct {
	in *cases.AddNodesDynamicInner
}

// Run implements the event interface.
func (w *AddNodesDynamic) Run(driver *Driver) bool {
	res := w.in.Step(driver.tickCount)
	if res == 0 {
		return false
	}
	driver.AddNode(res)
	return false
}

// DeleteNodes deletes nodes randomly
type DeleteNodes struct {
	in *cases.DeleteNodesInner
}

// Run implements the event interface
func (w *DeleteNodes) Run(driver *Driver) bool {
	res := w.in.Step(driver.tickCount)
	if res == 0 {
		return false
	}
	driver.DeleteNode(res)
	return false
}
