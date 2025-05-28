// Copyright 2025 TiKV Project Authors.
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
	"container/list"
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	// maxSpeedCalculationWindow is the maximum size of the time window used to calculate the speed,
	// but it does not mean that all data in it will be used to calculate the speed,
	// which data is used depends on the patrol region duration
	maxSpeedCalculationWindow = 2 * time.Hour
	// minSpeedCalculationWindow is the minimum speed calculation window
	minSpeedCalculationWindow = 10 * time.Minute
)

// progressIndicator reflects a specified progress.
type progressIndicator struct {
	*Progress

	targetRegionSize float64
	// We use a fixed interval's history to calculate the latest average speed.
	history *list.List
	// We use (maxSpeedCalculationWindow / updateInterval + 1) to get the windowCapacity.
	// Assume that the windowCapacity is 4, the init value is 1. After update 3 times with 2, 3, 4 separately. The window will become [1, 2, 3, 4].
	// Then we update it again with 5, the window will become [2, 3, 4, 5].
	windowCapacity int
	// windowLength is used to determine what data will be computed.
	// Assume that the windowLength is 2, the init value is 1. The value that will be calculated are [1].
	// After update 3 times with 2, 3, 4 separately. The value that will be calculated are [3,4] and the values in queue are [(1,2),3,4].
	// It helps us avoid calculation results jumping change when patrol-region-interval changes.
	windowLength int
	// front is the first element which should be used.
	// currentWindowLength indicates where the front is currently in the queue.
	// Assume that the windowLength is 2, the init value is 1. The front is [1] and currentWindowLength is 1.
	// After update 3 times with 2, 3, 4 separately.
	// The front is [3], the currentWindowLength is 2, and values in queue are [(1,2),3,4]
	//                                                                                ^ front
	//                                                                                - - currentWindowLength = len([3,4]) = 2
	// We will always keep the currentWindowLength equal to windowLength if the actual size is enough.
	front               *list.Element
	currentWindowLength int

	updateInterval time.Duration
	completeAt     time.Time
}

func newProgressIndicator(
	action Action,
	current, total float64,
	updateInterval time.Duration,
) *progressIndicator {
	history := list.New()
	history.PushBack(current)
	if minSpeedCalculationWindow < updateInterval {
		log.Warn("updateInterval is too large, it should be smaller than minSpeedCalculationWindow",
			zap.Duration("updateInterval", updateInterval))
		updateInterval = minSpeedCalculationWindow
	}
	pi := &progressIndicator{
		Progress: &Progress{
			Action:          action,
			ProgressPercent: current / total,
			LeftSecond:      math.MaxFloat64,
			CurrentSpeed:    0,
		},
		front:               history.Front(),
		targetRegionSize:    total,
		history:             history,
		windowCapacity:      int(maxSpeedCalculationWindow/updateInterval) + 1,
		windowLength:        int(minSpeedCalculationWindow / updateInterval),
		updateInterval:      updateInterval,
		currentWindowLength: 1,
	}

	return pi
}

func (p *progressIndicator) adjustWindowLength(dur time.Duration) {
	if dur < minSpeedCalculationWindow {
		log.Warn("The window duration is too small, to ensure enough data to calculate progress, "+
			"it will be reset to minSpeedCalculationWindow(10m). ", zap.Duration("duration", dur))
		dur = minSpeedCalculationWindow
	} else if dur > maxSpeedCalculationWindow {
		log.Warn("The window duration is too large, it will be reset to maxSpeedCalculationWindow(2h). ",
			zap.Duration("duration", dur))
		dur = maxSpeedCalculationWindow
	}
	p.windowLength = int(dur/p.updateInterval) + 1
}

func (p *progressIndicator) updateProgress() {
	if p.targetRegionSize == 0 {
		p.ProgressPercent = 1
		p.LeftSecond = 0
		p.CurrentSpeed = 0
		p.completeAt = time.Now()
		return
	}

	currentRegionSize := p.history.Back().Value.(float64)
	// It means it just init and we haven't update the progress
	if p.history.Len() <= 1 {
		p.CurrentSpeed = 0
	} else {
		// the value increases, e.g., [1, 2, 3]
		p.CurrentSpeed = (currentRegionSize - p.front.Value.(float64)) /
			(float64(p.currentWindowLength-1) * p.updateInterval.Seconds())
	}
	if p.CurrentSpeed < 0 {
		p.CurrentSpeed = 0
	}

	if currentRegionSize >= p.targetRegionSize {
		// It means the progress is finished.
		currentRegionSize = p.targetRegionSize
		p.completeAt = time.Now()
	}

	p.ProgressPercent = currentRegionSize / p.targetRegionSize
	p.LeftSecond = (p.targetRegionSize - currentRegionSize) / p.CurrentSpeed
	if math.IsNaN(p.LeftSecond) || math.IsInf(p.LeftSecond, 0) {
		p.LeftSecond = math.MaxFloat64
	}
}

func (p *progressIndicator) push(data float64) {
	p.history.PushBack(data)
	p.currentWindowLength++
	p.moveWindow()

	for p.history.Len() > p.windowCapacity {
		p.history.Remove(p.history.Front())
	}

	p.updateProgress()
}

func (p *progressIndicator) moveWindow() {
	// try to move `front` into correct place.
	for p.currentWindowLength > p.windowLength {
		p.front = p.front.Next()
		p.currentWindowLength--
	}
	for p.currentWindowLength < p.windowLength && p.front.Prev() != nil {
		p.front = p.front.Prev()
		p.currentWindowLength++
	}
}
