// Copyright 2020 TiKV Project Authors.
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

const defaultDecay = 0.075

// EMA works as an exponential moving average filter.
// References: https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average.
type EMA struct {
	// EMA will learn from (2-decay)/decay records. It must be less than 1 and greater than 0.
	decay float64
	// Exponential smoothing puts substantial weight on past observations, so the initial value of demand will have an
	// unreasonably large effect on early forecasts. This problem can be overcome by allowing the process to evolve for
	// a reasonable number of periods (say 10 or more) and using the arithmetic average of the demand during
	// those periods as the initial forecast.
	wakeNum       uint64
	count         uint64
	value         float64
	instantaneous float64
}

// NewEMA returns an EMA.
func NewEMA(decays ...float64) *EMA {
	decay := defaultDecay
	if len(decays) != 0 && decays[0] < 1 && decays[0] > 0 {
		decay = decays[0]
	}
	wakeNum := uint64((2 - decay) / decay)
	return &EMA{
		decay:   decay,
		wakeNum: wakeNum,
	}
}

// Add adds a data point.
func (e *EMA) Add(num float64) {
	e.instantaneous = num
	if e.count < e.wakeNum {
		e.count++
		e.value += num
	} else if e.count == e.wakeNum {
		e.count++
		e.value /= float64(e.wakeNum)
		e.value = (num * e.decay) + (e.value * (1 - e.decay))
	} else {
		e.value = (num * e.decay) + (e.value * (1 - e.decay))
	}
}

// Get returns the result of the data set.If return 0.0, it means the filter can not be wake up.
func (e *EMA) Get() float64 {
	if e.count == 0 {
		return 0
	}
	if e.count <= e.wakeNum {
		return e.value / float64(e.count)
	}
	return e.value
}

// Reset cleans the data set.
func (e *EMA) Reset() {
	e.count = 0
	e.value = 0
}

// Set = Reset + Add.
func (e *EMA) Set(n float64) {
	e.value = n
	e.count = 1
}

// GetInstantaneous returns the value just added.
func (e *EMA) GetInstantaneous() float64 {
	return e.instantaneous
}
