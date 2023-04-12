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

// MovingAvg provides moving average.
// Ref: https://en.wikipedia.org/wiki/Moving_average
type MovingAvg interface {
	// Add adds a data point to the data set.
	Add(data float64)
	// Get returns the moving average.
	Get() float64
	// GetInstantaneous returns the value just added.
	GetInstantaneous() float64
	// Reset cleans the data set.
	Reset()
	// Set = Reset + Add
	Set(data float64)
}
