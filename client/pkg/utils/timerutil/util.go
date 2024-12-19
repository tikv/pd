// Copyright 2024 TiKV Project Authors.
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

package timerutil

import "time"

// SafeResetTimer is used to reset timer safely.
// Before Go 1.23, the only safe way to use Reset was to call Timer.Stop and explicitly drain the timer first.
// We need be careful here, see more details in the comments of Timer.Reset.
// https://pkg.go.dev/time@master#Timer.Reset
func SafeResetTimer(t *time.Timer, d time.Duration) {
	// Stop the timer if it's not stopped.
	if !t.Stop() {
		select {
		case <-t.C: // try to drain from the channel
		default:
		}
	}
	t.Reset(d)
}
