// Copyright 2021 TiKV Project Authors.
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

package checker

import (
	"sync/atomic"
	"time"
)

// PauseController sets and stores delay time in checkers.
type PauseController struct {
	delayUntil int64
}

// IsPaused check if checker is paused
func (c *PauseController) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&c.delayUntil)
	return time.Now().Unix() < delayUntil
}

// PauseOrResume pause or resume the checker
func (c *PauseController) PauseOrResume(t int64) {
	delayUntil := time.Now().Unix() + t
	atomic.StoreInt64(&c.delayUntil, delayUntil)
}
