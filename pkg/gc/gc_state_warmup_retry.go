// Copyright 2026 TiKV Project Authors.
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

package gc

import "time"

type gcStateWarmupRetry struct {
	id      uint32
	retryAt time.Time
}

// gcStateWarmupRetries orders pending retries by their next eligible time.
// A task leaves the heap before its scope is queued again, so a scope has at
// most one task. Snapshot reconciliation filters obsolete tasks eagerly.
type gcStateWarmupRetries []gcStateWarmupRetry

func (h *gcStateWarmupRetries) Len() int { return len(*h) }

func (h *gcStateWarmupRetries) Less(i, j int) bool { return (*h)[i].retryAt.Before((*h)[j].retryAt) }

func (h *gcStateWarmupRetries) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// Push appends a retry for container/heap to order.
func (h *gcStateWarmupRetries) Push(value any) { *h = append(*h, value.(gcStateWarmupRetry)) }

// Pop removes the last retry after container/heap moves the minimum there.
func (h *gcStateWarmupRetries) Pop() any {
	last := len(*h) - 1
	retry := (*h)[last]
	(*h)[last] = gcStateWarmupRetry{}
	*h = (*h)[:last]
	return retry
}
