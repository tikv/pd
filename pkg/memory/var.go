// Copyright 2023 TiKV Project Authors.
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

package memory

import (
	"time"

	atomicutil "go.uber.org/atomic"
)

// Process global variables for memory limit.
var (
	ServerMemoryLimitOriginText  = atomicutil.NewString("0")
	ServerMemoryLimit            = atomicutil.NewUint64(0)
	ServerMemoryLimitSessMinSize = atomicutil.NewUint64(128 << 20)

	QueryForceDisk       = atomicutil.NewInt64(0)
	TriggerMemoryLimitGC = atomicutil.NewBool(false)
	MemoryLimitGCLast    = atomicutil.NewTime(time.Time{})
	MemoryLimitGCTotal   = atomicutil.NewInt64(0)
)
