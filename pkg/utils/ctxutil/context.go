// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctxutil

// CtxKey is a custom type used as a key for values stored in Context.
type CtxKey string

const (
	// HeartbeatTracerKey is the key for the heartbeat tracer in the context.
	HeartbeatTracerKey CtxKey = "h_tracer"
	// TaskRunnerKey is the key for the task runner in the context.
	TaskRunnerKey CtxKey = "task_runner"
	// LimiterKey is the key for the concurrency limiter in the context.
	LimiterKey CtxKey = "limiter"
)
