// Copyright 2022 TiKV Project Authors.
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

package requestutil

import (
	"context"
	"time"
)

// The key type is unexported to prevent collisions
type key int

const (
	// requestInfoKey is the context key for the request info.
	requestInfoKey key = iota
	// endTimeKey stores the request end time as Unix seconds (int64), retained
	// for compatibility with callers that calculate duration from timestamps.
	endTimeKey
	// requestDurationKey stores elapsed time (time.Duration) with subsecond
	// precision. Metrics prefer it over subtracting whole-second timestamps.
	requestDurationKey
)

// WithRequestInfo returns a copy of parent in which the request info value is set
func WithRequestInfo(parent context.Context, requestInfo RequestInfo) context.Context {
	return context.WithValue(parent, requestInfoKey, requestInfo)
}

// RequestInfoFrom returns the value of the request info key on the ctx
func RequestInfoFrom(ctx context.Context) (RequestInfo, bool) {
	requestInfo, ok := ctx.Value(requestInfoKey).(RequestInfo)
	return requestInfo, ok
}

// WithEndTime returns a copy of parent in which the end time value is set
func WithEndTime(parent context.Context, endTime int64) context.Context {
	return context.WithValue(parent, endTimeKey, endTime)
}

// EndTimeFrom returns the value of the execution info key on the ctx
func EndTimeFrom(ctx context.Context) (int64, bool) {
	info, ok := ctx.Value(endTimeKey).(int64)
	return info, ok
}

// WithRequestDuration returns a copy of parent with the elapsed request duration.
func WithRequestDuration(parent context.Context, duration time.Duration) context.Context {
	return context.WithValue(parent, requestDurationKey, duration)
}

// RequestDurationFrom returns the elapsed request duration from ctx.
func RequestDurationFrom(ctx context.Context) (time.Duration, bool) {
	duration, ok := ctx.Value(requestDurationKey).(time.Duration)
	return duration, ok
}
