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

package requestutil

import (
	"context"
)

type key int

const (
	// requestInfoKey is the context key for the request compoenent.
	requestInfoKey key = iota
)

// WithRequestInfo returns a copy of parent in which the user value is set
func WithRequestInfo(parent context.Context, requestInfo RequestInfo) context.Context {
	return context.WithValue(parent, requestInfoKey, requestInfo)
}

// RequestInfoFrom returns the value of the request info key on the ctx
func RequestInfoFrom(ctx context.Context) (RequestInfo, bool) {
	requestInfo, ok := ctx.Value(requestInfoKey).(RequestInfo)
	return requestInfo, ok
}
