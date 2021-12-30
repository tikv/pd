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
	// compoenent is the context key for the request compoenent.
	sourceInfoKey key = iota

	// serviceLabel is the context key for the request user.
	serviceLabelKey
)

// WithSourceInfo returns a copy of parent in which the user value is set
func WithSourceInfo(parent context.Context, sourceInfo SourceInfo) context.Context {
	return context.WithValue(parent, sourceInfoKey, sourceInfo)
}

// SourceInfoFrom returns the value of the source info key on the ctx
func SourceInfoFrom(ctx context.Context) (SourceInfo, bool) {
	sourceInfo, ok := ctx.Value(sourceInfoKey).(SourceInfo)
	return sourceInfo, ok
}

// WithServiceLabel returns a copy of parent in which the service label value is set
func WithServiceLabel(parent context.Context, serviceLabel string) context.Context {
	return context.WithValue(parent, serviceLabelKey, serviceLabel)
}

// ServiceLabelFrom returns the value of the service label key on the ctx
func ServiceLabelFrom(ctx context.Context) (string, bool) {
	serviceLabel, ok := ctx.Value(serviceLabelKey).(string)
	return serviceLabel, ok
}
