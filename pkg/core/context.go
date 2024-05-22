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

package core

import (
	"context"

	"github.com/tikv/pd/pkg/ratelimit"
)

// MetaProcessContext is a context for meta process.
type MetaProcessContext struct {
	context.Context
	Tracer     RegionHeartbeatProcessTracer
	TaskRunner ratelimit.Runner
	MiscRunner ratelimit.Runner
	LogRunner  ratelimit.Runner
}

// NewMetaProcessContext creates a new MetaProcessContext.
// used in tests, can be changed if no need to test concurrency.
func ContextTODO() *MetaProcessContext {
	return &MetaProcessContext{
		Context:    context.TODO(),
		Tracer:     NewNoopHeartbeatProcessTracer(),
		TaskRunner: ratelimit.NewSyncRunner(),
		MiscRunner: ratelimit.NewSyncRunner(),
		LogRunner:  ratelimit.NewSyncRunner(),
		// Limit default is nil
	}
}
