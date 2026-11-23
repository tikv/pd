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

// Package protectedchanges holds a temporary probe used to verify the
// protected-changes check end to end.
package protectedchanges

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// SmokeError logs an error over several lines, which is the form the check has
// to recognise as one protected change.
func SmokeError(err error) {
	log.Error("an error log added on several lines",
		zap.Error(err),
	)
}
