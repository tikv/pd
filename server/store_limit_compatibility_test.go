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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckDefaultStoreLimitPersistenceFeature(t *testing.T) {
	re := require.New(t)
	re.NoError(checkDefaultStoreLimitPersistenceFeature("PD", "pd-1", "new-build", "new-build"))
	re.ErrorContains(
		checkDefaultStoreLimitPersistenceFeature("PD", "pd-1", "new-build", ""),
		"PD member pd-1 does not support persisted default store limits")
	re.ErrorContains(
		checkDefaultStoreLimitPersistenceFeature("Scheduling Service", "scheduling-1", "old-build", "new-build"),
		"Scheduling Service member scheduling-1 does not support persisted default store limits")
}
