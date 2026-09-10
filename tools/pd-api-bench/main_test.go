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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/tools/pd-api-bench/cases"
)

func TestValidateCaseNames(t *testing.T) {
	registered := map[string]struct{}{"valid": {}}
	require.NoError(t, validateCaseNames(map[string]cases.Config{"valid": {}}, registered, "test"))
	require.EqualError(t,
		validateCaseNames(map[string]cases.Config{"valid": {}, "invalid": {}}, registered, "test"),
		"test case invalid not implemented")
}
