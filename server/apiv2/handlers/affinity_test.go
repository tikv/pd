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

package handlers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDiagnosticGroupIDsQueryLimit(t *testing.T) {
	re := require.New(t)
	rawIDs := make([]string, 585)
	for i := range rawIDs {
		rawIDs[i] = "g0"
	}
	rawQuery := strings.Repeat("ids=g0&", len(rawIDs)) + "include_status=true"
	re.Greater(len(rawQuery), 4096)
	_, err := parseDiagnosticGroupIDs(rawIDs, len(rawQuery))
	re.EqualError(err, "diagnostic query exceeds 100 ids or 4096 bytes")

	ids, err := parseDiagnosticGroupIDs([]string{"g0"}, 4096)
	re.NoError(err)
	re.Equal([]string{"g0"}, ids)
}
