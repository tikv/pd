// Copyright 2025 TiKV Project Authors.
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

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestCheckDiff(t *testing.T) {
	a := `# pkg/storage TestTimestampTxn
# pkg/unsaferecovery TestFailed
# pkg/window TestWindowResetBucket
# pkg/window TestWindowResetBuckets
# pkg/window TestWindowResetWindow
# pkg/window TestWindowSize/ms//test_service/registry/ get
/gta/timestamp get
/ms//test_service/registry/ get
/ms//tso//gta get
/ms//tso//gta/timestamp get
/ms//tso//primary get
/ms//tso//primary/expected_primary get
/ms//tso/keyspace_groups/election//primary get
/ms//tso/keyspace_groups/election//primary/expected_primary get
/ms//tso/registry/ get`
	b := `# pkg/window TestWindowAppend
# pkg/window TestWindowResetBucket
# pkg/window TestWindowResetBuckets
# pkg/window TestWindowResetWindow
# pkg/window TestWindowSize/pd//timestamp get
/gta/timestamp get
/ms//test_service/registry/ get`

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(a),
		B:        difflib.SplitLines(b),
		FromFile: "a",
		ToFile:   "b",
		Context:  3,
	}
	diffText, _ := difflib.GetUnifiedDiffString(diff)

	require.Equal(t, `--- a
+++ b
@@ -1,15 +1,7 @@
-# pkg/storage TestTimestampTxn
-# pkg/unsaferecovery TestFailed
+# pkg/window TestWindowAppend
 # pkg/window TestWindowResetBucket
 # pkg/window TestWindowResetBuckets
 # pkg/window TestWindowResetWindow
-# pkg/window TestWindowSize/ms//test_service/registry/ get
+# pkg/window TestWindowSize/pd//timestamp get
 /gta/timestamp get
 /ms//test_service/registry/ get
-/ms//tso//gta get
-/ms//tso//gta/timestamp get
-/ms//tso//primary get
-/ms//tso//primary/expected_primary get
-/ms//tso/keyspace_groups/election//primary get
-/ms//tso/keyspace_groups/election//primary/expected_primary get
-/ms//tso/registry/ get
`, diffText)

	diff = difflib.UnifiedDiff{
		A:        difflib.SplitLines("aaaaa"),
		B:        difflib.SplitLines("aaaaa"),
		FromFile: "a",
		ToFile:   "b",
		Context:  3,
	}
	diffText, _ = difflib.GetUnifiedDiffString(diff)
	require.Equal(t, "", diffText)
}
