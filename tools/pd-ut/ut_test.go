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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestBuildTestBinaryMultiCleansTempFileOnFailure(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	t.Setenv("PATH", "")
	require.Equal(t, tempDir, os.TempDir())

	_, err := buildTestBinaryMulti(nil)
	require.Error(t, err)

	tempFiles, err := filepath.Glob(filepath.Join(tempDir, "pd_tests*"))
	require.NoError(t, err)
	require.Empty(t, tempFiles)
}

func TestRunCleansCoverTempDirOnFailure(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)

	originalArgs := os.Args
	originalCoverFileTempDir := coverFileTempDir
	originalCoverProfile := coverProfile
	defer func() {
		os.Args = originalArgs
		coverFileTempDir = originalCoverFileTempDir
		coverProfile = originalCoverProfile
	}()

	os.Args = []string{"pd-ut", "--parallel", "invalid", "--coverprofile", filepath.Join(tempDir, "coverage.out")}
	require.Equal(t, 1, run())

	tempFiles, err := filepath.Glob(filepath.Join(tempDir, "cov*"))
	require.NoError(t, err)
	require.Empty(t, tempFiles)
}

func TestCollectCoverProfileFileReturnsError(t *testing.T) {
	originalCoverFileTempDir := coverFileTempDir
	originalCoverProfile := coverProfile
	defer func() {
		coverFileTempDir = originalCoverFileTempDir
		coverProfile = originalCoverProfile
	}()

	coverFileTempDir = filepath.Join(t.TempDir(), "missing")
	coverProfile = filepath.Join(t.TempDir(), "coverage.out")
	require.Error(t, collectCoverProfileFile())
}

func TestRunCommandWithTempDirCleansUp(t *testing.T) {
	for _, exitCode := range []string{"0", "1"} {
		t.Run("exit-code-"+exitCode, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "child-temp-dir")
			//nolint:gosec // The helper must re-execute the current test binary.
			cmd := exec.Command(os.Args[0], "-test.run=^TestRunCommandWithTempDirHelperProcess$", "--", marker)
			cmd.Env = append(os.Environ(),
				"PD_UT_TEMP_DIR_HELPER=1",
				"PD_UT_TEMP_DIR_HELPER_EXIT_CODE="+exitCode,
			)

			err := runCommandWithTempDir(cmd)
			if exitCode == "0" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			content, err := os.ReadFile(marker)
			require.NoError(t, err)
			childTempDir := strings.TrimSpace(string(content))
			require.NotEmpty(t, childTempDir)
			_, err = os.Stat(childTempDir)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestRunCommandWithTempDirHelperProcess(_ *testing.T) {
	if os.Getenv("PD_UT_TEMP_DIR_HELPER") != "1" {
		return
	}

	marker := os.Args[len(os.Args)-1]
	tempDir := os.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "leftover"), []byte("test"), 0600); err != nil {
		os.Exit(2)
	}
	if err := os.WriteFile(marker, []byte(tempDir), 0600); err != nil {
		os.Exit(2)
	}
	if os.Getenv("PD_UT_TEMP_DIR_HELPER_EXIT_CODE") == "1" {
		os.Exit(1)
	}
}

func TestCheckDiff(t *testing.T) {
	re := require.New(t)
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
	diffText, err := difflib.GetUnifiedDiffString(diff)
	re.NoError(err)

	re.Equal(`--- a
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
	diffText, err = difflib.GetUnifiedDiffString(diff)
	re.NoError(err)
	re.Empty(diffText)
}
