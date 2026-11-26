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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestRootPackageExecution(t *testing.T) {
	sourceDir, err := os.Getwd()
	require.NoError(t, err)
	xprog := filepath.Join(t.TempDir(), "xprog")
	cmd := goCommand("build", "-tags=xprog", "-o", xprog, filepath.Join(sourceDir, "xprog.go"))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	originalWorkDir, originalRepoRoot, originalModulePath, originalCache := workDir, repoRoot, modulePath, cache
	originalParallel, originalBudget := parallel, cpuBudget
	parallel, cpuBudget = 2, 0
	t.Cleanup(func() {
		workDir, repoRoot, modulePath, cache = originalWorkDir, originalRepoRoot, originalModulePath, originalCache
		parallel, cpuBudget = originalParallel, originalBudget
	})
	for _, tc := range []struct {
		name   string
		module string
		pkg    string
	}{
		{"root", "github.com/tikv/pd", "."},
		{"subpackage", "github.com/tikv/pd", "pkg/example"},
		{"integration-root", "github.com/tikv/pd/tests/integrations", "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot = t.TempDir()
			modulePath, cache = tc.module, false
			workDir = filepath.Join(repoRoot, strings.TrimPrefix(tc.module, "github.com/tikv/pd"))
			require.NoError(t, os.MkdirAll(filepath.Join(repoRoot, "bin"), 0700))
			require.NoError(t, os.Symlink(xprog, filepath.Join(repoRoot, "bin", "xprog")))
			require.NoError(t, os.MkdirAll(filepath.Join(workDir, tc.pkg), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(workDir, "go.mod"), []byte("module "+tc.module+"\n\ngo 1.20\n"), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(workDir, tc.pkg, "fixture_test.go"), []byte(`package fixture
import ("os"; "testing")
func TestRoot(t *testing.T) {
	if os.Getenv("PD_UT_ROOT_FAIL") == "1" { t.Fatal("fixture failure") }
	if err := os.WriteFile(os.Getenv("PD_UT_ROOT_MARKER"), []byte("executed"), 0600); err != nil { t.Fatal(err) }
}
`), 0600))
			marker := filepath.Join(t.TempDir(), "executed")
			t.Setenv("PD_UT_ROOT_MARKER", marker)
			t.Setenv("PD_UT_ROOT_FAIL", "")
			tasks, err := runExistingTestCases([]string{tc.pkg})
			require.NoError(t, err)
			require.Equal(t, []task{{tc.pkg, "TestRoot"}}, tasks)
			result := new(numa).runTestCase(tc.pkg, "TestRoot")
			require.NoError(t, result.err)
			content, err := os.ReadFile(marker)
			require.NoError(t, err)
			require.Equal(t, "executed", string(content))
			t.Setenv("PD_UT_ROOT_FAIL", "1")
			result = new(numa).runTestCase(tc.pkg, "TestRoot")
			require.Error(t, result.err)
			require.NotNil(t, result.Failure)
		})
	}
}

func TestListTestCasesReturnsError(t *testing.T) {
	originalWorkDir := workDir
	workDir = t.TempDir()
	t.Cleanup(func() { workDir = originalWorkDir })
	for _, tc := range []struct {
		name   string
		script string
	}{
		{"missing", ""},
		{"exit-after-output", "#!/bin/sh\necho TestRoot\nexit 1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.script != "" {
				require.NoError(t, os.WriteFile(testFileFullPath("."), []byte(tc.script), 0700)) // #nosec G306 -- pd-ut must execute this test-only script.
			}
			tasks, err := listTestCases(".", nil)
			require.Error(t, err)
			require.Empty(t, tasks)
			wg := new(sync.WaitGroup)
			tasksChannel := make(chan []task, 1)
			errorsChannel := make(chan error, 1)
			wg.Add(1)
			go listTestCasesConcurrent(wg, ".", tasksChannel, errorsChannel)
			wg.Wait()
			require.Len(t, errorsChannel, 1)
			require.Error(t, <-errorsChannel)
			require.Empty(t, tasksChannel)
		})
	}
}

func TestTestCommandCPUAllocation(t *testing.T) {
	originalParallel, originalBudget := parallel, cpuBudget
	t.Cleanup(func() {
		parallel, cpuBudget = originalParallel, originalBudget
	})
	for _, tc := range []struct {
		name      string
		workers   int
		budget    int
		suiteCPUs string
	}{
		{"default", 4, 0, "2"},
		{"legacy-override", 8, 0, "4"},
		{"bounded-override", 8, 4, "2"},
		{"fewer-workers", 4, 8, "2"},
		{"single-cpu", 8, 1, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel, cpuBudget = tc.workers, tc.budget
			for name, cpus := range map[string]string{"TestSuite": tc.suiteCPUs, "TestPlain": "1"} {
				command := (&numa{}).testCommand("pkg/test", name)
				index := slices.Index(command.Args, "-test.cpu")
				require.NotEqual(t, -1, index)
				require.Equal(t, cpus, command.Args[index+1], name)
			}
		})
	}
}

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
			// Restrict the path conveyed by the helper process to the
			// system temp directory that created it.
			require.True(t, filepath.IsAbs(childTempDir),
				"child temp dir %q is not absolute", childTempDir)
			rel, err := filepath.Rel(os.TempDir(), childTempDir)
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(rel, ".."),
				"child temp dir %q escapes %q", childTempDir, os.TempDir())
			//nolint:gosec // The path is restricted to the system temp dir above.
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
	// The marker only conveys this process's temp dir to the parent;
	// it is not user-controlled input.
	if err := os.WriteFile(filepath.Join(tempDir, "leftover"), []byte("test"), 0600); err != nil {
		os.Exit(2)
	}
	//nolint:gosec // Test-only path marker written by the helper process.
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
