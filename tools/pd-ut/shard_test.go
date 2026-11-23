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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectPackages(t *testing.T) {
	available := []string{"server", "server/api", "pkg/mcs/tso/server"}
	selected, err := selectPackages(available, "server")
	require.NoError(t, err)
	require.Equal(t, []string{"server"}, selected)
	for _, selection := range []string{"missing", "server,server", "server,"} {
		_, err = selectPackages(available, selection)
		require.Error(t, err)
	}
}

func TestOrderTasks(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "durations.json")
	require.NoError(t, os.WriteFile(filename, []byte(`{"tests":{
		"github.com/tikv/pd/pkg/a.TestSlow":100,
		"github.com/tikv/pd/pkg/a.TestFast":1
	}}`), 0600))
	tasks := []task{{"pkg/a", "TestFast"}, {"pkg/b", "TestNew"}, {"pkg/a", "TestSlow"}}
	require.NoError(t, orderTasks(tasks, filename))
	require.Equal(t, []task{{"pkg/a", "TestSlow"}, {"pkg/a", "TestFast"}, {"pkg/b", "TestNew"}}, tasks)
	require.Error(t, orderTasks(tasks, filename+".missing"))
}

func TestRunTestCaseResults(t *testing.T) {
	if outcome := os.Getenv("PD_UT_RESULT_FIXTURE"); outcome != "" {
		switch outcome {
		case "fail":
			t.Fatal("fixture failure")
		case "skip":
			t.Skip("fixture skip")
		case "skipped-child":
			t.Run("child", func(t *testing.T) { t.Skip("fixture child skip") })
		}
		return
	}

	originalWorkDir := workDir
	workDir = t.TempDir()
	t.Cleanup(func() { workDir = originalWorkDir })
	executable, err := os.Executable()
	require.NoError(t, err)
	require.NoError(t, os.Symlink(executable, filepath.Join(workDir, testFileName("."))))
	for _, outcome := range []string{"pass", "fail", "skip", "skipped-child"} {
		t.Run(outcome, func(t *testing.T) {
			t.Setenv("PD_UT_RESULT_FIXTURE", outcome)
			result := new(numa).runTestCase(".", "TestRunTestCaseResults")
			require.Equal(t, outcome == "fail", result.Failure != nil)
			require.Equal(t, outcome == "fail", result.err != nil)
			require.Equal(t, outcome == "skip", result.SkipMessage != nil)
			if result.Failure != nil {
				require.Contains(t, result.Failure.Contents, "fixture failure")
			}
		})
	}
}
