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

package realcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type triggerDeadlineCtx struct {
	context.Context
	done chan struct{}
}

func newTriggerDeadlineCtx() *triggerDeadlineCtx {
	return &triggerDeadlineCtx{
		Context: context.Background(),
		done:    make(chan struct{}),
	}
}

func (c *triggerDeadlineCtx) Done() <-chan struct{} {
	return c.done
}

func (c *triggerDeadlineCtx) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *triggerDeadlineCtx) trigger() {
	close(c.done)
}

func TestWaitForClusterReady(t *testing.T) {
	t.Run("times out instead of resetting deadline each tick", func(t *testing.T) {
		re := require.New(t)
		dctx := newTriggerDeadlineCtx()

		calls := 0
		err := waitForClusterReady(dctx, 5*time.Millisecond, func(context.Context) (string, error) {
			calls++
			if calls == 2 {
				dctx.trigger()
			}
			return "Error: no playground running", errors.New("exit status 1")
		})
		re.ErrorIs(err, context.DeadlineExceeded)
		re.ErrorContains(err, "exit status 1")
		re.Equal(2, calls)

		// Also verify real wall-clock timeout returns promptly without relying on tick count.
		wallCtx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		start := time.Now()
		err = waitForClusterReady(wallCtx, 10*time.Millisecond, func(context.Context) (string, error) {
			return "Error: no playground running", errors.New("exit status 1")
		})
		re.ErrorIs(err, context.DeadlineExceeded)
		re.Less(time.Since(start), 2*time.Second)
	})

	t.Run("returns nil once check succeeds", func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		calls := 0
		err := waitForClusterReady(ctx, 5*time.Millisecond, func(context.Context) (string, error) {
			calls++
			if calls < 3 {
				return "", errors.New("not ready yet")
			}
			return "ready", nil
		})
		re.NoError(err)
		re.Equal(3, calls)
	})

	t.Run("kills blocking child process group on deadline", func(t *testing.T) {
		re := require.New(t)
		tmpDir := t.TempDir()
		pidFile := filepath.Join(tmpDir, "child.pid")
		tmpPIDFile := pidFile + ".tmp"
		cmdStr := fmt.Sprintf("sleep 30 & pid=$!; echo \"$pid\" > %q && mv %q %q; wait", tmpPIDFile, tmpPIDFile, pidFile)

		dctx := newTriggerDeadlineCtx()
		errCh := make(chan error, 1)
		go func() {
			errCh <- waitForClusterReady(dctx, 10*time.Millisecond, func(checkCtx context.Context) (string, error) {
				return runCommandWithOutputContext(checkCtx, cmdStr, "")
			})
		}()

		var childPID int
		re.Eventually(func() bool {
			pidBytes, readErr := os.ReadFile(pidFile) //nolint:gosec
			if readErr != nil {
				return false
			}
			parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if parseErr != nil || parsed <= 0 {
				return false
			}
			childPID = parsed
			return isProcessRunning(childPID)
		}, 5*time.Second, 20*time.Millisecond)

		triggeredAt := time.Now()
		dctx.trigger()

		err := <-errCh
		re.ErrorIs(err, context.DeadlineExceeded)
		re.Less(time.Since(triggeredAt), 2*time.Second)
		re.Eventually(func() bool {
			return !isProcessRunning(childPID)
		}, 2*time.Second, 20*time.Millisecond)
	})
}

func TestParseProcState(t *testing.T) {
	re := require.New(t)

	state, ppid, pgid, ok := parseProcStat([]byte("1234 (sleep) S 1 1234 1234 0 -1"))
	re.True(ok)
	re.Equal(byte('S'), state)
	re.Equal(1, ppid)
	re.Equal(1234, pgid)

	state, ok = parseProcState([]byte("1234 (cmd (with) parens) Z 42 999 1234 0 -1"))
	re.True(ok)
	re.Equal(byte('Z'), state)

	state, ppid, pgid, ok = parseProcStat([]byte("1234 (dead) X 10 20 1234 0 -1"))
	re.True(ok)
	re.Equal(byte('X'), state)
	re.Equal(10, ppid)
	re.Equal(20, pgid)

	_, ok = parseProcState([]byte("invalid"))
	re.False(ok)

	re.Equal(
		[]string{"/tmp/dir with space/bin/tikv-server", "--data-dir=/tmp/dir with space/data/tag1/tikv-0/data"},
		parseProcCmdline([]byte("/tmp/dir with space/bin/tikv-server\x00--data-dir=/tmp/dir with space/data/tag1/tikv-0/data\x00")),
	)
	re.Nil(parseProcCmdline(nil))

	psProcs := parsePSOutput("101 1 101 Sl /bin/tikv-server --data-dir=/tmp/a/tikv-0/data --pd-endpoints=http://127.0.0.1:2379\n102 101 101 Z /bin/pd-server\n")
	re.Len(psProcs, 2)
	re.Equal(procInfo{
		pid:   101,
		ppid:  1,
		pgid:  101,
		state: 'S',
		args:  []string{"/bin/tikv-server", "--data-dir=/tmp/a/tikv-0/data", "--pd-endpoints=http://127.0.0.1:2379"},
	}, psProcs[0])
	re.Equal(byte('Z'), psProcs[1].state)
}

func TestBuildPlaygroundArgs(t *testing.T) {
	re := require.New(t)

	slimArgs := buildPlaygroundArgs("tikv-slim", "tag_slim", 10000, map[string]int{"pd": 1, "kv": 1, "db": 0, "tiflash": 0})
	re.Equal([]string{
		"playground", "nightly",
		"--mode", "tikv-slim",
		"--kv", "1",
		"--pd", "1",
		"--without-monitor",
		"--tag", "tag_slim",
		"--port-offset", "10000",
		"--pd.binpath", "./bin/pd-server",
		"--kv.binpath", "./third_bin/tikv-server",
	}, slimArgs)

	msArgs := buildPlaygroundArgs("ms", "tag_ms", 0, nil)
	re.Equal([]string{
		"playground", "nightly",
		"--kv", "3",
		"--tiflash", "1",
		"--db", "1",
		"--pd", "3",
		"--without-monitor",
		"--tag", "tag_ms",
		"--pd.mode", "ms",
		"--tso", "1",
		"--scheduling", "1",
		"--pd.binpath", "./bin/pd-server",
		"--kv.binpath", "./third_bin/tikv-server",
		"--db.binpath", "./third_bin/tidb-server",
		"--tiflash.binpath", "./third_bin/tiflash",
		"--tso.binpath", "./bin/pd-server",
		"--scheduling.binpath", "./bin/pd-server",
	}, msArgs)
}

func TestSelectPDEndpoints(t *testing.T) {
	re := require.New(t)

	procs := []procInfo{
		{
			pid: 100, ppid: 1, pgid: 100, state: 'S',
			args: []string{"sh", "-c", "ps -ef | grep tikv-server | awk -F '--pd-endpoints=' '{print $2}'"},
		},
		{
			pid: 101, ppid: 1, pgid: 101, state: 'S',
			args: []string{"/bin/tiup", "playground", "nightly", "--tag", "tag1"},
		},
		{
			pid: 102, ppid: 101, pgid: 101, state: 'S',
			args: []string{"/bin/tiup-playground", "nightly", "--tag", "tag1"},
		},
		{
			// Escaped PGID (real TiUP spawns tikv-server in its own PGID) under spaced TIUP_HOME.
			pid: 103, ppid: 102, pgid: 103, state: 'S',
			args: []string{"/bin/tikv-server", "--addr=127.0.0.1:20160", "--pd-endpoints=http://127.0.0.1:2379", "--data-dir=/tmp/tiup home 1/data/tag1/tikv-0/data"},
		},
		{
			pid: 104, ppid: 102, pgid: 104, state: 'S',
			args: []string{"/bin/tikv-server", "--addr=127.0.0.1:20161", "--pd-endpoints", "http://127.0.0.1:2379,http://127.0.0.1:2380", "--data-dir", "/tmp/tiup home 1/data/tag1/tikv-1/data"},
		},
		{
			// Script-wrapped tikv-server stub (#!/bin/sh) under Cluster 1.
			pid: 105, ppid: 1, pgid: 105, state: 'S',
			args: []string{"/bin/sh", "/tmp/stub/tikv-server", "--pd-endpoints=http://127.0.0.1:2381", "--data-dir=/tmp/tiup home 1/data/tag1/tikv-2/data"},
		},
		{
			// Identical tag "tag1" under a different TIUP_HOME.
			pid: 203, ppid: 202, pgid: 203, state: 'S',
			args: []string{"/bin/tikv-server", "--addr=127.0.0.1:30160", "--pd-endpoints=http://127.0.0.1:39681", "--data-dir=/tmp/tiup home 2/data/tag1/tikv-0/data"},
		},
		{
			// Second TiKV with its own data-dir that only borrows Cluster 1's --config must NOT match Cluster 1.
			pid: 204, ppid: 202, pgid: 204, state: 'S',
			args: []string{
				"/bin/tikv-server",
				"--addr=127.0.0.1:30161",
				"--pd-endpoints=http://127.0.0.1:39682",
				"--config=/tmp/tiup home 1/data/tag1/tikv-0/tikv.toml",
				"--data-dir=/tmp/tiup home 2/data/tag1/tikv-1/data",
			},
		},
		{
			// Similar tag prefix "tag10" under the same TIUP_HOME.
			pid: 303, ppid: 302, pgid: 303, state: 'S',
			args: []string{"/bin/tikv-server", "--addr=127.0.0.1:40160", "--pd-endpoints=http://127.0.0.1:49681", "--data-dir=/tmp/tiup home 1/data/tag10/tikv-0/data"},
		},
		{
			// Unrelated binary with similar name.
			pid: 403, ppid: 1, pgid: 403, state: 'S',
			args: []string{"/bin/tikv-server-cse", "--pd-endpoints=http://127.0.0.1:59681", "--data-dir=/tmp/tiup home 1/data/tag1/tikv-9/data"},
		},
		{
			// Zombie tikv-server must be ignored.
			pid: 404, ppid: 102, pgid: 404, state: 'Z',
			args: []string{"/bin/tikv-server", "--pd-endpoints=http://127.0.0.1:60000", "--data-dir=/tmp/tiup home 1/data/tag1/tikv-2/data"},
		},
	}

	re.Equal(
		[]string{"http://127.0.0.1:2379", "http://127.0.0.1:2380", "http://127.0.0.1:2381"},
		selectPDEndpoints(procs, "/tmp/tiup home 1/data/tag1", 101, 500),
	)
	re.Equal(
		[]string{"http://127.0.0.1:39681", "http://127.0.0.1:39682"},
		selectPDEndpoints(procs, "/tmp/tiup home 2/data/tag1", 0, 500),
	)
	re.Equal(
		[]string{"http://127.0.0.1:49681"},
		selectPDEndpoints(procs, "/tmp/tiup home 1/data/tag10", 0, 500),
	)
	re.Equal(
		[]string{"http://127.0.0.1:2379", "http://127.0.0.1:2380", "http://127.0.0.1:2381", "http://127.0.0.1:39681", "http://127.0.0.1:39682", "http://127.0.0.1:49681"},
		selectPDEndpoints(procs, "", 0, 500),
	)
}

func TestSelectOwnedPIDs(t *testing.T) {
	re := require.New(t)

	dataDir1 := "/tmp/tiup home 1/data/pd_test_1"
	dataDir2 := "/tmp/tiup home 2/data/pd_test_1"
	dataDir10 := "/tmp/tiup home 1/data/pd_test_10"

	procs := []procInfo{
		// Cluster 1 process tree (tiup + tiup-playground in pgid 101; pd-server child with own pgid 103).
		{
			pid: 101, ppid: 500, pgid: 101, state: 'S',
			args: []string{"/tmp/tiup home 1/bin/tiup", "playground", "nightly", "--tag", "pd_test_1"},
		},
		{
			pid: 102, ppid: 101, pgid: 101, state: 'S',
			args: []string{"/tmp/tiup home 1/components/playground/v1.17.1/tiup-playground", "nightly", "--tag", "pd_test_1"},
		},
		{
			pid: 103, ppid: 102, pgid: 103, state: 'S',
			args: []string{"/repo/bin/pd-server", "--data-dir=" + dataDir1 + "/pd-0/data"},
		},
		// Escaped orphaned tikv-server belonging to Cluster 1 (reparented to PID 1, own PGID).
		{
			pid: 104, ppid: 1, pgid: 104, state: 'S',
			args: []string{"/repo/third_bin/tikv-server", "--data-dir=" + dataDir1 + "/tikv-0/data"},
		},
		// Independent read-only tiup playground display running in another PGID must NOT be selected.
		{
			pid: 105, ppid: 1, pgid: 105, state: 'S',
			args: []string{"/tmp/tiup home 1/bin/tiup", "playground", "display", "--tag", "pd_test_1"},
		},
		{
			pid: 106, ppid: 105, pgid: 105, state: 'S',
			args: []string{"/tmp/tiup home 1/components/playground/v1.17.1/tiup-playground", "display", "--tag", "pd_test_1"},
		},
		// Independent read-only viewers referencing Cluster 1's dataDir must NEVER be selected.
		{
			pid: 150, ppid: 500, pgid: 500, state: 'S',
			args: []string{"tail", "-f", dataDir1 + "/pd-0/pd.log"},
		},
		{
			pid: 151, ppid: 500, pgid: 500, state: 'S',
			args: []string{"/bin/cat", dataDir1 + "/tikv-0/tikv.log"},
		},
		{
			pid: 152, ppid: 500, pgid: 500, state: 'S',
			args: []string{"python3", "-c", "open('" + dataDir1 + "/pd-0/pd.log')"},
		},
		// Cluster 2 with IDENTICAL tag "pd_test_1" but different TIUP_HOME (including one borrowing Cluster 1's --config).
		{
			pid: 201, ppid: 600, pgid: 201, state: 'S',
			args: []string{"/tmp/tiup home 2/bin/tiup", "playground", "nightly", "--tag", "pd_test_1"},
		},
		{
			pid: 202, ppid: 201, pgid: 202, state: 'S',
			args: []string{"/repo/bin/pd-server", "--data-dir=" + dataDir2 + "/pd-0/data"},
		},
		{
			pid: 203, ppid: 1, pgid: 203, state: 'S',
			args: []string{"/repo/third_bin/tikv-server", "--config=" + dataDir1 + "/tikv-0/tikv.toml", "--data-dir=" + dataDir2 + "/tikv-0/data"},
		},
		// Cluster 10 with similar prefix tag "pd_test_10" in the same TIUP_HOME.
		{
			pid: 301, ppid: 700, pgid: 301, state: 'S',
			args: []string{"/tmp/tiup home 1/bin/tiup", "playground", "nightly", "--tag", "pd_test_10"},
		},
		{
			pid: 302, ppid: 301, pgid: 302, state: 'S',
			args: []string{"/repo/bin/pd-server", "--data-dir=" + dataDir10 + "/pd-0/data"},
		},
		// Self PID and zombie in targetPGID must be excluded.
		{
			pid: 500, ppid: 1, pgid: 101, state: 'S',
			args: []string{"/test/binary"},
		},
		{
			pid: 107, ppid: 101, pgid: 101, state: 'Z',
			args: []string{"/repo/bin/pd-server", "--data-dir=" + dataDir1 + "/pd-1/data"},
		},
	}

	re.Equal([]int{101, 102, 103, 104}, selectOwnedPIDs(procs, dataDir1, 101, 500))
	re.Equal([]int{201, 202, 203}, selectOwnedPIDs(procs, dataDir2, 201, 500))
	re.Equal([]int{301, 302}, selectOwnedPIDs(procs, dataDir10, 301, 500))

	// Reused launcher PID in a different PGID and its descendants must NOT be selected.
	reusedProcs := []procInfo{
		{
			pid: 42001, ppid: 41000, pgid: 41000, state: 'S',
			args: []string{"/usr/bin/unrelated"},
		},
		{
			pid: 42002, ppid: 42001, pgid: 41000, state: 'S',
			args: []string{"/usr/bin/unrelated-child"},
		},
	}
	re.Empty(selectOwnedPIDs(reusedProcs, "", 42001, 500))
	re.False(hasLivePGIDMembers(reusedProcs, 42001, 500))

	// If another live process still belongs to target PGID 42001 while PID 42001
	// was reused in PGID 41000, only the true PGID 42001 member is selected.
	mixedProcs := append(reusedProcs, procInfo{
		pid: 42003, ppid: 1, pgid: 42001, state: 'S',
		args: []string{"/bin/tiup-playground", "nightly"},
	})
	re.Equal([]int{42003}, selectOwnedPIDs(mixedProcs, "", 42001, 500))
	re.True(hasLivePGIDMembers(mixedProcs, 42001, 500))

	// An un-reaped zombie still in targetPGID must not be selected itself,
	// while its live escaped descendant is still traversed.
	zombieParentProcs := []procInfo{
		{
			pid: 43001, ppid: 500, pgid: 43001, state: 'Z',
			args: []string{"/bin/tiup", "playground", "nightly"},
		},
		{
			pid: 43002, ppid: 43001, pgid: 43002, state: 'S',
			args: []string{"/bin/helper"},
		},
	}
	re.Equal([]int{43002}, selectOwnedPIDs(zombieParentProcs, "", 43001, 500))
	re.False(hasLivePGIDMembers(zombieParentProcs, 43001, 500))
}

func TestTagUniqueAcrossProcesses(t *testing.T) {
	re := require.New(t)

	s := clusterSuite{suiteName: "cluster_id", mode: "tikv-slim"}
	tagA := s.tag()
	tagB := s.tag()
	re.NotEqual(tagA, tagB)
	re.Contains(tagA, fmt.Sprintf("_%d_", os.Getpid()))

	runHelper := func() (*exec.Cmd, *strings.Builder) {
		var buf strings.Builder
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperPrintTag$") //nolint:gosec
		cmd.Env = append(os.Environ(), "PD_TEST_HELPER_PRINT_TAG=1")
		cmd.Stdout = &buf
		return cmd, &buf
	}

	// Start two independent test processes concurrently so they execute in the same second.
	cmd1, out1 := runHelper()
	cmd2, out2 := runHelper()
	re.NoError(cmd1.Start())
	re.NoError(cmd2.Start())
	re.NoError(cmd1.Wait())
	re.NoError(cmd2.Wait())

	extractTag := func(output string) string {
		for line := range strings.SplitSeq(output, "\n") {
			if tag, ok := strings.CutPrefix(line, "TAG="); ok {
				return strings.TrimSpace(tag)
			}
		}
		return ""
	}
	procTag1 := extractTag(out1.String())
	procTag2 := extractTag(out2.String())
	re.NotEmpty(procTag1)
	re.NotEmpty(procTag2)
	re.NotEqual(procTag1, procTag2)
	re.Contains(procTag1, fmt.Sprintf("_%d_", cmd1.Process.Pid))
	re.Contains(procTag2, fmt.Sprintf("_%d_", cmd2.Process.Pid))
}

func TestHelperPrintTag(_ *testing.T) {
	if os.Getenv("PD_TEST_HELPER_PRINT_TAG") != "1" {
		return
	}
	s := clusterSuite{suiteName: "cluster_id", mode: "tikv-slim"}
	fmt.Printf("TAG=%s\n", s.tag())
}

func TestHelperFakeServer(_ *testing.T) {
	if os.Getenv("PD_TEST_HELPER_FAKE_SERVER") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
}

func TestClusterIsolationWithIdenticalTagDifferentTiUPHome(t *testing.T) {
	re := require.New(t)

	repoDir := t.TempDir()
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
	for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server"} {
		re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
	}

	// Deliberately force an identical tag across two clusters with different TIUP_HOME,
	// including a TIUP_HOME path containing spaces.
	sharedTag := fmt.Sprintf("pd_real_cluster_test_iso_%d_%d", os.Getpid(), time.Now().UnixNano())
	tiupHome1 := filepath.Join(t.TempDir(), "tiup home one")
	tiupHome2 := filepath.Join(t.TempDir(), "tiup home two")
	dataDir1 := filepath.Join(tiupHome1, "data", sharedTag)
	dataDir2 := filepath.Join(tiupHome2, "data", sharedTag)
	re.NoError(os.MkdirAll(filepath.Join(dataDir1, "pd-0"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(dataDir2, "pd-0"), 0750))
	pdLog1 := filepath.Join(dataDir1, "pd-0", "pd.log")
	re.NoError(os.WriteFile(pdLog1, []byte("pd log line\n"), 0600))

	makeFakeTiUP := func(home, pidFile string) string {
		binPath := filepath.Join(home, "bin", "tiup")
		re.NoError(os.MkdirAll(filepath.Dir(binPath), 0750))
		script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "$TIUP_HOME/components/playground/v1.17.1/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  echo $$ > %q
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  echo "Pid Role Uptime"
  exit 0
fi
exit 1
`, pidFile)
		re.NoError(os.WriteFile(binPath, []byte(script), 0600))
		re.NoError(os.Chmod(binPath, 0700)) //nolint:gosec
		return binPath
	}

	startFakeServer := func(binaryName string, extraArgs ...string) int {
		args := append([]string{filepath.Join(repoDir, "bin", binaryName), "-test.run=^TestHelperFakeServer$", "--"}, extraArgs...)
		cmd := &exec.Cmd{
			Path:        os.Args[0],
			Args:        args,
			Env:         append(os.Environ(), "PD_TEST_HELPER_FAKE_SERVER=1"),
			SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
		}
		re.NoError(cmd.Start())
		go func() {
			_ = cmd.Wait()
		}()
		t.Cleanup(func() {
			if isProcessRunning(cmd.Process.Pid) {
				_ = syscall.Kill(cmd.Process.Pid, syscall.SIGKILL)
			}
		})
		return cmd.Process.Pid
	}

	origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
	origTimeout, origPoll := clusterWaitTimeout, clusterPollPeriod
	t.Cleanup(func() {
		realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
		clusterWaitTimeout, clusterPollPeriod = origTimeout, origPoll
	})
	realClusterRepoRoot = repoDir
	playgroundLogDir = t.TempDir()
	clusterWaitTimeout = 2 * time.Second
	clusterPollPeriod = 40 * time.Millisecond

	pidFile1 := filepath.Join(t.TempDir(), "c1.pid")
	pidFile2 := filepath.Join(t.TempDir(), "c2.pid")
	fakeTiUP1 := makeFakeTiUP(tiupHome1, pidFile1)
	fakeTiUP2 := makeFakeTiUP(tiupHome2, pidFile2)

	tiupBin = fakeTiUP1
	t.Setenv("TIUP_HOME", tiupHome1)
	c1 := newCluster(re, sharedTag, dataDir1, "tikv-slim", map[string]int{"pd": 1, "kv": 1})
	defer c1.stop()
	c1.start()

	tiupBin = fakeTiUP2
	t.Setenv("TIUP_HOME", tiupHome2)
	c2 := newCluster(re, sharedTag, dataDir2, "tikv-slim", map[string]int{"pd": 1, "kv": 1})
	defer c2.stop()
	c2.start()

	// Spawn escaped (independent PGID & PPID) fake pd-server and tikv-server for both clusters.
	// Cluster 2's tikv-server also borrows Cluster 1's --config while keeping its own --data-dir.
	pd1PID := startFakeServer("pd-server", "--data-dir="+filepath.Join(dataDir1, "pd-0", "data"))
	kv1PID := startFakeServer("tikv-server", "--pd-endpoints=http://127.0.0.1:23791", "--data-dir="+filepath.Join(dataDir1, "tikv-0", "data"))
	pd2PID := startFakeServer("pd-server", "--data-dir="+filepath.Join(dataDir2, "pd-0", "data"))
	kv2PID := startFakeServer(
		"tikv-server",
		"--pd-endpoints=http://127.0.0.1:23792",
		"--config="+filepath.Join(dataDir1, "tikv-0", "tikv.toml"),
		"--data-dir="+filepath.Join(dataDir2, "tikv-0", "data"),
	)

	// Spawn an independent read-only "tiup playground display --tag <sharedTag>" in its own PGID.
	displayPID := startFakeServer("tiup", "playground", "display", "--tag", sharedTag)

	// Spawn an independent read-only log viewer referencing Cluster 1's dataDir.
	viewerCmd := exec.Command("tail", "-f", pdLog1) //nolint:gosec
	re.NoError(viewerCmd.Start())
	viewerPID := viewerCmd.Process.Pid
	go func() {
		_ = viewerCmd.Wait()
	}()
	t.Cleanup(func() {
		if isProcessRunning(viewerPID) {
			_ = syscall.Kill(viewerPID, syscall.SIGKILL)
		}
	})

	readPID := func(path string) int {
		var pid int
		re.Eventually(func() bool {
			b, err := os.ReadFile(path) //nolint:gosec
			if err != nil {
				return false
			}
			v, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil {
				return false
			}
			pid = v
			return true
		}, time.Second, 20*time.Millisecond)
		return pid
	}
	tiup1PID := readPID(pidFile1)
	tiup2PID := readPID(pidFile2)

	// Endpoint discovery must isolate strictly by cluster dataDir even with identical tags and spaced TIUP_HOME.
	re.Equal([]string{"http://127.0.0.1:23791"}, c1.pdEndpoints())
	re.Equal([]string{"http://127.0.0.1:23792"}, c2.pdEndpoints())

	// Stopping Cluster 1 must kill its own tiup and escaped pd-server/tikv-server,
	// while sparing the independent tiup display, read-only log viewer, and all of Cluster 2's processes.
	c1.stop()
	re.Eventually(func() bool {
		return !isProcessRunning(tiup1PID) && !isProcessRunning(pd1PID) && !isProcessRunning(kv1PID)
	}, 2*time.Second, 20*time.Millisecond)
	re.True(isProcessRunning(displayPID))
	re.True(isProcessRunning(viewerPID))
	re.True(isProcessRunning(tiup2PID))
	re.True(isProcessRunning(pd2PID))
	re.True(isProcessRunning(kv2PID))
	re.Equal([]string{"http://127.0.0.1:23792"}, c2.pdEndpoints())

	// Stopping Cluster 2 must kill all of Cluster 2's processes and still spare display and log viewer.
	c2.stop()
	re.Eventually(func() bool {
		return !isProcessRunning(tiup2PID) && !isProcessRunning(pd2PID) && !isProcessRunning(kv2PID)
	}, 2*time.Second, 20*time.Millisecond)
	re.True(isProcessRunning(displayPID))
	re.True(isProcessRunning(viewerPID))
}

func TestClusterWithSpacesInTiUPHome(t *testing.T) {
	re := require.New(t)

	defaultHome := t.TempDir()
	spacedTiUPHome := filepath.Join(t.TempDir(), "profile with space")
	re.NoError(os.MkdirAll(filepath.Join(spacedTiUPHome, "bin"), 0750))
	t.Setenv("HOME", defaultHome)
	t.Setenv("TIUP_HOME", spacedTiUPHome)

	// Populate default ~/.tiup with a fake playground, while TIUP_HOME is empty.
	defaultMarker := filepath.Join(defaultHome, ".tiup", "playground-installed")
	re.NoError(os.MkdirAll(filepath.Dir(defaultMarker), 0750))
	re.NoError(os.WriteFile(defaultMarker, []byte("ok"), 0600))

	repoDir := t.TempDir()
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
	for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server"} {
		re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
	}

	fakeTiUP := filepath.Join(spacedTiUPHome, "bin", "tiup")
	installLog := filepath.Join(t.TempDir(), "install.log")
	displayReady := filepath.Join(t.TempDir(), "display.ready")
	script := fmt.Sprintf(`#!/bin/sh
home="${TIUP_HOME:-$HOME/.tiup}"
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  if [ -f "$home/playground-installed" ]; then
    echo "$home/components/playground/v1.17.1/tiup-playground"
    exit 0
  fi
  exit 1
fi
if [ "$1" = "install" ] && [ "$2" = "playground" ]; then
  mkdir -p "$home"
  echo installed >> %q
  touch "$home/playground-installed"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  touch %q
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  if [ -f %q ]; then
    echo "Pid Role Uptime"
    exit 0
  fi
  exit 1
fi
exit 1
`, installLog, displayReady, displayReady)
	re.NoError(os.WriteFile(fakeTiUP, []byte(script), 0600))
	re.NoError(os.Chmod(fakeTiUP, 0700)) //nolint:gosec

	origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
	origTimeout, origPoll := clusterWaitTimeout, clusterPollPeriod
	t.Cleanup(func() {
		realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
		clusterWaitTimeout, clusterPollPeriod = origTimeout, origPoll
	})
	realClusterRepoRoot = repoDir
	tiupBin = ""
	playgroundLogDir = filepath.Join(t.TempDir(), "logs with space")
	clusterWaitTimeout = 2 * time.Second
	clusterPollPeriod = 40 * time.Millisecond

	re.Equal(fakeTiUP, resolveTiUPBin())
	re.False(isPlaygroundInstalled(fakeTiUP))

	s := clusterSuite{suiteName: "cluster_id", mode: "tikv-slim"}
	tag := s.tag()
	re.Equal(filepath.Join(spacedTiUPHome, "data", tag), dataDirForTag(tag))

	c := newCluster(re, tag, dataDirForTag(tag), s.mode, map[string]int{"pd": 1, "kv": 1, "db": 0, "tiflash": 0})
	defer c.stop()
	c.start()

	re.True(isPlaygroundInstalled(fakeTiUP))
	// Calling again should reuse the installed playground without reinstalling.
	ensurePlaygroundInstalled(re, fakeTiUP)
	logBytes, err := os.ReadFile(installLog)
	re.NoError(err)
	re.Equal("installed\n", string(logBytes))
}

func TestClusterStopDoesNotKillSimilarTag(t *testing.T) {
	re := require.New(t)

	pidFile1 := filepath.Join(t.TempDir(), "c1.pid")
	pidFile10 := filepath.Join(t.TempDir(), "c10.pid")
	repoDir := t.TempDir()
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
	for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server"} {
		re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
	}

	makeFakeTiUP := func(pidFile string) string {
		binPath := filepath.Join(t.TempDir(), "tiup")
		script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "/fake/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  echo $$ > %q
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  echo "Pid Role Uptime"
  exit 0
fi
exit 1
`, pidFile)
		re.NoError(os.WriteFile(binPath, []byte(script), 0600))
		re.NoError(os.Chmod(binPath, 0700)) //nolint:gosec
		return binPath
	}

	origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
	origTimeout, origPoll := clusterWaitTimeout, clusterPollPeriod
	t.Cleanup(func() {
		realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
		clusterWaitTimeout, clusterPollPeriod = origTimeout, origPoll
	})
	realClusterRepoRoot = repoDir
	playgroundLogDir = t.TempDir()
	clusterWaitTimeout = 2 * time.Second
	clusterPollPeriod = 40 * time.Millisecond

	prefix := fmt.Sprintf("pd_real_cluster_test_sim_%d", time.Now().UnixNano())
	tag1 := prefix + "_1"
	tag10 := prefix + "_10"

	tiupBin = makeFakeTiUP(pidFile1)
	c1 := newCluster(re, tag1, filepath.Join(t.TempDir(), tag1), "tikv-slim", map[string]int{"pd": 1, "kv": 1})
	defer c1.stop()
	c1.start()

	tiupBin = makeFakeTiUP(pidFile10)
	c10 := newCluster(re, tag10, filepath.Join(t.TempDir(), tag10), "tikv-slim", map[string]int{"pd": 1, "kv": 1})
	defer c10.stop()
	c10.start()

	readPID := func(path string) int {
		var pid int
		re.Eventually(func() bool {
			b, err := os.ReadFile(path)
			if err != nil {
				return false
			}
			v, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil {
				return false
			}
			pid = v
			return true
		}, time.Second, 20*time.Millisecond)
		return pid
	}

	pid1 := readPID(pidFile1)
	pid10 := readPID(pidFile10)
	re.True(isProcessRunning(pid1))
	re.True(isProcessRunning(pid10))

	// Stopping _1 must not terminate concurrently running _10.
	c1.stop()
	re.False(isProcessRunning(pid1))
	re.True(isProcessRunning(pid10))

	c10.stop()
	re.False(isProcessRunning(pid10))
}

type failNowRecorder struct {
	failed bool
}

func (f *failNowRecorder) Errorf(string, ...any) {
	f.failed = true
}

func (f *failNowRecorder) FailNow() {
	f.failed = true
	runtime.Goexit()
}

func setupFakeFailingClusterEnv(t *testing.T, pidFile string) (repoDir, fakeTiUP, logDir string) {
	t.Helper()
	re := require.New(t)
	repoDir = t.TempDir()
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
	for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server", "third_bin/tidb-server", "third_bin/tiflash"} {
		re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
	}

	logDir = t.TempDir()
	fakeTiUP = filepath.Join(t.TempDir(), "tiup")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "/fake/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  echo "Error: no playground running"
  exit 1
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  echo $$ > %q
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
exit 1
`, pidFile)
	re.NoError(os.WriteFile(fakeTiUP, []byte(script), 0600))
	re.NoError(os.Chmod(fakeTiUP, 0700)) //nolint:gosec
	return repoDir, fakeTiUP, logDir
}

func TestClusterCleanupOnPartialStartFailure(t *testing.T) {
	t.Run("direct cluster start registers defer stop before start", func(t *testing.T) {
		re := require.New(t)
		pidFile := filepath.Join(t.TempDir(), "cluster.pid")
		repoDir, fakeTiUP, logDir := setupFakeFailingClusterEnv(t, pidFile)

		origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
		origTimeout, origPoll := clusterWaitTimeout, clusterPollPeriod
		t.Cleanup(func() {
			realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
			clusterWaitTimeout, clusterPollPeriod = origTimeout, origPoll
		})
		realClusterRepoRoot = repoDir
		tiupBin = fakeTiUP
		playgroundLogDir = logDir
		clusterWaitTimeout = 200 * time.Millisecond
		clusterPollPeriod = 40 * time.Millisecond

		rec := &failNowRecorder{}
		done := make(chan struct{})
		s := clusterSuite{suiteName: "partial_fail_direct", mode: "tikv-slim"}
		tag := s.tag()
		go func() {
			defer close(done)
			c := newCluster(require.New(rec), tag, dataDirForTag(tag), s.mode, map[string]int{"pd": 1, "kv": 1, "db": 0, "tiflash": 0})
			defer c.stop()
			c.start()
		}()
		<-done

		re.True(rec.failed)
		pidBytes, err := os.ReadFile(pidFile)
		re.NoError(err)
		pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		re.NoError(err)
		re.Eventually(func() bool {
			return !isProcessRunning(pid)
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("suite SetupSuite cleans up cluster when waitReady times out", func(t *testing.T) {
		re := require.New(t)
		pidFile := filepath.Join(t.TempDir(), "suite_cluster.pid")
		repoDir, fakeTiUP, logDir := setupFakeFailingClusterEnv(t, pidFile)

		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperSetupSuitePartialStartFailure$") //nolint:gosec
		cmd.Env = append(os.Environ(),
			"PD_TEST_HELPER_SETUP_SUITE_FAIL=1",
			"PD_TEST_HELPER_REPO="+repoDir,
			"PD_TEST_HELPER_TIUP="+fakeTiUP,
			"PD_TEST_HELPER_LOGDIR="+logDir,
		)
		output, err := cmd.CombinedOutput()
		re.Error(err, "expected helper suite to fail on startup timeout, output: %s", string(output))
		re.Contains(string(output), "TiUP is not ready after timeout")

		pidBytes, readErr := os.ReadFile(pidFile)
		re.NoError(readErr)
		pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		re.NoError(parseErr)
		re.Eventually(func() bool {
			return !isProcessRunning(pid)
		}, 2*time.Second, 20*time.Millisecond)
	})
}

type helperFailingSuite struct {
	clusterSuite
}

func (s *helperFailingSuite) TestShouldNotRun() {
	s.FailNow("test body should not run after SetupSuite failure")
}

func TestHelperSetupSuitePartialStartFailure(t *testing.T) {
	if os.Getenv("PD_TEST_HELPER_SETUP_SUITE_FAIL") != "1" {
		return
	}
	realClusterRepoRoot = os.Getenv("PD_TEST_HELPER_REPO")
	tiupBin = os.Getenv("PD_TEST_HELPER_TIUP")
	playgroundLogDir = os.Getenv("PD_TEST_HELPER_LOGDIR")
	clusterWaitTimeout = 200 * time.Millisecond
	fullClusterWaitTimeout = 200 * time.Millisecond
	clusterPollPeriod = 40 * time.Millisecond

	suite.Run(t, &helperFailingSuite{
		suiteName: "partial_fail_suite",
	})
}

func TestClusterWaitTimeoutBudget(t *testing.T) {
	re := require.New(t)

	// Full clusters include TiFlash, whose default TiUP --tiflash.timeout is 120s
	// after PD and TiDB startup, so fullClusterWaitTimeout must exceed both.
	re.Greater(fullClusterWaitTimeout, 120*time.Second)
	re.Greater(fullClusterWaitTimeout, clusterWaitTimeout)
	re.Equal(clusterWaitTimeout, (&cluster{mode: "tikv-slim"}).waitTimeout())
	re.Equal(fullClusterWaitTimeout, (&cluster{mode: ""}).waitTimeout())
	re.Equal(fullClusterWaitTimeout, (&cluster{mode: "ms"}).waitTimeout())

	repoDir := t.TempDir()
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
	re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
	for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server", "third_bin/tidb-server", "third_bin/tiflash"} {
		re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
	}

	readyFile := filepath.Join(t.TempDir(), "display.ready")
	fakeTiUP := filepath.Join(t.TempDir(), "tiup")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "/fake/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  if [ -f %q ]; then
    echo "Pid Role Uptime"
    exit 0
  fi
  echo "Error: no playground running"
  exit 1
fi
exit 1
`, readyFile)
	re.NoError(os.WriteFile(fakeTiUP, []byte(script), 0600))
	re.NoError(os.Chmod(fakeTiUP, 0700)) //nolint:gosec

	origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
	origTimeout, origFullTimeout, origPoll := clusterWaitTimeout, fullClusterWaitTimeout, clusterPollPeriod
	t.Cleanup(func() {
		realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
		clusterWaitTimeout, fullClusterWaitTimeout, clusterPollPeriod = origTimeout, origFullTimeout, origPoll
	})
	realClusterRepoRoot = repoDir
	tiupBin = fakeTiUP
	playgroundLogDir = t.TempDir()
	clusterWaitTimeout = 200 * time.Millisecond
	fullClusterWaitTimeout = 2 * time.Second
	clusterPollPeriod = 20 * time.Millisecond

	// A full cluster that becomes ready after clusterWaitTimeout (200ms) still
	// succeeds within fullClusterWaitTimeout (2s).
	readyTimer := time.AfterFunc(350*time.Millisecond, func() {
		_ = os.WriteFile(readyFile, []byte("ready"), 0600)
	})
	defer readyTimer.Stop()

	fullTag := fmt.Sprintf("pd_real_cluster_test_full_budget_%d", time.Now().UnixNano())
	fullCluster := newCluster(re, fullTag, filepath.Join(t.TempDir(), fullTag), "")
	defer fullCluster.stop()
	start := time.Now()
	fullCluster.start()
	re.GreaterOrEqual(time.Since(start), 300*time.Millisecond)

	// A tikv-slim cluster still enforces the shorter clusterWaitTimeout.
	re.NoError(os.Remove(readyFile))
	rec := &failNowRecorder{}
	done := make(chan struct{})
	slimTag := fmt.Sprintf("pd_real_cluster_test_slim_budget_%d", time.Now().UnixNano())
	go func() {
		defer close(done)
		c := newCluster(require.New(rec), slimTag, filepath.Join(t.TempDir(), slimTag), "tikv-slim", map[string]int{"pd": 1, "kv": 1})
		defer c.stop()
		c.start()
	}()
	<-done
	re.True(rec.failed)
}

func TestClusterStopPGIDSignalGuard(t *testing.T) {
	type signalCall struct {
		pid int
		sig syscall.Signal
	}

	setupEnv := func(t *testing.T, tiupScript string) (*sync.Mutex, *[]signalCall) {
		t.Helper()
		re := require.New(t)
		repoDir := t.TempDir()
		re.NoError(os.MkdirAll(filepath.Join(repoDir, "bin"), 0750))
		re.NoError(os.MkdirAll(filepath.Join(repoDir, "third_bin"), 0750))
		for _, rel := range []string{"bin/pd-server", "third_bin/tikv-server"} {
			re.NoError(os.WriteFile(filepath.Join(repoDir, rel), []byte("#!/bin/sh\n"), 0600))
		}
		fakeTiUP := filepath.Join(t.TempDir(), "tiup")
		re.NoError(os.WriteFile(fakeTiUP, []byte(tiupScript), 0600))
		re.NoError(os.Chmod(fakeTiUP, 0700)) //nolint:gosec

		origRepo, origBin, origLogDir := realClusterRepoRoot, tiupBin, playgroundLogDir
		origTimeout, origPoll, origGrace := clusterWaitTimeout, clusterPollPeriod, clusterStopGraceTimeout
		origKill := killProcess
		var mu sync.Mutex
		var calls []signalCall
		t.Cleanup(func() {
			realClusterRepoRoot, tiupBin, playgroundLogDir = origRepo, origBin, origLogDir
			clusterWaitTimeout, clusterPollPeriod, clusterStopGraceTimeout = origTimeout, origPoll, origGrace
			killProcess = origKill
		})
		realClusterRepoRoot = repoDir
		tiupBin = fakeTiUP
		playgroundLogDir = t.TempDir()
		clusterWaitTimeout = 2 * time.Second
		clusterPollPeriod = 20 * time.Millisecond
		clusterStopGraceTimeout = 150 * time.Millisecond
		killProcess = func(pid int, sig syscall.Signal) error {
			mu.Lock()
			calls = append(calls, signalCall{pid: pid, sig: sig})
			mu.Unlock()
			return origKill(pid, sig)
		}
		return &mu, &calls
	}

	t.Run("skips final group SIGKILL after clean launcher exit", func(t *testing.T) {
		re := require.New(t)
		script := `#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "/fake/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  sleep 30 &
  child=$!
  wait "$child"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  echo "Pid Role Uptime"
  exit 0
fi
exit 1
`
		mu, calls := setupEnv(t, script)
		tag := fmt.Sprintf("pd_real_cluster_test_pgid_clean_%d", time.Now().UnixNano())
		c := newCluster(re, tag, filepath.Join(t.TempDir(), tag), "tikv-slim", map[string]int{"pd": 1, "kv": 1})
		defer c.stop()
		c.start()
		pgid := c.pgid
		re.Positive(pgid)

		c.stop()

		mu.Lock()
		recorded := append([]signalCall(nil), (*calls)...)
		mu.Unlock()
		var sawGroupTerm, sawGroupKill bool
		for _, call := range recorded {
			if call.pid == -pgid && call.sig == syscall.SIGTERM {
				sawGroupTerm = true
			}
			if call.pid == -pgid && call.sig == syscall.SIGKILL {
				sawGroupKill = true
			}
		}
		re.True(sawGroupTerm, "expected initial group SIGTERM")
		re.False(sawGroupKill, "must not send final group SIGKILL after launcher is reaped with no live PGID members")
	})

	t.Run("sends final group SIGKILL when group member survives reaped launcher", func(t *testing.T) {
		re := require.New(t)
		childPIDFile := filepath.Join(t.TempDir(), "survivor.pid")
		tmpPIDFile := childPIDFile + ".tmp"
		script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--binary" ] && [ "$2" = "playground" ]; then
  echo "/fake/tiup-playground"
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "nightly" ]; then
  sh -c 'trap "" TERM; echo $$ > %q && mv %q %q; sleep 30' &
  trap 'exit 0' TERM
  sleep 30 &
  wait
  exit 0
fi
if [ "$1" = "playground" ] && [ "$2" = "display" ]; then
  echo "Pid Role Uptime"
  exit 0
fi
exit 1
`, tmpPIDFile, tmpPIDFile, childPIDFile)
		mu, calls := setupEnv(t, script)
		tag := fmt.Sprintf("pd_real_cluster_test_pgid_survivor_%d", time.Now().UnixNano())
		c := newCluster(re, tag, filepath.Join(t.TempDir(), tag), "tikv-slim", map[string]int{"pd": 1, "kv": 1})
		defer c.stop()
		c.start()
		pgid := c.pgid
		re.Positive(pgid)

		var survivorPID int
		re.Eventually(func() bool {
			b, err := os.ReadFile(childPIDFile) //nolint:gosec
			if err != nil {
				return false
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil || pid <= 0 {
				return false
			}
			survivorPID = pid
			return isProcessRunning(survivorPID)
		}, 5*time.Second, 20*time.Millisecond)
		t.Cleanup(func() {
			if survivorPID > 0 && isProcessRunning(survivorPID) {
				_ = syscall.Kill(survivorPID, syscall.SIGKILL)
			}
		})

		c.stop()

		re.Eventually(func() bool {
			return !isProcessRunning(survivorPID)
		}, 2*time.Second, 20*time.Millisecond)

		mu.Lock()
		recorded := append([]signalCall(nil), (*calls)...)
		mu.Unlock()
		var sawGroupKill bool
		for _, call := range recorded {
			if call.pid == -pgid && call.sig == syscall.SIGKILL {
				sawGroupKill = true
			}
		}
		re.True(sawGroupKill, "expected final group SIGKILL when a member in PGID survives SIGTERM")
	})
}

func TestResolveTiUPBinAndRelativeTiUPHome(t *testing.T) {
	re := require.New(t)
	origBin := tiupBin
	t.Cleanup(func() { tiupBin = origBin })
	tiupBin = ""

	defaultHome := t.TempDir()
	t.Setenv("HOME", defaultHome)

	// Relative TIUP_HOME resolves to an absolute path and requires an executable tiup binary.
	workDir := t.TempDir()
	t.Chdir(workDir)
	t.Setenv("TIUP_HOME", ".tiup-local")

	absExpectedHome := filepath.Join(workDir, ".tiup-local")
	re.Equal(absExpectedHome, tiupHome())
	re.Equal(filepath.Join(absExpectedHome, "data", "tag1"), dataDirForTag("tag1"))

	customBin := filepath.Join(absExpectedHome, "bin", "tiup")
	re.NoError(os.MkdirAll(filepath.Dir(customBin), 0750))
	re.NoError(os.WriteFile(customBin, []byte("#!/bin/sh\n"), 0600))
	// Non-executable custom binary must fall back to default $HOME/.tiup/bin/tiup.
	re.Equal(filepath.Join(defaultHome, ".tiup", "bin", "tiup"), resolveTiUPBin())

	re.NoError(os.Chmod(customBin, 0700)) //nolint:gosec
	re.Equal(customBin, resolveTiUPBin())

	// Literal TIUP_HOME containing glob metacharacters works with os.RemoveAll.
	globHome := filepath.Join(t.TempDir(), "ci[dev")
	t.Setenv("TIUP_HOME", globHome)
	dataDir := dataDirForTag("tag_glob")
	re.NoError(os.MkdirAll(dataDir, 0750))
	re.NoError(os.RemoveAll(dataDir))
	re.False(fileExists(dataDir))
}
