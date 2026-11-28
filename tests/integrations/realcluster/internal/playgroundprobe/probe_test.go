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

package playgroundprobe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const nsLastPIDPath = "/proc/sys/kernel/ns_last_pid"

// writeLookupScript writes a fake tiup whose leader records its PID and the PID
// of a background child started with childCmd, then exits while the child keeps
// the lookup's stdout open.
func writeLookupScript(t *testing.T, childCmd string) (tiup, leaderFile, childFile string) {
	t.Helper()
	dir := t.TempDir()
	leaderFile, childFile = filepath.Join(dir, "leader"), filepath.Join(dir, "child")
	script := fmt.Sprintf("#!/bin/sh\necho $$ > %q\n%s &\necho $! > %q\nexit 0\n", leaderFile, childCmd, childFile)
	tiup = filepath.Join(dir, "tiup")
	require.NoError(t, os.WriteFile(tiup, []byte(script), 0700)) //nolint:gosec
	return tiup, leaderFile, childFile
}

func readPIDs(leaderFile, childFile string) (leader, child int, ok bool) {
	a, err1 := os.ReadFile(leaderFile) //nolint:gosec
	b, err2 := os.ReadFile(childFile)  //nolint:gosec
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	leader, err1 = strconv.Atoi(strings.TrimSpace(string(a)))
	child, err2 = strconv.Atoi(strings.TrimSpace(string(b)))
	return leader, child, err1 == nil && err2 == nil && leader > 1 && child > 1
}

func procState(pid int) (byte, bool) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	// The state follows the parenthesized command name, which may contain spaces.
	i := strings.LastIndexByte(string(stat), ')')
	if i < 0 || i+2 >= len(stat) {
		return 0, false
	}
	return stat[i+2], true
}

// TestLookupBinaryKeepsLeaderUnreapedUntilGroupKill verifies that a timed-out
// lookup kills its process group while the exited leader is still an unreaped
// zombie, which keeps the PGID from being reused, and reaps it only afterwards.
func TestLookupBinaryKeepsLeaderUnreapedUntilGroupKill(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("waiting for exit without reaping requires Linux waitid")
	}
	re := require.New(t)
	tiup, leaderFile, childFile := writeLookupScript(t, "sleep 30")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := LookupBinary(ctx, tiup, nil)
		done <- err
	}()

	var leader, child int
	re.Eventually(func() bool {
		var ok bool
		leader, child, ok = readPIDs(leaderFile, childFile)
		return ok
	}, time.Second, 5*time.Millisecond)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	re.Eventually(func() bool {
		state, ok := procState(leader)
		return ok && state == 'Z'
	}, time.Second, 5*time.Millisecond, "exited lookup leader must stay unreaped while the lookup waits on its output")

	re.ErrorIs(<-done, context.DeadlineExceeded)
	_, err := os.Stat(fmt.Sprintf("/proc/%d", leader))
	re.True(os.IsNotExist(err), "lookup leader must be reaped before LookupBinary returns")
	re.Eventually(func() bool {
		state, ok := procState(child)
		return !ok || state == 'Z'
	}, time.Second, 5*time.Millisecond, "group kill must reach the child holding the output pipe")
}

// TestLookupBinaryTimeoutDoesNotKillReusedPGID reproduces PID reuse of a reaped
// lookup leader by an unrelated process group leader. It changes ns_last_pid, so
// it runs only in an isolated PID namespace, for example:
//
//	unshare -Urpf --mount-proc env PD_TEST_ISOLATED_PID_NS=1 go test -run TestLookupBinaryTimeoutDoesNotKillReusedPGID
func TestLookupBinaryTimeoutDoesNotKillReusedPGID(t *testing.T) {
	if os.Getenv("PD_TEST_ISOLATED_PID_NS") != "1" {
		t.Skip("requires PD_TEST_ISOLATED_PID_NS=1 in an isolated PID namespace")
	}
	if f, err := os.OpenFile(nsLastPIDPath, os.O_WRONLY, 0); err != nil {
		t.Skipf("%s is not writable: %v", nsLastPIDPath, err)
	} else {
		_ = f.Close()
	}
	re := require.New(t)
	// The child leaves the lookup's process group but keeps its stdout open.
	tiup, leaderFile, childFile := writeLookupScript(t, "setsid sleep 30")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := LookupBinary(ctx, tiup, nil)
		done <- err
	}()

	var leader, child int
	re.Eventually(func() bool {
		var ok bool
		leader, child, ok = readPIDs(leaderFile, childFile)
		return ok
	}, time.Second, 5*time.Millisecond)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	re.Eventually(func() bool {
		pgid, err := syscall.Getpgid(child)
		return err == nil && pgid == child
	}, time.Second, 5*time.Millisecond)
	re.Eventually(func() bool {
		state, ok := procState(leader)
		return !ok || state == 'Z'
	}, time.Second, 5*time.Millisecond)

	// Try to start an unrelated process group leader with the lookup leader's PID.
	re.NoError(os.WriteFile(nsLastPIDPath, []byte(strconv.Itoa(leader-1)), 0600))
	victim := exec.Command("sleep", "30")
	victim.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	re.NoError(victim.Start())
	t.Cleanup(func() {
		_ = victim.Process.Kill()
		_ = victim.Wait()
	})
	if victim.Process.Pid != leader {
		// The leader's PID is still reserved, so its group cannot be reused.
		_, err := os.Stat(fmt.Sprintf("/proc/%d", leader))
		re.NoError(err, "lookup leader PID %d was neither reserved nor reused (victim got %d)", leader, victim.Process.Pid)
	}

	re.ErrorIs(<-done, context.DeadlineExceeded)
	re.Never(func() bool {
		state, ok := procState(victim.Process.Pid)
		return !ok || state == 'Z'
	}, 200*time.Millisecond, 10*time.Millisecond, "timed-out lookup killed unrelated process group %d", victim.Process.Pid)
}
