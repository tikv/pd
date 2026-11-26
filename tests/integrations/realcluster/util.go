// Copyright 2023 TiKV Project Authors.
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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

const physicalShiftBits = 18

// GetTimeFromTS extracts time.Time from a timestamp.
func GetTimeFromTS(ts uint64) time.Time {
	ms := ExtractPhysical(ts)
	return time.Unix(ms/1e3, (ms%1e3)*1e6)
}

// ExtractPhysical returns a ts's physical part.
func ExtractPhysical(ts uint64) int64 {
	return int64(ts >> physicalShiftBits)
}

func runCommandWithOutput(cmdStr string) (string, error) {
	return runCommandWithOutputInDir(cmdStr, "")
}

func runCommandWithOutputInDir(cmdStr, dir string) (string, error) {
	return runCommandWithOutputContext(context.Background(), cmdStr, dir)
}

func runCommandWithOutputContext(ctx context.Context, cmdStr, dir string) (string, error) {
	return runExecWithOutputContext(ctx, dir, "sh", "-c", cmdStr)
}

func runExecWithOutputContext(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec
	cmd.Dir = dir
	if ctx.Done() != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error {
			if cmd.Process == nil {
				return nil
			}
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		cmd.WaitDelay = 200 * time.Millisecond
	}
	log.Info(cmd.String())
	out, err := cmd.Output()
	if err != nil {
		return string(out), err
	}
	return string(out), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path) //nolint:gosec
	return !os.IsNotExist(err)
}

func isExecutable(path string) bool {
	info, err := os.Stat(path) //nolint:gosec
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err = process.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	// On Linux, kill(pid, 0) still succeeds for un-reaped zombie processes.
	if stat, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); readErr == nil {
		if state, ok := parseProcState(stat); ok && (state == 'Z' || state == 'X') {
			return false
		}
	}
	return true
}

type procInfo struct {
	pid   int
	ppid  int
	pgid  int
	state byte
	args  []string
}

func parseProcState(stat []byte) (byte, bool) {
	state, _, _, ok := parseProcStat(stat)
	return state, ok
}

func parseProcStat(stat []byte) (state byte, ppid, pgid int, ok bool) {
	idx := bytes.LastIndexByte(stat, ')')
	if idx < 0 || idx+2 >= len(stat) {
		return 0, 0, 0, false
	}
	fields := strings.Fields(string(stat[idx+1:]))
	if len(fields) < 3 || len(fields[0]) != 1 {
		return 0, 0, 0, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, 0, false
	}
	pgid, err = strconv.Atoi(fields[2])
	if err != nil {
		return 0, 0, 0, false
	}
	return fields[0][0], ppid, pgid, true
}

func parseProcCmdline(raw []byte) []string {
	raw = bytes.TrimRight(raw, "\x00")
	if len(raw) == 0 {
		return nil
	}
	parts := bytes.Split(raw, []byte{0})
	args := make([]string, 0, len(parts))
	for _, part := range parts {
		args = append(args, string(part))
	}
	return args
}

func procBinaryAndArgs(args []string) (bin string, rest []string) {
	if len(args) == 0 {
		return "", nil
	}
	base := filepath.Base(args[0])
	switch base {
	case "sh", "bash", "dash":
		if len(args) >= 2 && !strings.HasPrefix(args[1], "-") && isClusterServerBinary(args[1]) {
			return filepath.Base(args[1]), args[2:]
		}
	}
	return base, args[1:]
}

func isClusterServerBinary(arg0 string) bool {
	switch filepath.Base(arg0) {
	case "pd-server", "tikv-server", "tidb-server", "tiflash":
		return true
	default:
		return false
	}
}

func listProcesses() ([]procInfo, error) {
	procs, err := readProcessesFromProc("/proc")
	if err == nil || !os.IsNotExist(err) {
		return procs, err
	}
	return listProcessesFromPS()
}

func listProcessesFromPS() ([]procInfo, error) {
	output, err := runExecWithOutputContext(context.Background(), "", "ps", "-eo", "pid=,ppid=,pgid=,stat=,args=")
	if err != nil {
		return nil, err
	}
	return parsePSOutput(output), nil
}

func parsePSOutput(output string) []procInfo {
	var procs []procInfo
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 || len(fields[3]) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 {
			continue
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		pgid, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		procs = append(procs, procInfo{
			pid:   pid,
			ppid:  ppid,
			pgid:  pgid,
			state: fields[3][0],
			args:  fields[4:],
		})
	}
	return procs
}

func readProcessesFromProc(procRoot string) ([]procInfo, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, err
	}
	procs := make([]procInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		procDir := filepath.Join(procRoot, entry.Name())
		statBytes, err := os.ReadFile(filepath.Join(procDir, "stat")) //nolint:gosec
		if err != nil {
			continue
		}
		state, ppid, pgid, ok := parseProcStat(statBytes)
		if !ok {
			continue
		}
		cmdlineBytes, err := os.ReadFile(filepath.Join(procDir, "cmdline")) //nolint:gosec
		if err != nil {
			continue
		}
		args := parseProcCmdline(cmdlineBytes)
		if len(args) == 0 {
			continue
		}
		procs = append(procs, procInfo{
			pid:   pid,
			ppid:  ppid,
			pgid:  pgid,
			state: state,
			args:  args,
		})
	}
	return procs, nil
}

func hasLivePGIDMembers(procs []procInfo, targetPGID, selfPID int) bool {
	if targetPGID <= 0 {
		return false
	}
	for _, p := range procs {
		if p.pid <= 0 || p.pid == selfPID || p.state == 'Z' || p.state == 'X' {
			continue
		}
		if p.pgid == targetPGID {
			return true
		}
	}
	return false
}

func collectOwnedTreePIDs(procs []procInfo, targetPGID, selfPID int) map[int]struct{} {
	owned := make(map[int]struct{})
	if targetPGID <= 0 {
		return owned
	}
	childrenByPPID := make(map[int][]int, len(procs))
	existing := make(map[int]struct{}, len(procs))
	var queue []int
	queued := make(map[int]struct{})

	for _, p := range procs {
		if p.pid <= 0 || p.pid == selfPID {
			continue
		}
		if p.state == 'Z' || p.state == 'X' {
			// An un-reaped zombie still in targetPGID cannot have had its PID reused;
			// traverse its live descendants without adding the zombie itself to owned.
			if p.pgid == targetPGID {
				if _, ok := queued[p.pid]; !ok {
					queued[p.pid] = struct{}{}
					queue = append(queue, p.pid)
				}
			}
			continue
		}
		existing[p.pid] = struct{}{}
		childrenByPPID[p.ppid] = append(childrenByPPID[p.ppid], p.pid)
		if p.pgid == targetPGID {
			if _, ok := queued[p.pid]; !ok {
				queued[p.pid] = struct{}{}
				queue = append(queue, p.pid)
			}
		}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == selfPID {
			continue
		}
		if _, exists := existing[cur]; exists {
			owned[cur] = struct{}{}
		}
		for _, child := range childrenByPPID[cur] {
			if _, ok := queued[child]; !ok {
				queued[child] = struct{}{}
				queue = append(queue, child)
			}
		}
	}
	return owned
}

func matchesClusterServerDataDir(binName string, args []string, cleanDataDir string) bool {
	if cleanDataDir == "" {
		return false
	}
	var flags []string
	switch filepath.Base(binName) {
	case "pd-server", "tikv-server":
		flags = []string{"--data-dir"}
	case "tidb-server":
		flags = []string{"--log-file", "--temp-dir"}
	case "tiflash":
		flags = []string{"--config-file"}
	default:
		return false
	}
	for _, val := range extractFlagValues(args, flags...) {
		if pathWithinDataDir(val, cleanDataDir) {
			return true
		}
	}
	return false
}

func extractFlagValues(args []string, flags ...string) []string {
	var vals []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		for _, flag := range flags {
			if val, ok := strings.CutPrefix(arg, flag+"="); ok {
				if val != "" {
					vals = append(vals, val)
				}
				break
			}
			if arg == flag && i+1 < len(args) {
				if args[i+1] != "" {
					vals = append(vals, args[i+1])
				}
				i++
				break
			}
		}
	}
	return vals
}

func pathWithinDataDir(candidate, cleanDataDir string) bool {
	if candidate == "" || cleanDataDir == "" {
		return false
	}
	cleanCandidate := filepath.Clean(candidate)
	if cleanCandidate == cleanDataDir {
		return true
	}
	prefix := cleanDataDir
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(cleanCandidate, prefix)
}

func newPDClient(re *require.Assertions, pdEndpoints []string) pd.Client {
	cli, err := pd.NewClientWithContext(
		context.Background(), caller.TestComponent, pdEndpoints,
		pd.SecurityOption{}, opt.WithMaxErrorRetry(1),
	)
	re.NoError(err)
	return cli
}

func selectPDEndpoints(procs []procInfo, dataDir string, targetPGID, selfPID int) []string {
	var cleanDataDir string
	if dataDir != "" {
		cleanDataDir = filepath.Clean(dataDir)
	}
	scoped := cleanDataDir != "" || targetPGID > 0
	var ownedTree map[int]struct{}
	if targetPGID > 0 {
		ownedTree = collectOwnedTreePIDs(procs, targetPGID, selfPID)
	}

	var pdAddrs []string
	for _, p := range procs {
		if p.pid <= 0 || p.pid == selfPID || p.state == 'Z' || p.state == 'X' || len(p.args) == 0 {
			continue
		}
		binName, procArgs := procBinaryAndArgs(p.args)
		if binName != "tikv-server" {
			continue
		}
		if scoped {
			_, inTree := ownedTree[p.pid]
			if !inTree && !matchesClusterServerDataDir(binName, procArgs, cleanDataDir) {
				continue
			}
		}
		pdAddrs = append(pdAddrs, extractPDEndpointsFromArgs(procArgs)...)
	}
	return removeDuplicates(pdAddrs)
}

func extractPDEndpointsFromArgs(args []string) []string {
	const flag = "--pd-endpoints"
	const flagPrefix = flag + "="
	var pdAddrs []string
	for i := 0; i < len(args); i++ {
		var addrList string
		if val, ok := strings.CutPrefix(args[i], flagPrefix); ok {
			addrList = val
		} else if args[i] == flag && i+1 < len(args) {
			addrList = args[i+1]
			i++
		} else {
			continue
		}
		for addr := range strings.SplitSeq(addrList, ",") {
			// length of addr is less than 5 means it must not be a valid address
			if len(addr) < 5 {
				continue
			}
			pdAddrs = append(pdAddrs, addr)
		}
	}
	return pdAddrs
}

func removeDuplicates(arr []string) []string {
	uniqueMap := make(map[string]bool)
	var result []string

	for _, item := range arr {
		if _, exists := uniqueMap[item]; !exists {
			uniqueMap[item] = true
			result = append(result, item)
		}
	}

	return result
}
