// Copyright 2024 TiKV Project Authors.
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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/tests/integrations/realcluster/internal/playgroundprobe"
)

var (
	clusterTagSeq          atomic.Uint64
	playgroundInstallLocks sync.Map // canonical TiUP home key -> *sync.Mutex
)

type clusterSuite struct {
	suite.Suite

	suiteName string
	mode      string
	cluster   *cluster
	cli       pd.Client
}

// SetupSuite will run before the tests in the suite are run.
func (s *clusterSuite) SetupSuite() {
	re := s.Require()

	tag := s.tag()
	dataDir := dataDirForTag(tag)
	re.NoError(os.RemoveAll(dataDir))

	s.cluster = newCluster(re, tag, dataDir, s.mode)
	s.T().Cleanup(s.cluster.stop)
	s.cluster.start()
	s.cli = newPDClient(re, s.cluster.pdEndpoints())
}

// TearDownSuite will run after all the tests in the suite have been run.
func (s *clusterSuite) TearDownSuite() {
	if s.cli != nil {
		s.cli.Close()
	}
}

func (s *clusterSuite) tag() string {
	pid := os.Getpid()
	now := time.Now().UnixNano()
	seq := clusterTagSeq.Add(1)
	if s.mode == "ms" {
		return fmt.Sprintf("pd_real_cluster_test_ms_%s_%d_%d_%d", s.suiteName, pid, now, seq)
	}
	return fmt.Sprintf("pd_real_cluster_test_%s_%d_%d_%d", s.suiteName, pid, now, seq)
}

func tiupHome() string {
	if home := os.Getenv("TIUP_HOME"); home != "" {
		if abs, err := filepath.Abs(home); err == nil {
			return abs
		}
		return home
	}
	return filepath.Join(os.Getenv("HOME"), ".tiup")
}

func resolvedTiUPHomeEnv() string {
	if os.Getenv("TIUP_HOME") == "" {
		return ""
	}
	return tiupHome()
}

func envWithTiUPHome(absHome string) []string {
	if absHome == "" {
		return nil
	}
	base := os.Environ()
	env := make([]string, 0, len(base)+1)
	for _, kv := range base {
		if strings.HasPrefix(kv, "TIUP_HOME=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, "TIUP_HOME="+absHome)
}

func dataDirForTag(tag string) string {
	return filepath.Join(tiupHome(), "data", tag)
}

var (
	playgroundLogDir        = "/tmp/real_cluster/playground"
	tiupBin                 = ""
	realClusterRepoRoot     = ""
	clusterWaitTimeout      = 100 * time.Second
	fullClusterWaitTimeout  = 300 * time.Second
	clusterPollPeriod       = time.Second
	clusterStopGraceTimeout = 3 * time.Second
	clusterStopKillTimeout  = time.Second
	clusterStopWaitTimeout  = time.Second
	tiupProbeTimeout        = 30 * time.Second
	tiupInstallTimeout      = 3 * time.Minute
	killProcess             = syscall.Kill
	listProcessesFn         = listProcesses
	readProcStartTimeFn     = readProcStartTime
)

func resolveTiUPBin() string {
	return resolveTiUPBinForTiUPHome(resolvedTiUPHomeEnv())
}

func resolveTiUPBinForTiUPHome(absTiUPHome string) string {
	if tiupBin != "" {
		return tiupBin
	}
	if absTiUPHome != "" {
		candidate := filepath.Join(absTiUPHome, "bin", "tiup")
		if isExecutable(candidate) {
			return candidate
		}
	}
	return filepath.Join(os.Getenv("HOME"), ".tiup", "bin", "tiup")
}

const (
	defaultTiKVCount    = 3
	defaultTiDBCount    = 1
	defaultPDCount      = 3
	defaultTiFlashCount = 1
)

type cluster struct {
	re                *require.Assertions
	tag               string
	datadir           string
	tiupHome          string
	mode              string
	portOffset        int
	instances         map[string]int
	pgid              int
	launcherStartTime uint64
	cmd               *exec.Cmd
	cmdDone           chan struct{}
	trackedProcs      map[int]procInfo
	pendingExitProcs  []procInfo
}

func newCluster(re *require.Assertions, tag, datadir, mode string, ins ...map[string]int) *cluster {
	var instances map[string]int
	if len(ins) == 0 {
		instances = map[string]int{
			"kv":      defaultTiKVCount,
			"db":      defaultTiDBCount,
			"pd":      defaultPDCount,
			"tiflash": defaultTiFlashCount,
		}
	} else {
		instances = ins[0]
	}

	return &cluster{
		re:        re,
		datadir:   datadir,
		tiupHome:  resolvedTiUPHomeEnv(),
		tag:       tag,
		mode:      mode,
		instances: instances,
	}
}

func (c *cluster) effectiveTiUPHome() string {
	if c.tiupHome != "" {
		return c.tiupHome
	}
	return resolvedTiUPHomeEnv()
}

func (c *cluster) start() {
	log.Info("start to deploy a cluster", zap.String("mode", c.mode))
	c.deploy()
	c.waitReady()
}

func (c *cluster) restart() {
	log.Info("start to restart", zap.String("tag", c.tag))
	c.re.NoError(c.stopWithErr())
	c.start()
	log.Info("restart success")
}

func (c *cluster) stop() {
	if err := c.stopWithErr(); err != nil {
		log.Warn("cluster stop did not complete cleanly", zap.String("tag", c.tag), zap.Error(err))
	}
}

func (c *cluster) stopWithErr() error {
	// Once reaping has started, all owned processes were already verified exited and the
	// background cmd.Wait() may have already reaped the launcher; never use the old pgid/cmd
	// as PGID ownership proof again—only wait for c.cmdDone to finish reaping.
	if c.cmdDone != nil {
		select {
		case <-c.cmdDone:
			c.pgid = 0
			c.launcherStartTime = 0
			c.cmd = nil
			c.cmdDone = nil
			c.trackedProcs = nil
			c.pendingExitProcs = nil
			log.Info("cluster stopped", zap.String("tag", c.tag))
			return nil
		case <-time.After(clusterStopWaitTimeout):
			return fmt.Errorf("cluster %s launcher pid %d was not reaped within %s", c.tag, c.pgid, clusterStopWaitTimeout)
		}
	}
	if c.pgid == 0 && c.cmd == nil && len(c.trackedProcs) == 0 && len(c.pendingExitProcs) == 0 {
		return nil
	}

	pgid := c.pgid
	launcherStartTime := c.launcherStartTime
	cmd := c.cmd
	selfPID := os.Getpid()

	procs, err := listProcessesFn()
	if err != nil {
		log.Warn("failed to collect pids", zap.String("tag", c.tag), zap.Error(err))
	}
	tracked := make(map[int]procInfo, len(c.trackedProcs))
	for pid, info := range c.trackedProcs {
		tracked[pid] = info
	}
	owned := mergeOwnedProcs(tracked, procs, selectOwnedProcs(procs, c.datadir, pgid, launcherStartTime, selfPID))
	hasGroupMembers := hasLivePGIDMembers(procs, pgid, launcherStartTime, selfPID)
	exitTargets := make([]procInfo, 0, len(c.pendingExitProcs)+len(owned))
	exitTargets = append(exitTargets, c.pendingExitProcs...)
	exitTargets = append(exitTargets, owned...)

	if pgid > 0 && (hasGroupMembers || (err != nil && cmd != nil)) {
		_ = killProcess(-pgid, syscall.SIGTERM)
	}
	for _, p := range owned {
		// First try SIGTERM.
		_ = killProcess(p.pid, syscall.SIGTERM)
	}

	// Wait up to clusterStopGraceTimeout for graceful exit, then force kill verified survivors.
	if err != nil || len(owned) > 0 || hasGroupMembers || len(exitTargets) > 0 {
		deadline := time.Now().Add(clusterStopGraceTimeout)
		for time.Now().Before(deadline) {
			scanProcs, scanErr := listProcessesFn()
			if scanErr == nil {
				active := mergeOwnedProcs(tracked, scanProcs, selectOwnedProcs(scanProcs, c.datadir, pgid, launcherStartTime, selfPID))
				exitTargets = append(exitTargets, active...)
				if len(active) == 0 &&
					!hasLivePGIDMembers(scanProcs, pgid, launcherStartTime, selfPID) &&
					!hasLiveMatchingProcs(scanProcs, exitTargets) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Re-scan before force-killing to catch any component spawned while shutting down
	// and re-verify both PGID ownership and previously tracked process identities
	// (including children reparented to PID 1 after the launcher exited).
	procs, err = listProcessesFn()
	if err != nil {
		log.Warn("failed to refresh pids", zap.String("tag", c.tag), zap.Error(err))
	}
	survivors := mergeOwnedProcs(tracked, procs, selectOwnedProcs(procs, c.datadir, pgid, launcherStartTime, selfPID))
	hasGroupMembers = hasLivePGIDMembers(procs, pgid, launcherStartTime, selfPID)
	exitTargets = append(exitTargets, survivors...)

	if pgid > 0 && (hasGroupMembers || (err != nil && cmd != nil)) {
		// While cmd remains un-reaped, POSIX guarantees pgid cannot be reused.
		// Even without cmd, hasLivePGIDMembers requires launcherStartTime verification.
		_ = killProcess(-pgid, syscall.SIGKILL)
	}
	for _, p := range survivors {
		_ = killProcess(p.pid, syscall.SIGKILL)
	}
	pendingUnconfirmed := filterLiveMatchingProcs(procs, exitTargets)
	procsExited := err == nil && len(survivors) == 0 && len(pendingUnconfirmed) == 0 && (pgid <= 0 || !hasGroupMembers)
	var remainingProcs []procInfo
	var remainingGroup bool
	var lastPostErr error
	if !procsExited {
		killDeadline := time.Now().Add(clusterStopKillTimeout)
		for {
			postProcs, postErr := listProcessesFn()
			lastPostErr = postErr
			if postErr == nil {
				remainingProcs = mergeOwnedProcs(tracked, postProcs, selectOwnedProcs(postProcs, c.datadir, pgid, launcherStartTime, selfPID))
				remainingGroup = hasLivePGIDMembers(postProcs, pgid, launcherStartTime, selfPID)
				exitTargets = append(exitTargets, remainingProcs...)
				pendingUnconfirmed = filterLiveMatchingProcs(postProcs, exitTargets)
				if len(remainingProcs) == 0 && !remainingGroup && len(pendingUnconfirmed) == 0 {
					procsExited = true
					break
				}
			}
			if !time.Now().Before(killDeadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !procsExited {
		// Keep pgid, launcherStartTime, cmd, and live process records intact so a subsequent
		// stop() retries cleanup and restart()/deploy() cannot launch against live instances.
		if lastPostErr == nil {
			nextTracked := make(map[int]procInfo, len(remainingProcs))
			for _, p := range remainingProcs {
				if p.startTime > 0 {
					nextTracked[p.pid] = p
				}
			}
			c.trackedProcs = nextTracked
			c.pendingExitProcs = pendingUnconfirmed
		} else {
			c.trackedProcs = tracked
			c.pendingExitProcs = exitTargets
		}
		return fmt.Errorf(
			"cluster %s still has owned processes after SIGKILL timeout (survivors=%d, pendingExit=%d, livePGID=%t, scanErr=%v)",
			c.tag, len(remainingProcs), len(pendingUnconfirmed), remainingGroup, lastPostErr,
		)
	}

	// Reap the launcher only after all PGID and per-PID signals and exit waits complete,
	// keeping its PID/PGID reserved against kernel reuse throughout stop().
	if cmd != nil {
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		c.cmdDone = done
		c.trackedProcs = nil
		c.pendingExitProcs = nil
		select {
		case <-c.cmdDone:
		case <-time.After(clusterStopWaitTimeout):
			return fmt.Errorf("cluster %s launcher pid %d was not reaped within %s", c.tag, pgid, clusterStopWaitTimeout)
		}
	}
	c.pgid = 0
	c.launcherStartTime = 0
	c.cmd = nil
	c.cmdDone = nil
	c.trackedProcs = nil
	c.pendingExitProcs = nil
	log.Info("cluster stopped", zap.String("tag", c.tag))
	return nil
}

func (c *cluster) pdEndpoints() []string {
	procs, err := listProcessesFn()
	c.re.NoError(err)
	return selectPDEndpoints(procs, c.datadir, c.pgid, c.launcherStartTime, os.Getpid())
}

func (c *cluster) deploy() {
	re := c.re
	re.Zero(c.pgid, "cannot start cluster while previous instance is still tracked")
	re.Nil(c.cmd, "cannot start cluster while previous launcher is still un-reaped")
	re.Empty(c.trackedProcs, "cannot start cluster while previous owned processes remain tracked")
	re.Empty(c.pendingExitProcs, "cannot start cluster while previous owned processes have not exited")
	repoRoot := realClusterRepoRoot
	if repoRoot == "" {
		var err error
		repoRoot, err = filepath.Abs("../../..")
		re.NoError(err)
	}

	needThirdBin := !fileExists(filepath.Join(repoRoot, "third_bin/tikv-server"))
	if c.mode != "tikv-slim" {
		needThirdBin = needThirdBin ||
			!fileExists(filepath.Join(repoRoot, "third_bin/tidb-server")) ||
			!fileExists(filepath.Join(repoRoot, "third_bin/tiflash"))
	}
	if needThirdBin {
		log.Info("downloading binaries...")
		log.Info("this may take a few minutes, you can also download them manually and put them in the bin directory.")
		_, err := runCommandWithOutputInDir("./tests/integrations/realcluster/download_integration_test_binaries.sh", repoRoot)
		re.NoError(err)
	}
	if !fileExists(filepath.Join(repoRoot, "bin/pd-server")) {
		log.Info("compile pd binaries...")
		_, err := runCommandWithOutputInDir("make pd-server", repoRoot)
		re.NoError(err)
	}

	if !fileExists(playgroundLogDir) {
		re.NoError(os.MkdirAll(playgroundLogDir, 0750))
	}

	absTiUPHome := c.effectiveTiUPHome()
	c.tiupHome = absTiUPHome
	bin := resolveTiUPBinForTiUPHome(absTiUPHome)
	ensurePlaygroundInstalledWithTiUPHome(re, bin, absTiUPHome)

	logPath := filepath.Join(playgroundLogDir, c.tag+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600) //nolint:gosec
	re.NoError(err)

	playgroundArgs := buildPlaygroundArgs(c.mode, c.tag, c.portOffset, c.instances)
	cmd := exec.Command(bin, playgroundArgs...) //nolint:gosec
	cmd.Dir = repoRoot
	if env := envWithTiUPHome(absTiUPHome); len(env) > 0 {
		cmd.Env = env
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	log.Info(cmd.String())
	err = cmd.Start()
	_ = logFile.Close()
	re.NoError(err)

	// Keep cmd un-reaped until the end of stop() so POSIX guarantees PID/PGID c.pgid
	// cannot be reused by an unrelated process or process group before cleanup finishes.
	c.pgid = cmd.Process.Pid
	c.cmd = cmd
	c.cmdDone = nil
	c.trackedProcs = nil
	c.pendingExitProcs = nil
	c.launcherStartTime = readProcStartTimeFn(cmd.Process.Pid)
}

func ensurePlaygroundInstalled(re *require.Assertions, bin string) {
	ensurePlaygroundInstalledWithTiUPHome(re, bin, resolvedTiUPHomeEnv())
}

func ensurePlaygroundInstalledWithTiUPHome(re *require.Assertions, bin, absTiUPHome string) {
	if isPlaygroundBinaryUsable(probePlaygroundBinaryWithTiUPHome(bin, absTiUPHome), absTiUPHome) {
		return
	}
	unlock := lockPlaygroundInstall(re, absTiUPHome)
	defer unlock()

	binPath := probePlaygroundBinaryWithTiUPHome(bin, absTiUPHome)
	if isPlaygroundBinaryUsable(binPath, absTiUPHome) {
		return
	}
	if binPath != "" {
		cleanupIncompletePlaygroundDir(binPath, absTiUPHome)
	}
	log.Info("installing tiup playground component before starting cluster")
	ctx, cancel := context.WithTimeout(context.Background(), tiupInstallTimeout)
	defer cancel()
	output, err := runExecWithEnvOutputContext(ctx, "", envWithTiUPHome(absTiUPHome), bin, "install", "playground")
	if output != "" {
		log.Info(output)
	}
	re.NoError(err)
	installedPath := probePlaygroundBinaryWithTiUPHome(bin, absTiUPHome)
	re.Truef(isPlaygroundBinaryUsable(installedPath, absTiUPHome), "tiup playground binary %q is not runnable after install", installedPath)
}

const playgroundInstallLockFileName = ".realcluster-playground-install.lock"

func lockPlaygroundInstall(re *require.Assertions, absTiUPHome string) func() {
	lockKey := "default"
	lockPath := filepath.Join(os.TempDir(), fmt.Sprintf("pd-realcluster-tiup-playground-%d.lock", os.Geteuid()))
	if tiupHome := effectiveTiUPHome(absTiUPHome); tiupHome != "" {
		re.NoError(os.MkdirAll(tiupHome, 0750)) //nolint:gosec // tiupHome is the configured TIUP_HOME or $HOME/.tiup directory
		canonicalHome := canonicalPathForLock(tiupHome)
		lockKey = canonicalHome
		lockPath = filepath.Join(canonicalHome, playgroundInstallLockFileName)
	}
	muVal, _ := playgroundInstallLocks.LoadOrStore(lockKey, &sync.Mutex{})
	mu := muVal.(*sync.Mutex)
	mu.Lock()

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600) //nolint:gosec
	if err != nil {
		mu.Unlock()
		re.NoError(err)
	}
	if err := lockFileExclusive(int(lockFile.Fd())); err != nil {
		_ = lockFile.Close()
		mu.Unlock()
		re.NoError(err)
	}
	return func() {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		mu.Unlock()
	}
}

func lockFileExclusive(fd int) error {
	for {
		err := syscall.Flock(fd, syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func canonicalPathForLock(path string) string {
	clean := filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(resolved)
	}
	var tail []string
	cur := clean
	for {
		parent := filepath.Dir(cur)
		if parent == cur {
			return clean
		}
		tail = append(tail, filepath.Base(cur))
		cur = parent
		if resolved, err := filepath.EvalSymlinks(cur); err == nil {
			slices.Reverse(tail)
			parts := make([]string, 0, len(tail)+1)
			parts = append(parts, filepath.Clean(resolved))
			parts = append(parts, tail...)
			return filepath.Join(parts...)
		}
	}
}

func isPlaygroundInstalled(bin string) bool {
	return isPlaygroundInstalledWithTiUPHome(bin, resolvedTiUPHomeEnv())
}

func isPlaygroundInstalledWithTiUPHome(bin, absTiUPHome string) bool {
	return isPlaygroundBinaryUsable(probePlaygroundBinaryWithTiUPHome(bin, absTiUPHome), absTiUPHome)
}

func isPlaygroundBinaryUsable(binPath, absTiUPHome string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), tiupProbeTimeout)
	defer cancel()
	return playgroundprobe.Check(ctx, binPath, envWithTiUPHome(absTiUPHome))
}

func probePlaygroundBinaryWithTiUPHome(bin, absTiUPHome string) string {
	ctx, cancel := context.WithTimeout(context.Background(), tiupProbeTimeout)
	defer cancel()
	output, err := runExecWithEnvOutputContext(ctx, "", envWithTiUPHome(absTiUPHome), bin, "--binary", "playground")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(output)
}

func effectiveTiUPHome(absTiUPHome string) string {
	if absTiUPHome != "" {
		return filepath.Clean(absTiUPHome)
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Clean(filepath.Join(home, ".tiup"))
}

func cleanupIncompletePlaygroundDir(binPath, absTiUPHome string) {
	tiupHome := effectiveTiUPHome(absTiUPHome)
	if binPath == "" || tiupHome == "" {
		return
	}
	verDir := filepath.Dir(filepath.Clean(binPath))
	playgroundRoot := filepath.Clean(filepath.Join(tiupHome, "components", "playground"))
	parentDir := filepath.Clean(filepath.Dir(verDir))
	if evalParent, err := filepath.EvalSymlinks(parentDir); err == nil {
		parentDir = filepath.Clean(evalParent)
	}
	if evalRoot, err := filepath.EvalSymlinks(playgroundRoot); err == nil {
		playgroundRoot = filepath.Clean(evalRoot)
	}
	if parentDir != playgroundRoot || filepath.Clean(verDir) == playgroundRoot {
		return
	}
	log.Warn("removing incomplete tiup playground version directory before reinstall",
		zap.String("dir", verDir), zap.String("binary", binPath))
	_ = os.RemoveAll(verDir) //nolint:gosec // verDir is verified above to be a direct child of playgroundRoot
}

func buildPlaygroundArgs(mode, tag string, portOffset int, instances map[string]int) []string {
	kvCount, exist := instances["kv"]
	if !exist {
		kvCount = defaultTiKVCount
	}
	dbCount, exist := instances["db"]
	if !exist {
		dbCount = defaultTiDBCount
	}
	pdCount, exist := instances["pd"]
	if !exist {
		pdCount = defaultPDCount
	}
	tiflashCount, exist := instances["tiflash"]
	if !exist {
		tiflashCount = defaultTiFlashCount
	}

	var playgroundOpts []string
	if mode == "tikv-slim" {
		playgroundOpts = []string{
			"playground",
			"nightly",
			"--mode", "tikv-slim",
			"--kv", strconv.Itoa(kvCount),
			"--pd", strconv.Itoa(pdCount),
			"--without-monitor",
			"--tag", tag,
		}
	} else {
		playgroundOpts = []string{
			"playground",
			"nightly",
			"--kv", strconv.Itoa(kvCount),
			"--tiflash", strconv.Itoa(tiflashCount),
			"--db", strconv.Itoa(dbCount),
			"--pd", strconv.Itoa(pdCount),
			"--without-monitor",
			"--tag", tag,
		}
	}

	if portOffset > 0 {
		playgroundOpts = append(playgroundOpts, "--port-offset", strconv.Itoa(portOffset))
	}

	if mode == "ms" {
		playgroundOpts = append(playgroundOpts,
			"--pd.mode", "ms",
			"--tso", "1",
			"--scheduling", "1",
		)
	}

	playgroundOpts = append(playgroundOpts, buildBinPathsOpts(mode)...)
	return playgroundOpts
}

func selectOwnedProcs(procs []procInfo, dataDir string, targetPGID int, launcherStartTime uint64, selfPID int) []procInfo {
	ownedTree := collectOwnedTreePIDs(procs, targetPGID, launcherStartTime, selfPID)
	var cleanDataDir string
	if dataDir != "" {
		cleanDataDir = filepath.Clean(dataDir)
	}

	var owned []procInfo
	seen := make(map[int]struct{})
	for _, p := range procs {
		if p.pid <= 0 || p.pid == selfPID || p.state == 'Z' || p.state == 'X' {
			continue
		}
		_, inTree := ownedTree[p.pid]
		if !inTree && !matchesEscapedClusterProcess(p, cleanDataDir) {
			continue
		}
		if _, ok := seen[p.pid]; !ok {
			seen[p.pid] = struct{}{}
			owned = append(owned, p)
		}
	}
	return owned
}

func matchesEscapedClusterProcess(p procInfo, cleanDataDir string) bool {
	if cleanDataDir == "" {
		return false
	}
	binName, procArgs := procBinaryAndArgs(p.args)
	if !isClusterServerBinary(binName) {
		return false
	}
	return matchesClusterServerDataDir(binName, procArgs, cleanDataDir)
}

func (c *cluster) waitTimeout() time.Duration {
	if c.mode == "tikv-slim" {
		return clusterWaitTimeout
	}
	return fullClusterWaitTimeout
}

func (c *cluster) waitReady() {
	re := c.re
	timeout := c.waitTimeout()
	log.Info("start to wait TiUP ready", zap.String("tag", c.tag), zap.Duration("timeout", timeout))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	absTiUPHome := c.effectiveTiUPHome()
	bin := resolveTiUPBinForTiUPHome(absTiUPHome)
	env := envWithTiUPHome(absTiUPHome)
	err := waitForClusterReady(ctx, clusterPollPeriod, func(checkCtx context.Context) (string, error) {
		log.Info("check TiUP ready", zap.String("tag", c.tag))
		return runExecWithEnvOutputContext(checkCtx, "", env, bin, "playground", "display", "--tag", c.tag)
	})
	if err != nil {
		if logBytes, readErr := os.ReadFile(filepath.Join(playgroundLogDir, c.tag+".log")); readErr == nil && len(logBytes) > 0 {
			log.Warn("TiUP playground log on timeout", zap.String("tag", c.tag), zap.ByteString("log", logBytes))
		}
		re.FailNowf("TiUP is not ready after timeout", "tag: %s, err: %v", c.tag, err)
	}
	log.Info("TiUP is ready", zap.String("tag", c.tag))
}

func waitForClusterReady(ctx context.Context, pollInterval time.Duration, check func(context.Context) (string, error)) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w (last check error: %w)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
			if ctx.Err() != nil {
				if lastErr != nil {
					return fmt.Errorf("%w (last check error: %w)", ctx.Err(), lastErr)
				}
				return ctx.Err()
			}
			output, err := check(ctx)
			if err == nil {
				return nil
			}
			lastErr = err
			if output != "" {
				log.Info(output)
			}
			log.Info("TiUP is not ready, will retry", zap.Error(err))
		}
	}
}

func buildBinPathsOpts(mode string) []string {
	if mode == "tikv-slim" {
		return []string{
			"--pd.binpath", "./bin/pd-server",
			"--kv.binpath", "./third_bin/tikv-server",
		}
	}

	opts := []string{
		"--pd.binpath", "./bin/pd-server",
		"--kv.binpath", "./third_bin/tikv-server",
		"--db.binpath", "./third_bin/tidb-server",
		"--tiflash.binpath", "./third_bin/tiflash",
	}

	if mode == "ms" {
		opts = append(opts,
			"--tso.binpath", "./bin/pd-server",
			"--scheduling.binpath", "./bin/pd-server",
		)
	}

	return opts
}
