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
	"debug/elf"
	"debug/macho"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	c.stop()
	c.start()
	log.Info("restart success")
}

func (c *cluster) stop() {
	if c.pgid == 0 && c.cmd == nil {
		return
	}

	pgid := c.pgid
	launcherStartTime := c.launcherStartTime
	cmd := c.cmd
	selfPID := os.Getpid()

	procs, err := listProcessesFn()
	if err != nil {
		log.Warn("failed to collect pids", zap.String("tag", c.tag), zap.Error(err))
	}
	tracked := make(map[int]procInfo)
	owned := mergeOwnedProcs(tracked, procs, selectOwnedProcs(procs, c.datadir, pgid, launcherStartTime, selfPID))
	hasGroupMembers := hasLivePGIDMembers(procs, pgid, launcherStartTime, selfPID)

	if pgid > 0 && (hasGroupMembers || (err != nil && cmd != nil)) {
		_ = killProcess(-pgid, syscall.SIGTERM)
	}
	for _, p := range owned {
		// First try SIGTERM.
		_ = killProcess(p.pid, syscall.SIGTERM)
	}

	// Wait up to clusterStopGraceTimeout for graceful exit, then force kill verified survivors.
	if err != nil || len(owned) > 0 || hasGroupMembers {
		deadline := time.Now().Add(clusterStopGraceTimeout)
		for time.Now().Before(deadline) {
			scanProcs, scanErr := listProcessesFn()
			if scanErr == nil {
				active := mergeOwnedProcs(tracked, scanProcs, selectOwnedProcs(scanProcs, c.datadir, pgid, launcherStartTime, selfPID))
				if len(active) == 0 && !hasLivePGIDMembers(scanProcs, pgid, launcherStartTime, selfPID) {
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

	if pgid > 0 && (hasGroupMembers || (err != nil && cmd != nil)) {
		// While cmd remains un-reaped, POSIX guarantees pgid cannot be reused.
		// Even without cmd, hasLivePGIDMembers requires launcherStartTime verification.
		_ = killProcess(-pgid, syscall.SIGKILL)
	}
	for _, p := range survivors {
		_ = killProcess(p.pid, syscall.SIGKILL)
	}
	if len(survivors) > 0 || (pgid > 0 && hasGroupMembers) {
		killDeadline := time.Now().Add(time.Second)
		for time.Now().Before(killDeadline) {
			postProcs, postErr := listProcessesFn()
			if postErr == nil &&
				!hasLiveMatchingProcs(postProcs, survivors) &&
				!hasLivePGIDMembers(postProcs, pgid, launcherStartTime, selfPID) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Reap the launcher only after all PGID and per-PID signals and exit waits complete,
	// keeping its PID/PGID reserved against kernel reuse throughout stop().
	if cmd != nil {
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	c.pgid = 0
	c.launcherStartTime = 0
	c.cmd = nil
	log.Info("cluster stopped", zap.String("tag", c.tag))
}

func (c *cluster) pdEndpoints() []string {
	procs, err := listProcessesFn()
	c.re.NoError(err)
	return selectPDEndpoints(procs, c.datadir, c.pgid, c.launcherStartTime, os.Getpid())
}

func (c *cluster) deploy() {
	re := c.re
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
	if !isExecutable(binPath) {
		return false
	}
	info, err := os.Stat(binPath) //nolint:gosec
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return false
	}
	if !hasCompleteBinaryLayout(binPath, info.Size()) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), tiupProbeTimeout)
	defer cancel()
	_, err = runExecWithEnvOutputContext(ctx, "", envWithTiUPHome(absTiUPHome), binPath, "--version")
	return err == nil
}

func hasCompleteBinaryLayout(path string, fileSize int64) bool {
	if fileSize <= 0 {
		return false
	}
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	}
	uSize := uint64(fileSize)

	switch {
	case magic == [4]byte{0x7f, 'E', 'L', 'F'}:
		ef, err := elf.NewFile(f)
		if err != nil {
			return false
		}
		return hasCompleteELFLayout(ef, uSize)
	case isMachOMagic(magic):
		mf, err := macho.NewFile(f)
		if err != nil {
			return false
		}
		return hasCompleteMachOLayout(mf, uSize)
	case isMachOFatMagic(magic):
		ff, err := macho.NewFatFile(f)
		if err != nil {
			return false
		}
		for _, arch := range ff.Arches {
			if !fitsInSize(uint64(arch.Offset), uint64(arch.Size), uSize) || !hasCompleteMachOLayout(arch.File, uint64(arch.Size)) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func hasCompleteELFLayout(ef *elf.File, fileSize uint64) bool {
	for _, p := range ef.Progs {
		if p.Filesz > 0 && !fitsInSize(p.Off, p.Filesz, fileSize) {
			return false
		}
	}
	for _, s := range ef.Sections {
		// Use FileSize (on-disk byte length) rather than Size, because Size is
		// the uncompressed length for SHF_COMPRESSED sections (e.g. Go DWARF debug info).
		if s.Type != elf.SHT_NOBITS && s.FileSize > 0 && !fitsInSize(s.Offset, s.FileSize, fileSize) {
			return false
		}
	}
	return true
}

func hasCompleteMachOLayout(mf *macho.File, fileSize uint64) bool {
	const (
		machoSectionTypeMask   = 0xff
		machoSectionZeroFill   = 0x1
		machoSectionGBZeroFill = 0xc
		machoSectionThreadZero = 0x12
	)
	for _, load := range mf.Loads {
		seg, ok := load.(*macho.Segment)
		if !ok || seg.Filesz == 0 {
			continue
		}
		if !fitsInSize(seg.Offset, seg.Filesz, fileSize) {
			return false
		}
	}
	for _, sec := range mf.Sections {
		secType := sec.Flags & machoSectionTypeMask
		if sec.Size == 0 || secType == machoSectionZeroFill || secType == machoSectionGBZeroFill || secType == machoSectionThreadZero {
			continue
		}
		if !fitsInSize(uint64(sec.Offset), sec.Size, fileSize) {
			return false
		}
	}
	return true
}

func fitsInSize(offset, length, total uint64) bool {
	return offset <= total && length <= total-offset
}

func isMachOMagic(magic [4]byte) bool {
	be := binary.BigEndian.Uint32(magic[:])
	le := binary.LittleEndian.Uint32(magic[:])
	return be == macho.Magic32 || be == macho.Magic64 || le == macho.Magic32 || le == macho.Magic64
}

func isMachOFatMagic(magic [4]byte) bool {
	return binary.BigEndian.Uint32(magic[:]) == macho.MagicFat
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
