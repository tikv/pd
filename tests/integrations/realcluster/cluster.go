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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
)

var clusterTagSeq atomic.Uint64

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
)

func resolveTiUPBin() string {
	if tiupBin != "" {
		return tiupBin
	}
	if os.Getenv("TIUP_HOME") != "" {
		candidate := filepath.Join(tiupHome(), "bin", "tiup")
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
	re         *require.Assertions
	tag        string
	datadir    string
	mode       string
	portOffset int
	instances  map[string]int
	pgid       int
	waitDone   chan struct{}
	pids       []int
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

	return &cluster{re: re, datadir: datadir, tag: tag, mode: mode, instances: instances}
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

func isWaitDone(waitDone <-chan struct{}) bool {
	if waitDone == nil {
		return true
	}
	select {
	case <-waitDone:
		return true
	default:
		return false
	}
}

func (c *cluster) stop() {
	if c.pgid == 0 && c.waitDone == nil {
		return
	}

	pgid := c.pgid
	waitDone := c.waitDone
	hasGroupMembers, err := c.collectPids()
	if err != nil {
		log.Warn("failed to collect pids", zap.String("tag", c.tag), zap.Error(err))
	}

	if pgid > 0 && (!isWaitDone(waitDone) || hasGroupMembers) {
		_ = killProcess(-pgid, syscall.SIGTERM)
	}
	for _, pid := range c.pids {
		// First try SIGTERM
		_ = killProcess(pid, syscall.SIGTERM)
	}

	// Wait up to clusterStopGraceTimeout for graceful exit, then force kill survivors.
	deadline := time.Now().Add(clusterStopGraceTimeout)
	for time.Now().Before(deadline) {
		if isWaitDone(waitDone) && !slices.ContainsFunc(c.pids, isProcessRunning) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Re-scan before force-killing to catch any component spawned while shutting down
	// and verify whether the process group still has live members.
	hasGroupMembers, err = c.collectPids()
	if err != nil {
		log.Warn("failed to refresh pids", zap.String("tag", c.tag), zap.Error(err))
	}
	if pgid > 0 && (!isWaitDone(waitDone) || hasGroupMembers) {
		// Only signal -pgid while the launcher is un-reaped or live members remain in
		// the group, avoiding signaling a released PGID after clean shutdown. When the
		// launcher was already reaped, a narrow check-to-signal window remains if the
		// last group member exits between the process scan and killProcess.
		_ = killProcess(-pgid, syscall.SIGKILL)
	}
	var killed []int
	for _, pid := range c.pids {
		if isProcessRunning(pid) {
			_ = killProcess(pid, syscall.SIGKILL)
			killed = append(killed, pid)
		}
	}
	if len(killed) > 0 {
		killDeadline := time.Now().Add(time.Second)
		for time.Now().Before(killDeadline) && slices.ContainsFunc(killed, isProcessRunning) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if waitDone != nil {
		select {
		case <-waitDone:
		case <-time.After(time.Second):
		}
	}
	c.pgid = 0
	c.waitDone = nil
	c.pids = nil
	log.Info("cluster stopped", zap.String("tag", c.tag))
}

func (c *cluster) pdEndpoints() []string {
	procs, err := listProcesses()
	c.re.NoError(err)
	return selectPDEndpoints(procs, c.datadir, c.pgid, os.Getpid())
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

	bin := resolveTiUPBin()
	ensurePlaygroundInstalled(re, bin)

	logPath := filepath.Join(playgroundLogDir, c.tag+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600) //nolint:gosec
	re.NoError(err)

	playgroundArgs := buildPlaygroundArgs(c.mode, c.tag, c.portOffset, c.instances)
	cmd := exec.Command(bin, playgroundArgs...) //nolint:gosec
	cmd.Dir = repoRoot
	if os.Getenv("TIUP_HOME") != "" {
		cmd.Env = append(os.Environ(), "TIUP_HOME="+tiupHome())
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	log.Info(cmd.String())
	err = cmd.Start()
	_ = logFile.Close()
	re.NoError(err)

	c.pgid = cmd.Process.Pid
	waitDone := make(chan struct{})
	c.waitDone = waitDone
	go func() {
		_ = cmd.Wait()
		close(waitDone)
	}()
}

func ensurePlaygroundInstalled(re *require.Assertions, bin string) {
	if isPlaygroundInstalled(bin) {
		return
	}
	log.Info("installing tiup playground component before starting cluster")
	ctx, cancel := context.WithTimeout(context.Background(), tiupInstallTimeout)
	defer cancel()
	output, err := runExecWithOutputContext(ctx, "", bin, "install", "playground")
	if output != "" {
		log.Info(output)
	}
	re.NoError(err)
}

func isPlaygroundInstalled(bin string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), tiupProbeTimeout)
	defer cancel()
	output, err := runExecWithOutputContext(ctx, "", bin, "--binary", "playground")
	return err == nil && strings.TrimSpace(output) != ""
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

// collectPids collects PIDs owned by this cluster's process tree or cluster data directory,
// and reports whether any live process still belongs to c.pgid.
func (c *cluster) collectPids() (bool, error) {
	procs, err := listProcesses()
	if err != nil {
		return false, fmt.Errorf("failed to collect pids: %w", err)
	}
	selfPID := os.Getpid()
	for _, pid := range selectOwnedPIDs(procs, c.datadir, c.pgid, selfPID) {
		if !slices.Contains(c.pids, pid) {
			c.pids = append(c.pids, pid)
		}
	}
	return hasLivePGIDMembers(procs, c.pgid, selfPID), nil
}

func selectOwnedPIDs(procs []procInfo, dataDir string, targetPGID, selfPID int) []int {
	ownedTree := collectOwnedTreePIDs(procs, targetPGID, selfPID)
	var cleanDataDir string
	if dataDir != "" {
		cleanDataDir = filepath.Clean(dataDir)
	}

	var pids []int
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
			pids = append(pids, p.pid)
		}
	}
	return pids
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

	bin := resolveTiUPBin()
	err := waitForClusterReady(ctx, clusterPollPeriod, func(checkCtx context.Context) (string, error) {
		log.Info("check TiUP ready", zap.String("tag", c.tag))
		return runExecWithOutputContext(checkCtx, "", bin, "playground", "display", "--tag", c.tag)
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
