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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

type clusterSuite struct {
	suite.Suite

	clusterCnt int
	suiteName  string
	ms         bool
}

var (
	playgroundLogDir = "/tmp/real_cluster/playground"
	tiupBin          = os.Getenv("HOME") + "/.tiup/bin/tiup"
)

const (
	defaultTiKVCount    = 3
	defaultTiDBCount    = 1
	defaultPDCount      = 3
	defaultTiFlashCount = 1
	maxRetries          = 3
	retryInterval       = 5 * time.Second
	deployTimeout       = 5 * time.Minute
)

// ProcessManager is used to manage the processes of the cluster.
type ProcessManager struct {
	tag  string
	pids []int
}

// NewProcessManager creates a new ProcessManager.
func NewProcessManager(tag string) *ProcessManager {
	return &ProcessManager{tag: tag}
}

// CollectPids will collect the pids of the processes.
func (pm *ProcessManager) CollectPids() error {
	output, err := runCommandWithOutput(fmt.Sprintf("pgrep -f %s", pm.tag))
	if err != nil {
		return fmt.Errorf("failed to collect pids: %v", err)
	}

	for _, pidStr := range strings.Split(strings.TrimSpace(output), "\n") {
		if pid, err := strconv.Atoi(pidStr); err == nil {
			pm.pids = append(pm.pids, pid)
		}
	}
	return nil
}

// Cleanup will send SIGTERM to all the processes and wait for a while.
func (pm *ProcessManager) Cleanup() {
	for _, pid := range pm.pids {
		// First try SIGTERM
		syscall.Kill(pid, syscall.SIGTERM)
	}

	// Wait and force kill if necessary
	time.Sleep(3 * time.Second)
	for _, pid := range pm.pids {
		if isProcessRunning(pid) {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// SetupSuite will run before the tests in the suite are run.
func (s *clusterSuite) SetupSuite() {
	re := s.Require()

	// Clean the data dir. It is the default data dir of TiUP.
	dataDir := filepath.Join(os.Getenv("HOME"), ".tiup", "data", "pd_real_cluster_test_"+s.suiteName+"_*")
	matches, err := filepath.Glob(dataDir)
	re.NoError(err)

	for _, match := range matches {
		re.NoError(runCommand("rm", "-rf", match))
	}
	s.startCluster()
}

// TearDownSuite will run after all the tests in the suite have been run.
func (s *clusterSuite) TearDownSuite() {
	// Even if the cluster deployment fails, we still need to destroy the cluster.
	// If the cluster does not fail to deploy, the cluster will be destroyed in
	// the cleanup function. And these code will not work.
	s.clusterCnt++
	s.stopCluster()
}

func (s *clusterSuite) startCluster() {
	log.Info("start to deploy a cluster", zap.Bool("ms", s.ms))
	s.deploy()
	s.waitReady()
	s.clusterCnt++
}

func (s *clusterSuite) stopCluster() {
	s.clusterCnt--
	tag := s.tag()

	pm := NewProcessManager(tag)
	if err := pm.CollectPids(); err != nil {
		log.Warn("failed to collect pids", zap.Error(err))
		return
	}

	pm.Cleanup()
	log.Info("cluster destroyed", zap.String("tag", tag))
}

func (s *clusterSuite) tag() string {
	if s.ms {
		return fmt.Sprintf("pd_real_cluster_test_ms_%s_%d", s.suiteName, s.clusterCnt)
	}
	return fmt.Sprintf("pd_real_cluster_test_%s_%d", s.suiteName, s.clusterCnt)
}

func (s *clusterSuite) restart() {
	tag := s.tag()
	log.Info("start to restart", zap.String("tag", tag))
	s.stopCluster()
	s.startCluster()
	log.Info("TiUP restart success")
}

func (s *clusterSuite) deploy() {
	re := s.Require()
	curPath, err := os.Getwd()
	re.NoError(err)
	re.NoError(os.Chdir("../../.."))

	if !fileExists("third_bin") || !fileExists("third_bin/tikv-server") || !fileExists("third_bin/tidb-server") || !fileExists("third_bin/tiflash") {
		log.Info("downloading binaries...")
		log.Info("this may take a few minutes, you can also download them manually and put them in the bin directory.")
		re.NoError(runCommand("sh",
			"./tests/integrations/realcluster/download_integration_test_binaries.sh"))
	}
	if !fileExists("bin") || !fileExists("bin/pd-server") {
		log.Info("complie pd binaries...")
		re.NoError(runCommand("make", "pd-server"))
	}

	if !fileExists(playgroundLogDir) {
		re.NoError(os.MkdirAll(playgroundLogDir, 0755))
	}

	// nolint:errcheck
	go func() {
		playgroundOpts := []string{
			fmt.Sprintf("--kv %d", defaultTiKVCount),
			fmt.Sprintf("--tiflash %d", defaultTiFlashCount),
			fmt.Sprintf("--db %d", defaultTiDBCount),
			fmt.Sprintf("--pd %d", defaultPDCount),
			"--without-monitor",
			fmt.Sprintf("--tag %s", s.tag()),
		}

		if s.ms {
			playgroundOpts = append(playgroundOpts,
				"--pd.mode ms",
				"--tso 1",
				"--scheduling 1",
			)
		}

		cmd := fmt.Sprintf(`%s playground nightly %s %s > %s 2>&1 & `,
			tiupBin,
			strings.Join(playgroundOpts, " "),
			buildBinPathsOpts(s.ms),
			filepath.Join(playgroundLogDir, s.tag()+".log"),
		)

		runCommand("sh", "-c", cmd)
	}()

	// Avoid to change the dir before execute `tiup playground`.
	time.Sleep(10 * time.Second)
	re.NoError(os.Chdir(curPath))
}

func buildBinPathsOpts(ms bool) string {
	opts := []string{
		"--pd.binpath ./bin/pd-server",
		"--kv.binpath ./third_bin/tikv-server",
		"--db.binpath ./third_bin/tidb-server",
		"--tiflash.binpath ./third_bin/tiflash",
	}

	if ms {
		opts = append(opts,
			"--tso.binpath ./bin/pd-server",
			"--scheduling.binpath ./bin/pd-server",
		)
	}

	return strings.Join(opts, " ")
}

func (s *clusterSuite) waitReady() {
	re := s.Require()
	const (
		interval = 5
		maxTimes = 20
	)
	log.Info("start to wait TiUP ready", zap.String("tag", s.tag()))
	timeout := time.After(time.Duration(maxTimes*interval) * time.Second)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for i := 0; i < maxTimes; i++ {
		select {
		case <-timeout:
			re.FailNowf("TiUP is not ready after timeout, tag: %s", s.tag())
		case <-ticker.C:
			log.Info("check TiUP ready", zap.String("tag", s.tag()))
			cmd := fmt.Sprintf(`%s playground display --tag %s`, tiupBin, s.tag())
			output, err := runCommandWithOutput(cmd)
			if err == nil {
				log.Info("TiUP is ready", zap.String("tag", s.tag()))
				return
			}
			log.Info(output)
			log.Info("TiUP is not ready, will retry", zap.Int("retry times", i),
				zap.String("tag", s.tag()), zap.Error(err))
		}
	}
	re.FailNowf("TiUP is not ready after max retries, tag: %s", s.tag())
}
