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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
)

type clusterSuite struct {
	suite.Suite

	suiteName string
	mode      string
	cluster   *cluster
}

// SetupSuite will run before the tests in the suite are run.
func (s *clusterSuite) SetupSuite() {
	re := s.Require()

	dataDir := s.dataDir()
	matches, err := filepath.Glob(dataDir)
	re.NoError(err)

	for _, match := range matches {
		_, err := runCommandWithOutput(fmt.Sprintf("rm -rf %s", match))
		re.NoError(err)
	}

	s.cluster = newCluster(re, s.tag(), dataDir, s.mode)
	s.cluster.start()
}

// TearDownSuite will run after all the tests in the suite have been run.
func (s *clusterSuite) TearDownSuite() {
	// Even if the cluster deployment fails, we still need to destroy the cluster.
	// If the cluster does not fail to deploy, the cluster will be destroyed in
	// the cleanup function. And these code will not work.
	s.cluster.stop()
}

func (s *clusterSuite) tag() string {
	if s.mode == "ms" {
		return fmt.Sprintf("pd_real_cluster_test_ms_%s_%d", s.suiteName, time.Now().Unix())
	}
	return fmt.Sprintf("pd_real_cluster_test_%s_%d", s.suiteName, time.Now().Unix())
}

func (s *clusterSuite) dataDir() string {
	if s.mode == "ms" {
		return filepath.Join(os.Getenv("HOME"), ".tiup", "data", fmt.Sprintf("pd_real_cluster_test_ms_%s_%d", s.suiteName, time.Now().Unix()))
	}
	return filepath.Join(os.Getenv("HOME"), ".tiup", "data", fmt.Sprintf("pd_real_cluster_test_%s_%d", s.suiteName, time.Now().Unix()))
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
)

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

type cluster struct {
	re      *require.Assertions
	tag     string
	datadir string
	mode    string
	pids    []int
}

func newCluster(re *require.Assertions, tag, datadir, mode string) *cluster {
	return &cluster{re: re, datadir: datadir, tag: tag, mode: mode}
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
	if err := c.collectPids(); err != nil {
		log.Warn("failed to collect pids", zap.Error(err))
		return
	}

	for _, pid := range c.pids {
		// First try SIGTERM
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}

	// Wait and force kill if necessary
	time.Sleep(3 * time.Second)
	for _, pid := range c.pids {
		if isProcessRunning(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	log.Info("cluster stopped", zap.String("tag", c.tag))
}

func (c *cluster) deploy() {
	re := c.re
	curPath, err := os.Getwd()
	re.NoError(err)
	re.NoError(os.Chdir("../../.."))

	if !fileExists("third_bin") || !fileExists("third_bin/tikv-server") || !fileExists("third_bin/tidb-server") || !fileExists("third_bin/tiflash") {
		log.Info("downloading binaries...")
		log.Info("this may take a few minutes, you can also download them manually and put them in the bin directory.")
		_, err := runCommandWithOutput("./tests/integrations/realcluster/download_integration_test_binaries.sh")
		re.NoError(err)
	}
	if !fileExists("bin") || !fileExists("bin/pd-server") {
		log.Info("compile pd binaries...")
		_, err := runCommandWithOutput("make pd-server")
		re.NoError(err)
	}

	if !fileExists(playgroundLogDir) {
		re.NoError(os.MkdirAll(playgroundLogDir, 0755))
	}

	// nolint:errcheck
	go func() {
		defer logutil.LogPanic()
		playgroundOpts := []string{
			fmt.Sprintf("--kv %d", defaultTiKVCount),
			fmt.Sprintf("--tiflash %d", defaultTiFlashCount),
			fmt.Sprintf("--db %d", defaultTiDBCount),
			fmt.Sprintf("--pd %d", defaultPDCount),
			"--without-monitor",
			fmt.Sprintf("--tag %s", c.tag),
		}

		if c.mode == "ms" {
			playgroundOpts = append(playgroundOpts,
				"--pd.mode ms",
				"--tso 1",
				"--scheduling 1",
			)
		}

		cmd := fmt.Sprintf(`%s playground nightly %s %s > %s 2>&1 & `,
			tiupBin,
			strings.Join(playgroundOpts, " "),
			buildBinPathsOpts(c.mode == "ms"),
			filepath.Join(playgroundLogDir, c.tag+".log"),
		)
		_, err := runCommandWithOutput(cmd)
		re.NoError(err)
	}()

	// Avoid to change the dir before execute `tiup playground`.
	time.Sleep(10 * time.Second)
	re.NoError(os.Chdir(curPath))
}

// collectPids will collect the pids of the processes.
func (c *cluster) collectPids() error {
	output, err := runCommandWithOutput(fmt.Sprintf("pgrep -f %s", c.tag))
	if err != nil {
		return fmt.Errorf("failed to collect pids: %v", err)
	}

	for _, pidStr := range strings.Split(strings.TrimSpace(output), "\n") {
		if pid, err := strconv.Atoi(pidStr); err == nil {
			c.pids = append(c.pids, pid)
		}
	}
	return nil
}

func (c *cluster) waitReady() {
	re := c.re
	log.Info("start to wait TiUP ready", zap.String("tag", c.tag))
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-time.After(100 * time.Second):
			re.FailNowf("TiUP is not ready after timeout, tag: %s", c.tag)
		case <-ticker.C:
			log.Info("check TiUP ready", zap.String("tag", c.tag))
			cmd := fmt.Sprintf(`%s playground display --tag %s`, tiupBin, c.tag)
			output, err := runCommandWithOutput(cmd)
			if err == nil {
				log.Info("TiUP is ready", zap.String("tag", c.tag))
				return
			}
			log.Info(output)
			log.Info("TiUP is not ready, will retry", zap.String("tag", c.tag), zap.Error(err))
		}
	}
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
