// Copyright 2024 TiKV Authors
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
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type realClusterSuite struct {
	suite.Suite

	clusterCnt int
	suiteName  string
}

// SetupSuite will run before the tests in the suite are run.
func (s *realClusterSuite) SetupSuite() {
	t := s.T()

	dataDir := filepath.Join(os.Getenv("HOME"), ".tiup", "data", "pd_real_cluster_test_"+s.suiteName+"_*")
	matches, err := filepath.Glob(dataDir)
	require.NoError(t, err)

	for _, match := range matches {
		require.NoError(t, runCommand("rm", "-rf", match))
	}
	s.startRealCluster(t)
	t.Cleanup(func() {
		s.stopRealCluster(t)
	})
}

// TearDownSuite will run after all the tests in the suite have been run.
func (s *realClusterSuite) TearDownSuite() {
	// Even if the cluster deployment fails, we still need to destroy the cluster.
	// If the cluster does not fail to deploy, the cluster will be destroyed in
	// the cleanup function. And these code will not work.
	s.clusterCnt++
	s.stopRealCluster(s.T())
}

func (s *realClusterSuite) startRealCluster(t *testing.T) {
	log.Info("start to deploy a real cluster")

	s.deploy(t)
	s.clusterCnt++
}

func (s *realClusterSuite) stopRealCluster(t *testing.T) {
	s.clusterCnt--

	log.Info("start to destroy a real cluster", zap.String("tag", s.tag()))
	destroy(t, s.tag())
	time.Sleep(5 * time.Second)
}

func (s *realClusterSuite) tag() string {
	return fmt.Sprintf("pd_real_cluster_test_%s_%d", s.suiteName, s.clusterCnt)
}

// func restartTiUP() {
// 	log.Info("start to restart TiUP")
// 	cmd := exec.Command("make", "deploy")
// 	cmd.Stdout = os.Stdout
// 	cmd.Stderr = os.Stderr
// 	err := cmd.Run()
// 	if err != nil {
// 		panic(err)
// 	}
// 	log.Info("TiUP restart success")
// }

func (s *realClusterSuite) deploy(t *testing.T) {
	tag := s.tag()
	deployTiupPlayground(t, tag)
	waitTiupReady(t, tag)
}

func destroy(t *testing.T, tag string) {
	cmdStr := fmt.Sprintf("ps -ef | grep 'tiup playground' | grep %s | awk '{print $2}' | head -n 1", tag)
	cmd := exec.Command("sh", "-c", cmdStr)
	bytes, err := cmd.Output()
	require.NoError(t, err)
	pid := string(bytes)
	// nolint:errcheck
	runCommand("sh", "-c", "kill -9 "+pid)
	log.Info("destroy success", zap.String("pid", pid))
}

func deployTiupPlayground(t *testing.T, tag string) {
	curPath, err := os.Getwd()
	require.NoError(t, err)

	log.Info(curPath)
	require.NoError(t, os.Chdir("../../.."))

	if !fileExists("third_bin") || !fileExists("third_bin/tikv-server") || !fileExists("third_bin/tidb-server") || !fileExists("third_bin/tiflash") {
		log.Info("downloading binaries...")
		log.Info("this may take a few minutes, you can also download them manually and put them in the bin directory.")
		require.NoError(t, runCommand("sh",
			"./tests/integrations/realcluster/download_integration_test_binaries.sh"))
	}
	if !fileExists("bin") || !fileExists("bin/pd-server") {
		log.Info("complie pd binaries...")
		require.NoError(t, runCommand("make", "pd-server"))
	}
	if !fileExists(filepath.Join(curPath, "playground")) {
		require.NoError(t, os.Mkdir(filepath.Join(curPath, "playground"), 0755))
	}
	// nolint:errcheck
	go runCommand("sh", "-c",
		`tiup playground nightly --kv 3 --tiflash 1 --db 1 --pd 3 \
		--without-monitor --tag `+tag+` --pd.binpath ./bin/pd-server \
		--kv.binpath ./third_bin/tikv-server \
		--db.binpath ./third_bin/tidb-server --tiflash.binpath ./third_bin/tiflash \
		--pd.config ./tests/integrations/realcluster/pd.toml \
		> `+filepath.Join(curPath, "playground", tag+".log")+` 2>&1 & `)

	time.Sleep(10 * time.Second)
	require.NoError(t, os.Chdir(curPath))
}

func waitTiupReady(t *testing.T, tag string) {
	const (
		interval = 5
		maxTimes = 20
	)
	log.Info("start to wait TiUP ready", zap.String("tag", tag))
	for i := 0; i < maxTimes; i++ {
		err := runCommand("tiup", "playground", "display", "--tag", tag)
		if err == nil {
			log.Info("TiUP is ready", zap.String("tag", tag))
			return
		}

		log.Info("TiUP is not ready, will retry", zap.Int("retry times", i),
			zap.String("tag", tag), zap.Error(err))
		time.Sleep(time.Duration(interval) * time.Second)
	}
	require.Failf(t, "TiUP is not ready", "tag: %s", tag)
}