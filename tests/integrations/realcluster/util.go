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
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
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
	cmd := exec.Command("sh", "-c", cmdStr)
	log.Info(cmd.String())
	bytes, err := cmd.Output()
	if err != nil {
		return string(bytes), err
	}
	return string(bytes), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func newPDClient(re *require.Assertions) pd.Client {
	pdEndpoints := getPDEndpoints(re)
	cli, err := pd.NewClientWithContext(
		context.Background(), caller.TestComponent, pdEndpoints,
		pd.SecurityOption{}, opt.WithMaxErrorRetry(1),
	)
	re.NoError(err)
	return cli
}

func getPDEndpoints(re *require.Assertions) []string {
	output, err := runCommandWithOutput("ps -ef | grep tikv-server | awk -F '--pd-endpoints=' '{print $2}' | awk '{print $1}'")
	re.NoError(err)
	var pdAddrs []string
	for _, addr := range strings.Split(strings.TrimSpace(output), "\n") {
		// length of addr is less than 5 means it must not be a valid address
		if len(addr) < 5 {
			continue
		}
		pdAddrs = append(pdAddrs, strings.Split(addr, ",")...)
	}
	return removeDuplicates(pdAddrs)
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
