// Copyright 2022 TiKV Project Authors.
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

package cgroup

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// They are cgroup filename for different data
	cgroupV1MemStat  = "memory.stat"
	cgroupV2MemLimit = "memory.max"

	cgroupV1MemLimitStatKey = "hierarchical_memory_limit"
)

const (
	procPathCGroup    = "/proc/self/cgroup"
	procPathMountInfo = "/proc/self/mountinfo"
)

// The controller is defined via either type `memory` for cgroup v1 or via empty type for cgroup v2,
// where the type is the second field in /proc/[pid]/cgroup file
func detectControlPath(cgroupFilePath string, controller string) (string, error) {
	//nolint:gosec
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read %s cgroup from cgroups file: %s", controller, cgroupFilePath)
	}
	defer func() {
		err := cgroup.Close()
		if err != nil {
			log.Error("close cgroupFilePath", zap.Error(err))
		}
	}()

	scanner := bufio.NewScanner(cgroup)
	var unifiedPathIfFound string
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{':'})
		if len(fields) != 3 {
			// The lines should always have three fields, there's something fishy here.
			continue
		}

		f0, f1 := string(fields[0]), string(fields[1])
		// First case if v2, second - v1. We give v2 the priority here.
		// There is also a `hybrid` mode when both  versions are enabled,
		// but no known container solutions support it.
		if f0 == "0" && f1 == "" {
			unifiedPathIfFound = string(fields[2])
		} else if f1 == controller {
			return string(fields[2]), nil
		}
	}

	return unifiedPathIfFound, nil
}

// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountInfoPath string, cRoot string, controller string) (string, int, error) {
	//nolint:gosec
	info, err := os.Open(mountInfoPath)
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to read mounts info from file: %s", mountInfoPath)
	}
	defer func() {
		err := info.Close()
		if err != nil {
			log.Error("close mountInfoPath", zap.Error(err))
		}
	}()

	scanner := bufio.NewScanner(info)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 10 {
			continue
		}

		ver, ok := detectCgroupVersion(fields, controller)
		if ok {
			mountPoint := string(fields[4])
			if ver == 2 {
				return mountPoint, ver, nil
			}
			// It is possible that the controller mount and the cgroup path are not the same (both are relative to the NS root).
			// So start with the mount and construct the relative path of the cgroup.
			// To test:
			//  1、start a docker to run unit test or tidb-server
			//   > docker run -it --cpus=8 --memory=8g --name test --rm ubuntu:18.04 bash
			//
			//  2、change the limit when the container is running
			//	docker update --cpus=8 <containers>
			nsRelativePath := string(fields[3])
			if !strings.Contains(nsRelativePath, "..") {
				// We don't expect to see err here ever but in case that it happens
				// the best action is to ignore the line and hope that the rest of the lines
				// will allow us to extract a valid path.
				if relPath, err := filepath.Rel(nsRelativePath, cRoot); err == nil {
					return filepath.Join(mountPoint, relPath), ver, nil
				}
			}
		}
	}

	return "", 0, fmt.Errorf("failed to detect cgroup root mount and version")
}

// Return version of cgroup mount for memory controller if found
func detectCgroupVersion(fields [][]byte, controller string) (_ int, found bool) {
	if len(fields) < 10 {
		return 0, false
	}

	// Due to strange format there can be optional fields in the middle of the set, starting
	// from the field #7. The end of the fields is marked with "-" field
	var pos = 6
	for pos < len(fields) {
		if bytes.Equal(fields[pos], []byte{'-'}) {
			break
		}

		pos++
	}

	// No optional fields separator found or there is less than 3 fields after it which is wrong
	if (len(fields) - pos - 1) < 3 {
		return 0, false
	}

	pos++

	// Check for controller specifically in cgroup v1 (it is listed in super
	// options field), as the value can't be found if it is not enforced.
	if bytes.Equal(fields[pos], []byte("cgroup")) && bytes.Contains(fields[pos+2], []byte(controller)) {
		return 1, true
	} else if bytes.Equal(fields[pos], []byte("cgroup2")) {
		return 2, true
	}

	return 0, false
}

func readInt64Value(root, filename string, cgVersion int) (value uint64, err error) {
	filePath := filepath.Join(root, filename)
	//nolint:gosec
	file, err := os.Open(filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "can't read %s from cgroup v%d", filename, cgVersion)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	present := scanner.Scan()
	if !present {
		return 0, errors.Wrapf(err, "no value found in %s from cgroup v%d", filename, cgVersion)
	}
	data := scanner.Bytes()
	trimmed := string(bytes.TrimSpace(data))
	// cgroupv2 has certain control files that default to "max", so handle here.
	if trimmed == "max" {
		return math.MaxInt64, nil
	}
	value, err = strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse value in %s from cgroup v%d", filename, cgVersion)
	}
	return value, nil
}
