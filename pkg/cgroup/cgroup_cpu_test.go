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

//go:build linux

package cgroup

import (
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
)

func checkKernelVersionNewerThan(re *require.Assertions, t *testing.T, major, minor int) bool {
	u := syscall.Utsname{}
	err := syscall.Uname(&u)
	re.NoError(err)
	releaseBs := make([]byte, 0, len(u.Release))
	for _, v := range u.Release {
		if v == 0 {
			break
		}
		releaseBs = append(releaseBs, byte(v))
	}
	releaseStr := string(releaseBs)
	t.Log("kernel release string:", releaseStr)
	versionInfoRE := regexp.MustCompile(`[0-9]+\.[0-9]+\.[0-9]+`)
	kernelVersion := versionInfoRE.FindAllString(releaseStr, 1)
	re.Len(kernelVersion, 1, fmt.Sprintf("release str is %s", releaseStr))
	kernelVersionPartRE := regexp.MustCompile(`[0-9]+`)
	kernelVersionParts := kernelVersionPartRE.FindAllString(kernelVersion[0], -1)
	re.Len(kernelVersionParts, 3, fmt.Sprintf("kernel version str is %s", kernelVersion[0]))
	t.Logf("parsed kernel version parts: major %s, minor %s, patch %s",
		kernelVersionParts[0], kernelVersionParts[1], kernelVersionParts[2])
	mustConvInt := func(s string) int {
		i, err := strconv.Atoi(s)
		re.NoError(err, s)
		return i
	}
	versionNewerThanFlag := false
	if mustConvInt(kernelVersionParts[0]) > major {
		versionNewerThanFlag = true
	} else if mustConvInt(kernelVersionParts[0]) == major && mustConvInt(kernelVersionParts[1]) > minor {
		versionNewerThanFlag = true
	}
	return versionNewerThanFlag
}

func TestGetCgroupCPU(t *testing.T) {
	re := require.New(t)
	exit := make(chan struct{})
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-exit:
					return
				default:
					runtime.Gosched()
				}
			}
		}()
	}
	cpu, err := GetCgroupCPU()
	if err == errs.ErrNoCPUControllerDetected {
		// for more information, please refer https://github.com/pingcap/tidb/pull/41347
		if checkKernelVersionNewerThan(re, t, 4, 7) {
			re.NoError(err, "linux version > v4.7 and err still happens")
		} else {
			t.Logf("the error '%s' is ignored because the kernel is too old", err)
		}
	} else {
		re.NoError(err)
		re.NotZero(cpu.Period)
		re.Less(int64(1), cpu.Period)
	}
	close(exit)
	wg.Wait()
}
