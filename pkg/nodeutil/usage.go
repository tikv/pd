// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package nodeutil

import (
	"bytes"
	"os/exec"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// GetCPUUsage get cpu usage
func GetCPUUsage() (float64, error) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return 0, err
	}
	return percent[0], err
}

// GetMemUsage get mem info
func GetMemUsage() (float64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.UsedPercent, err
}

// GetNetInfo get net info
func GetNetInfo() ([]net.IOCountersStat, error) {
	return net.IOCounters(true)
}

// GetNetLatency get net latency
func GetNetLatency(target string) ([]byte, error) {
	cmd := exec.Command("ping", target, "-c 1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	out = bytes.TrimSpace(out)
	lines := bytes.Split(out, []byte("\n"))
	line := bytes.Split(bytes.TrimSpace(lines[len(lines)-1]), []byte(" "))
	rtt := line[len(line)-2]
	avg := bytes.Split(rtt, []byte("/"))
	return avg[1], nil
}
