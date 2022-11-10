// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/tikv/pd/pkg/cgroup"
)

// MemTotal returns the total amount of RAM on this system
var MemTotal func() (uint64, error)

// GetMemTotalIgnoreErr returns the total amount of RAM on this system/container. If error occurs, return 0.
func GetMemTotalIgnoreErr() uint64 {
	if memTotal, err := MemTotal(); err == nil {
		return memTotal
	}
	return 0
}

// MemTotalNormal returns the total amount of RAM on this system in non-container environment.
func MemTotalNormal() (uint64, error) {
	total, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return total, nil
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return v.Total, err
	}
	memLimit.set(v.Total, time.Now())
	return v.Total, nil
}

type memInfoCache struct {
	updateTime time.Time
	mu         *sync.RWMutex
	mem        uint64
}

func (c *memInfoCache) get() (memo uint64, t time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	memo, t = c.mem, c.updateTime
	return
}

func (c *memInfoCache) set(memo uint64, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem, c.updateTime = memo, t
}

// expiration time is 60s
var memLimit *memInfoCache

// MemTotalCGroup returns the total amount of RAM on this system in container environment.
func MemTotalCGroup() (uint64, error) {
	memo, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return memo, nil
	}
	memo, err := cgroup.GetMemoryLimit()
	if err != nil {
		return memo, err
	}
	memLimit.set(memo, time.Now())
	return memo, nil
}

func init() {
	if cgroup.InContainer() {
		MemTotal = MemTotalCGroup
	} else {
		MemTotal = MemTotalNormal
	}
	memLimit = &memInfoCache{
		mu: &sync.RWMutex{},
	}
}
