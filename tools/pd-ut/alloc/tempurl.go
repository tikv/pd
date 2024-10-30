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

package alloc

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
)

var (
	testAddrMutex sync.Mutex
	testAddrMap   = make(map[string]struct{})
)

// Alloc allocates a local URL for testing.
func Alloc() string {
	for range 50 {
		if u := tryAllocTestURL(); u != "" {
			return u
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatal("failed to alloc test URL")
	return ""
}

func tryAllocTestURL() string {
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return ""
	}
	addr := fmt.Sprintf("http://%s", l.Addr())
	err = l.Close()
	if err != nil {
		log.Fatal("close failed", errs.ZapError(err))
	}

	testAddrMutex.Lock()
	defer testAddrMutex.Unlock()
	if _, ok := testAddrMap[addr]; ok {
		return ""
	}
	if !environmentCheck(addr) {
		return ""
	}
	testAddrMap[addr] = struct{}{}
	return addr
}
