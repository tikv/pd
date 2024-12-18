// Copyright 2018 TiKV Project Authors.
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

package tempurl

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

var (
	testAddrMutex syncutil.Mutex
	testAddrMap   = make(map[string]struct{})
)

// reference: /pd/tools/pd-ut/alloc/server.go
const AllocURLFromUT = "allocURLFromUT"

// Alloc allocates a local URL for testing.
func Alloc() string {
	for range 10 {
		if u := tryAllocTestURL(); u != "" {
			return u
		}
		time.Sleep(time.Second)
	}
	log.Fatal("failed to alloc test URL")
	return ""
}

func tryAllocTestURL() string {
	if url := getFromUT(); url != "" {
		return url
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal("listen failed", errs.ZapError(err))
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

func getFromUT() string {
	addr := os.Getenv(AllocURLFromUT)
	if addr == "" {
		return ""
	}

	req, err := http.NewRequest(http.MethodGet, addr, nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	url := string(body)
	return url
}
