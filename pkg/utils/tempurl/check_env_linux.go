// Copyright 2020 TiKV Project Authors.
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
// +build linux

package tempurl

import (
	"github.com/cakturk/go-netstat/netstat"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

func environmentCheck(addr string) bool {
	valid, err := checkAddr(addr[len("http://"):])
	if err != nil {
		log.Error("check port status failed", errs.ZapError(err))
		return false
	}
	return valid
}

func checkAddr(addr string) (bool, error) {
	// Check via netstat if there are any sockets on this address
	// We only check LocalAddr since we only care if something is binding/listening on this port
	// Note: We only allocate IPv4 addresses (127.0.0.1), so no need to check IPv6
	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.LocalAddr.String() == addr
	})
	if err != nil {
		return false, errs.ErrNetstatTCPSocks.Wrap(err)
	}
	// If any sockets exist (LISTEN, ESTABLISHED, TIME_WAIT, etc.), the port is not available
	return len(tabs) < 1, nil
}
