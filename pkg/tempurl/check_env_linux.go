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
// See the License for the specific language governing permissions and
// limitations under the License.
// +build linux

package tempurl

import (
	"strconv"
	"strings"

	"github.com/cakturk/go-netstat/netstat"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func environmentCheck(addr string) bool {
	valid, err := checkTimeWait(addr[len("http://127.0.0.1:"):])
	if err != nil {
		log.Info("check port status failed", zap.Error(err))
		return false
	}
	if !valid {
		return false
	}
	return true
}

func checkTimeWait(portStr string) (bool, error) {
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return false, err
	}

	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		remote := strings.ToLower(s.RemoteAddr.IP.String()) == "localhost" || s.RemoteAddr.IP.String() == "127.0.0.1"
		local := strings.ToLower(s.LocalAddr.IP.String()) == "localhost" || s.LocalAddr.IP.String() == "127.0.0.1"
		status := strings.ToUpper(s.State.String()) == "TIME_WAIT"
		return remote && status && local
	})
	if err != nil {
		return false, err
	}

	for _, e := range tabs {
		if e.LocalAddr.Port == uint16(port) || e.RemoteAddr.Port == uint16(port) {
			return false, nil
		}
	}
	return true, nil
}
