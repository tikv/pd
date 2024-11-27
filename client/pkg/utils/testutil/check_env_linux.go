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
// +build linux

package testutil

import (
	"github.com/cakturk/go-netstat/netstat"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func environmentCheck(addr string) bool {
	valid, err := checkAddr(addr[len("http://"):])
	if err != nil {
		log.Error("check port status failed", zap.Error(err))
		return false
	}
	return valid
}

func checkAddr(addr string) (bool, error) {
	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.RemoteAddr.String() == addr || s.LocalAddr.String() == addr
	})
	if err != nil {
		return false, err
	}
	return len(tabs) < 1, nil
}
