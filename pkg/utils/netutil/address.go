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

package netutil

import (
	"net"
	"net/http"
)

// fork from tidb, pr: https://github.com/pingcap/tidb/pull/20546

func isLoopBackOrUnspecifiedAddr(addr string) bool {
	tcpAddr, err := net.ResolveTCPAddr("", addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(tcpAddr.IP.String())
	return ip != nil && (ip.IsUnspecified() || ip.IsLoopback())
}

// ResolveLoopBackAddr resolve loop back address.
func ResolveLoopBackAddr(address, backAddress string) string {
	if isLoopBackOrUnspecifiedAddr(address) && !isLoopBackOrUnspecifiedAddr(backAddress) {
		addr, err1 := net.ResolveTCPAddr("", address)
		statusAddr, err2 := net.ResolveTCPAddr("", backAddress)
		if err1 == nil && err2 == nil {
			addr.IP = statusAddr.IP
			address = addr.String()
		}
	}
	return address
}

// IsEnableHTTPS returns true if client use tls.
func IsEnableHTTPS(client *http.Client) bool {
	if client == nil {
		return false
	}
	ts, ok := client.Transport.(*http.Transport)
	if !ok {
		return false
	}
	if ts.TLSClientConfig != nil && ts.TLSClientConfig.RootCAs != nil {
		return true
	}
	return false
}
