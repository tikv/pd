// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"net"
	"time"

	. "github.com/pingcap/check"
)

type testStoreConnSuite struct {
}

var _ = Suite(&testStoreConnSuite{})

type testConn struct {
}

func (c *testConn) Read(b []byte) (n int, err error)   { return len(b), nil }
func (c *testConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *testConn) Close() error                       { return nil }
func (c *testConn) LocalAddr() net.Addr                { return nil }
func (c *testConn) RemoteAddr() net.Addr               { return nil }
func (c *testConn) SetDeadline(t time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(t time.Time) error { return nil }

func testStoreConn(addr string) (*storeConn, error) {
	return &storeConn{
		conn:        &testConn{},
		touchedTime: time.Now()}, nil
}

func (s *testStoreConnSuite) TestStoreConns(c *C) {
	conns := newStoreConns(testStoreConn)
	c.Assert(conns.conns, HasLen, 0)

	addr1 := "127.0.0.1:1"
	oldConn, err := conns.GetConn(addr1)
	c.Assert(err, IsNil)
	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr1)

	newConn, err := conns.GetConn(addr1)
	c.Assert(err, IsNil)
	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr1)

	c.Assert(oldConn, Equals, newConn)

	conns.RemoveConn(addr1)
	c.Assert(conns.conns, HasLen, 0)

	addr2 := "127.0.0.1:2"
	conns.GetConn(addr2)
	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr2)

	conns.Close()
	c.Assert(conns.conns, HasLen, 0)

	// Test with idleTimeout conn.
	idleTimeout := 100 * time.Millisecond
	conns.SetIdleTimeout(idleTimeout)

	addr3 := "127.0.0.1:3"
	oldConn, err = conns.GetConn(addr3)
	c.Assert(err, IsNil)
	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr3)

	time.Sleep(2 * idleTimeout)

	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr3)

	newConn, err = conns.GetConn(addr3)
	c.Assert(err, IsNil)
	c.Assert(conns.conns, HasLen, 1)
	c.Assert(conns.conns, HasKey, addr3)

	c.Assert(oldConn, Not(Equals), newConn)

	addr4 := "127.0.0.1:4"
	conns.GetConn(addr4)
	c.Assert(conns.conns, HasLen, 2)
	c.Assert(conns.conns, HasKey, addr4)

	conns.Close()
	c.Assert(conns.conns, HasLen, 0)
}
