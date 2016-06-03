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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/sync2"
)

const (
	connectTimeout = 3 * time.Second
	idleTimeout    = 30 * time.Second
)

type storeConn struct {
	conn        net.Conn
	touchedTime time.Time
}

func (nc *storeConn) close() error {
	return errors.Trace(nc.conn.Close())
}

func newStoreConn(addr string) (*storeConn, error) {
	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &storeConn{
		conn:        conn,
		touchedTime: time.Now()}, nil
}

type createConnFunc func(addr string) (*storeConn, error)

var defaultConnFunc = newStoreConn

type storeConns struct {
	m           sync.Mutex
	conns       map[string]*storeConn
	idleTimeout sync2.AtomicDuration
	f           createConnFunc
}

// newStoreConns creates a new store conns.
func newStoreConns(f createConnFunc) *storeConns {
	ncs := new(storeConns)
	ncs.f = f
	ncs.conns = make(map[string]*storeConn)
	return ncs
}

// This function is not thread-safed.
func (ncs *storeConns) createNewConn(addr string) (*storeConn, error) {
	conn, err := ncs.f(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ncs.conns[addr] = conn
	return conn, nil
}

// SetIdleTimeout sets idleTimeout of each conn.
func (ncs *storeConns) SetIdleTimeout(idleTimeout time.Duration) {
	ncs.idleTimeout.Set(idleTimeout)
}

// GetConn gets the conn by addr.
func (ncs *storeConns) GetConn(addr string) (*storeConn, error) {
	ncs.m.Lock()
	defer ncs.m.Unlock()

	conn, ok := ncs.conns[addr]
	if !ok {
		return ncs.createNewConn(addr)
	}

	timeout := ncs.idleTimeout.Get()
	if timeout > 0 && conn.touchedTime.Add(timeout).Sub(time.Now()) < 0 {
		err := conn.close()
		if err != nil {
			return nil, errors.Trace(err)
		}

		return ncs.createNewConn(addr)
	}

	conn.touchedTime = time.Now()
	return conn, nil
}

// RemoveConn removes the conn by addr.
func (ncs *storeConns) RemoveConn(addr string) {
	ncs.m.Lock()
	defer ncs.m.Unlock()

	conn, ok := ncs.conns[addr]
	if !ok {
		return
	}

	err := conn.close()
	if err != nil {
		log.Warnf("close store conn failed - %v", err)
	}
	delete(ncs.conns, addr)
}

// Close closes the conns.
func (ncs *storeConns) Close() {
	ncs.m.Lock()
	defer ncs.m.Unlock()

	for _, conn := range ncs.conns {
		err := conn.close()
		if err != nil {
			log.Warnf("close store conn failed - %v", err)
		}
	}

	ncs.conns = map[string]*storeConn{}
}
