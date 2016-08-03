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
	"bufio"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

type conn struct {
	s *Server

	rb   *bufio.Reader
	wb   *bufio.Writer
	conn net.Conn
}

func newConn(s *Server, netConn net.Conn, bufrw *bufio.ReadWriter) (*conn, error) {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()

	if !s.isLeader() {
		return nil, errors.Errorf("server <%s> is not leader, cannot create new connection <%s>", s.Name(), netConn.RemoteAddr())
	}

	c := &conn{
		s:    s,
		rb:   bufio.NewReaderSize(bufrw.Reader, readBufferSize),
		wb:   bufio.NewWriterSize(netConn, writeBufferSize),
		conn: netConn,
	}

	s.conns[c] = struct{}{}

	return c, nil
}

func (c *conn) run() {
	defer func() {
		c.s.wg.Done()
		c.close()

		c.s.connsLock.Lock()
		delete(c.s.conns, c)
		c.s.connsLock.Unlock()
	}()

	for {
		msg := &msgpb.Message{}
		msgID, err := util.ReadMessage(c.rb, msg)
		if err != nil {
			log.Errorf("read request message err %v", err)
			return
		}

		if msg.GetMsgType() != msgpb.MessageType_PdReq {
			log.Errorf("invalid request message %v", msg)
			return
		}

		start := time.Now()
		request := msg.GetPdReq()
		requestCmdName := request.GetCmdType().String()
		label, ok := cmds[requestCmdName]
		if !ok {
			label = convertName(requestCmdName)
		}

		response, err := c.handleRequest(request)
		if err != nil {
			log.Errorf("handle request %s err %v", request, errors.ErrorStack(err))
			response = newError(err)

			cmdFailedCounter.WithLabelValues(label).Inc()
			cmdFailedDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
		}

		cmdCounter.WithLabelValues(label).Inc()
		cmdDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())

		if response == nil {
			// we don't need to response, maybe error?
			// if error, we will return an error response later.
			log.Warn("empty response")
			continue
		}

		updateResponse(request, response)

		msg = &msgpb.Message{
			MsgType: msgpb.MessageType_PdResp.Enum(),
			PdResp:  response,
		}

		if err = util.WriteMessage(c.wb, msgID, msg); err != nil {
			log.Errorf("write response message err %v", err)
			return
		}

		if err = c.wb.Flush(); err != nil {
			log.Errorf("flush response message err %v", err)
			return
		}
	}
}

func updateResponse(req *pdpb.Request, resp *pdpb.Response) {
	// We can use request field directly here.
	resp.CmdType = req.CmdType

	if req.Header == nil {
		return
	}

	if resp.Header == nil {
		resp.Header = &pdpb.ResponseHeader{}
	}

	resp.Header.Uuid = req.Header.Uuid
	resp.Header.ClusterId = req.Header.ClusterId
}

func (c *conn) close() error {
	return errors.Trace(c.conn.Close())
}

func (c *conn) handleRequest(req *pdpb.Request) (*pdpb.Response, error) {
	clusterID := req.GetHeader().GetClusterId()
	if clusterID != c.s.cfg.ClusterID {
		return nil, errors.Errorf("mismatch cluster id, need %d but got %d", c.s.cfg.ClusterID, clusterID)
	}

	switch req.GetCmdType() {
	case pdpb.CommandType_Tso:
		return c.handleTso(req)
	case pdpb.CommandType_AllocId:
		return c.handleAllocID(req)
	case pdpb.CommandType_Bootstrap:
		return c.handleBootstrap(req)
	case pdpb.CommandType_IsBootstrapped:
		return c.handleIsBootstrapped(req)
	case pdpb.CommandType_GetStore:
		return c.handleGetStore(req)
	case pdpb.CommandType_PutStore:
		return c.handlePutStore(req)
	case pdpb.CommandType_AskSplit:
		return c.handleAskSplit(req)
	case pdpb.CommandType_ReportSplit:
		return c.handleReportSplit(req)
	case pdpb.CommandType_GetRegion:
		return c.handleGetRegion(req)
	case pdpb.CommandType_RegionHeartbeat:
		return c.handleRegionHeartbeat(req)
	case pdpb.CommandType_StoreHeartbeat:
		return c.handleStoreHeartbeat(req)
	case pdpb.CommandType_GetClusterConfig:
		return c.handleGetClusterConfig(req)
	case pdpb.CommandType_PutClusterConfig:
		return c.handlePutClusterConfig(req)
	default:
		return nil, errors.Errorf("unsupported command %s", req)
	}
}
