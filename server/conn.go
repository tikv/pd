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
	"github.com/pingcap/pd/pkg/metrics"
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

	c := &conn{
		s:    s,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
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

	p := &leaderProxy{s: c.s}
	defer p.close()

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
		label := metrics.GetCmdLabel(request)

		var response *pdpb.Response

		if err = c.checkRequest(request); err != nil {
			log.Errorf("check request %s err %v", request, errors.ErrorStack(err))
			response = newError(err)
		} else if !c.s.IsLeader() {
			response, err = p.handleRequest(msgID, request)
			if err != nil {
				log.Errorf("proxy request %s err %v", request, errors.ErrorStack(err))
				response = newError(err)
			}
		} else {
			response, err = c.handleRequest(request)
			if err != nil {
				log.Errorf("handle request %s err %v", request, errors.ErrorStack(err))
				response = newError(err)
			}
		}

		if err == nil {
			cmdCounter.WithLabelValues(label).Inc()
			cmdDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
		} else {
			cmdFailedCounter.WithLabelValues(label).Inc()
			cmdFailedDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
		}

		if response == nil {
			// we don't need to response, maybe error?
			// if error, we will return an error response later.
			log.Warn("empty response")
			continue
		}

		updateResponse(request, response)

		msg = &msgpb.Message{
			MsgType: msgpb.MessageType_PdResp,
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

		cmdCompletedDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
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

func (c *conn) checkRequest(req *pdpb.Request) error {
	// Don't check cluster ID of this command type.
	if req.GetCmdType() == pdpb.CommandType_GetPDMembers {
		if req.Header == nil {
			req.Header = &pdpb.RequestHeader{}
		}
		req.Header.ClusterId = c.s.clusterID
	}

	clusterID := req.GetHeader().GetClusterId()
	if clusterID != c.s.clusterID {
		return errors.Errorf("mismatch cluster id, need %d but got %d", c.s.clusterID, clusterID)
	}
	return nil
}

func (c *conn) handleRequest(req *pdpb.Request) (*pdpb.Response, error) {
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
	case pdpb.CommandType_AskMerge:
		return c.handleAskMerge(req)
	case pdpb.CommandType_ReportSplit:
		return c.handleReportSplit(req)
	case pdpb.CommandType_GetRegion:
		return c.handleGetRegion(req)
	case pdpb.CommandType_GetRegionByID:
		return c.handleGetRegionByID(req)
	case pdpb.CommandType_RegionHeartbeat:
		return c.handleRegionHeartbeat(req)
	case pdpb.CommandType_StoreHeartbeat:
		return c.handleStoreHeartbeat(req)
	case pdpb.CommandType_GetClusterConfig:
		return c.handleGetClusterConfig(req)
	case pdpb.CommandType_PutClusterConfig:
		return c.handlePutClusterConfig(req)
	case pdpb.CommandType_GetPDMembers:
		return c.handleGetPDMembers(req)
	default:
		return nil, errors.Errorf("unsupported command %s", req)
	}
}

type leaderProxy struct {
	s    *Server
	conn net.Conn
}

func (p *leaderProxy) close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

func (p *leaderProxy) handleRequest(msgID uint64, req *pdpb.Request) (*pdpb.Response, error) {
	// Create a connection to leader.
	if p.conn == nil {
		leader, err := p.s.GetLeader()
		if err != nil {
			return nil, errors.Trace(err)
		}
		conn, err := rpcConnect(leader.GetAddr())
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.conn = conn
	}

	resp, err := rpcCall(p.conn, msgID, req)
	if err != nil {
		p.conn.Close()
		p.conn = nil
	}
	return resp, errors.Trace(err)
}
