package server

import (
	"bufio"
	"net"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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

func newConn(s *Server, netConn net.Conn) *conn {
	c := &conn{
		s:    s,
		rb:   bufio.NewReaderSize(netConn, readBufferSize),
		wb:   bufio.NewWriterSize(netConn, writeBufferSize),
		conn: netConn,
	}

	s.connsLock.Lock()
	s.conns[c] = struct{}{}
	s.connsLock.Unlock()

	return c
}

func (c *conn) run() {
	defer func() {
		c.s.wg.Done()
		c.Close()

		c.s.connsLock.Lock()
		delete(c.s.conns, c)
		c.s.connsLock.Unlock()
	}()

	for {
		request := &pdpb.Request{}
		msgID, err := util.ReadMessage(c.rb, request)
		if err != nil {
			log.Errorf("read request message err %v", err)
			return
		}

		response, err := c.handleRequest(request)
		if err != nil {
			log.Errorf("handle request %s err %v", request, errors.ErrorStack(err))
			response = NewError(err)
		}

		if response == nil {
			// we don't need to response, maybe error?
			// if error, we will return an error response later.
			log.Warn("empty response")
			continue
		}

		updateResponse(request, response)

		if err = util.WriteMessage(c.wb, msgID, response); err != nil {
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

func (c *conn) Close() error {
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
	case pdpb.CommandType_AskChangePeer:
		return c.handleAskChangePeer(req)
	case pdpb.CommandType_AskSplit:
		return c.handleAskSplit(req)
	case pdpb.CommandType_GetRegion:
		return c.handleGetRegion(req)
	case pdpb.CommandType_GetClusterConfig:
		return c.handleGetClusterConfig(req)
	case pdpb.CommandType_PutClusterConfig:
		return c.handlePutClusterConfig(req)
	default:
		return nil, errors.Errorf("unsupported command %s", req)
	}
}
