package server

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
)

type mockReportBucketsServer struct {
	grpc.ServerStream
	recvFunc func() (*pdpb.ReportBucketsRequest, error)
}

func (m *mockReportBucketsServer) SendAndClose(*pdpb.ReportBucketsResponse) error { return nil }
func (m *mockReportBucketsServer) Recv() (*pdpb.ReportBucketsRequest, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, io.EOF
}
func (m *mockReportBucketsServer) Context() context.Context { return context.Background() }

func TestBucketHeartbeatServerRecvEOF(t *testing.T) {
	stream := &mockReportBucketsServer{
		recvFunc: func() (*pdpb.ReportBucketsRequest, error) {
			return nil, io.EOF
		},
	}
	s := &bucketHeartbeatServer{stream: stream}
	_, err := s.recv()

	t.Logf("Error type: %T, value: %v", err, err)
	if err == io.EOF {
		t.Log("Error is strictly equal to io.EOF")
	} else {
		t.Log("Error is NOT strictly equal to io.EOF")
	}
	
	// We expect io.EOF exactly, but due to the bug it is wrapped.
    // This test should FAIL if the bug is present.
	if err != io.EOF {
		t.Fatalf("recv() returned error %v (type %T), want exactly io.EOF", err, err)
	}
	if atomic.LoadInt32(&s.closed) != 1 {
		t.Errorf("expected closed to be 1, got %d", atomic.LoadInt32(&s.closed))
	}
	if atomic.LoadInt32(&s.closed) != 1 {
		t.Errorf("expected closed to be 1, got %d", atomic.LoadInt32(&s.closed))
	}
}

type mockHeartbeatServer struct {
	grpc.ServerStream
	sendFunc func(*pdpb.RegionHeartbeatResponse) error
	recvFunc func() (*pdpb.RegionHeartbeatRequest, error)
}

func (m *mockHeartbeatServer) Send(resp *pdpb.RegionHeartbeatResponse) error {
	if m.sendFunc != nil {
		return m.sendFunc(resp)
	}
	return nil
}

func (m *mockHeartbeatServer) Recv() (*pdpb.RegionHeartbeatRequest, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, nil
}

func TestHeartbeatServerSendEOF(t *testing.T) {
	stream := &mockHeartbeatServer{
		sendFunc: func(_ *pdpb.RegionHeartbeatResponse) error {
			return io.EOF
		},
	}
	s := &heartbeatServer{stream: stream}
	err := s.Send(&pdpb.RegionHeartbeatResponse{})

	t.Logf("Error type: %T, value: %v", err, err)
	if err != io.EOF {
		t.Fatalf("Send() returned error %v (type %T), want exactly io.EOF", err, err)
	}
}

func TestHeartbeatServerRecvEOF(t *testing.T) {
	stream := &mockHeartbeatServer{
		recvFunc: func() (*pdpb.RegionHeartbeatRequest, error) {
			return nil, io.EOF
		},
	}
	s := &heartbeatServer{stream: stream}
	_, err := s.Recv()

	t.Logf("Error type: %T, value: %v", err, err)
	if err != io.EOF {
		t.Fatalf("Recv() returned error %v (type %T), want exactly io.EOF", err, err)
	}
	if atomic.LoadInt32(&s.closed) != 1 {
		t.Errorf("expected closed to be 1, got %d", atomic.LoadInt32(&s.closed))
	}
	if atomic.LoadInt32(&s.closed) != 1 {
		t.Errorf("expected closed to be 1, got %d", atomic.LoadInt32(&s.closed))
	}
}

type mockTsoServer struct {
	grpc.ServerStream
	sendFunc func(*pdpb.TsoResponse) error
	recvFunc func() (*pdpb.TsoRequest, error)
}

func (m *mockTsoServer) Send(resp *pdpb.TsoResponse) error {
	if m.sendFunc != nil {
		return m.sendFunc(resp)
	}
	return nil
}

func (m *mockTsoServer) Recv() (*pdpb.TsoRequest, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, nil
}

func TestTsoServerSendEOF(t *testing.T) {
	stream := &mockTsoServer{
		sendFunc: func(_ *pdpb.TsoResponse) error {
			return io.EOF
		},
	}
	s := &tsoServer{stream: stream}
	err := s.Send(&pdpb.TsoResponse{})

	t.Logf("Error type: %T, value: %v", err, err)
	if err != io.EOF {
		t.Fatalf("Send() returned error %v (type %T), want exactly io.EOF", err, err)
	}
}

func TestTsoServerRecvEOF(t *testing.T) {
	stream := &mockTsoServer{
		recvFunc: func() (*pdpb.TsoRequest, error) {
			return nil, io.EOF
		},
	}
	s := &tsoServer{stream: stream}
	_, err := s.recv(time.Second)

	t.Logf("Error type: %T, value: %v", err, err)
	if err != io.EOF {
		t.Fatalf("recv() returned error %v (type %T), want exactly io.EOF", err, err)
	}
	if atomic.LoadInt32(&s.closed) != 1 {
		t.Errorf("expected closed to be 1, got %d", atomic.LoadInt32(&s.closed))
	}
}
