package server_test

import (
	"context"
	"io"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestRegionHeartbeatEOF(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetServer(leader)
	addr := leaderServer.GetAddr()
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, addr)
	defer conn.Close()

	stream, err := grpcPDClient.RegionHeartbeat(ctx)
	re.NoError(err)

	// Send one heartbeat to establish connection
	err = stream.Send(&pdpb.RegionHeartbeatRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
	})
	re.NoError(err)

	// Close the send direction. This sends io.EOF to the server.
	// The server should handle this as a clean shutdown and return nil.
	err = stream.CloseSend()
	re.NoError(err)

	// The server should close the stream cleanly, resulting in io.EOF on the client side.
	// We might receive responses before EOF if the server sent any.
	for {
		_, err = stream.Recv()
		if err != nil {
			re.Equal(io.EOF, err)
			break
		}
	}
}

func TestReportBucketsEOF(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetServer(leader)
	addr := leaderServer.GetAddr()
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, addr)
	defer conn.Close()

	stream, err := grpcPDClient.ReportBuckets(ctx)
	re.NoError(err)

	// Send one bucket report to establish connection
	err = stream.Send(&pdpb.ReportBucketsRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
	})
	re.NoError(err)

	// Close the send direction.
	_, err = stream.CloseAndRecv()
	// If the server handles io.EOF correctly, it should process the close and return response (or EOF if no response).
	// ReportBuckets returns a response on CloseAndRecv usually.
	// If it fails with wrapped error, we might get an error here.
	if err != nil && err != io.EOF {
		re.NoError(err) 
	}
}
