// Copyright 2026 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	keyspacepkg "github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type keyspaceWatchStream struct {
	grpc.ServerStream
	ctx      context.Context
	snapshot chan struct{}
}

func (s *keyspaceWatchStream) Context() context.Context { return s.ctx }

func (s *keyspaceWatchStream) Send(*keyspacepb.WatchKeyspacesResponse) error {
	select {
	case s.snapshot <- struct{}{}:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	return nil
}

func (suite *keyspaceTestSuite) TestWatchKeyspacesCancellation() {
	re := suite.Require()
	service := &server.KeyspaceServer{GrpcServer: &server.GrpcServer{Server: suite.server.GetServer()}}
	request := &keyspacepb.WatchKeyspacesRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID())}
	// Repeated idle disconnects must terminate without another keyspace write.
	for range 3 {
		ctx, cancel := context.WithCancel(suite.T().Context())
		stream := &keyspaceWatchStream{ctx: ctx, snapshot: make(chan struct{}, 1)}
		result := make(chan error, 1)
		go func() { result <- service.WatchKeyspaces(request, stream) }()
		select {
		case <-stream.snapshot:
		case <-time.After(5 * time.Second):
			cancel()
			re.FailNow("initial keyspace snapshot was not sent")
		}
		cancel()
		select {
		case err := <-result:
			re.NoError(err)
		case <-time.After(5 * time.Second):
			re.FailNow("idle keyspace watch survived client cancellation")
		}
	}

	// Cancellation must also interrupt the initial etcd load.
	ctx, cancel := context.WithCancel(suite.T().Context())
	cancel()
	result := make(chan error, 1)
	go func() {
		result <- service.WatchKeyspaces(request, &keyspaceWatchStream{ctx: ctx, snapshot: make(chan struct{}, 1)})
	}()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		re.FailNow("keyspace watch survived cancellation during initial loading")
	}

	// A live client must not keep its watcher alive after the server shuts down.
	stream := &keyspaceWatchStream{ctx: suite.T().Context(), snapshot: make(chan struct{}, 1)}
	go func() { result <- service.WatchKeyspaces(request, stream) }()
	select {
	case <-stream.snapshot:
	case <-time.After(5 * time.Second):
		re.FailNow("initial keyspace snapshot was not sent")
	}
	suite.cancel()
	select {
	case err := <-result:
		re.NoError(err)
	case <-time.After(5 * time.Second):
		re.FailNow("keyspace watch survived server shutdown")
	}
}

func (suite *keyspaceTestSuite) TestLoadKeyspaceByIDGRPC() {
	re := suite.Require()
	service := &server.KeyspaceServer{
		GrpcServer: &server.GrpcServer{Server: suite.server.GetServer()},
	}
	ctx := context.Background()
	const (
		keyspaceName     = "grpc_load_by_id"
		encryptionConfig = "test-encryption-config"
	)

	created, err := suite.manager.CreateKeyspace(&keyspacepkg.CreateKeyspaceRequest{
		Name: keyspaceName,
		Config: map[string]string{
			"encryption": encryptionConfig,
		},
	})
	re.NoError(err)

	_, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID() + 1), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId()}})
	re.Error(err)

	resp, err := service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId() + 1000}})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	disabled, err := suite.manager.UpdateKeyspaceStateByID(
		created.GetId(), keyspacepb.KeyspaceState_DISABLED, 1,
	)
	re.NoError(err)
	re.False(suite.manager.CheckKeyspaceRegionBound(disabled))
	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{
		Header: testutil.NewRequestHeader(suite.server.GetClusterID()),
		Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{
			Id: disabled.GetId(),
		},
	})
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(disabled, resp.GetKeyspace())
	re.Equal(keyspacepb.KeyspaceState_DISABLED, resp.GetKeyspace().GetState())
	re.Equal(encryptionConfig, resp.GetKeyspace().GetConfig()["encryption"])

	resp, err = service.LoadKeyspace(ctx, &keyspacepb.LoadKeyspaceRequest{
		Header: testutil.NewRequestHeader(suite.server.GetClusterID()),
		Name:   keyspaceName,
	})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())
}
