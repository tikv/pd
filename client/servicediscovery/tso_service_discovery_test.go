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

package servicediscovery

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/tsopb"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
)

type legacyFindGroupServer struct {
	tsopb.UnimplementedTSOServer
	requests chan *tsopb.FindGroupByKeyspaceIDRequest
	response *tsopb.FindGroupByKeyspaceIDResponse
	err      error
}

func (s *legacyFindGroupServer) FindGroupByKeyspaceID(
	_ context.Context,
	request *tsopb.FindGroupByKeyspaceIDRequest,
) (*tsopb.FindGroupByKeyspaceIDResponse, error) {
	s.requests <- request
	return s.response, s.err
}

func startLegacyFindGroupServer(t *testing.T, response *tsopb.FindGroupByKeyspaceIDResponse, err error) (
	serverURL string,
	calleeID string,
	server *legacyFindGroupServer,
) {
	t.Helper()
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, listenErr)
	grpcServer := grpc.NewServer()
	server = &legacyFindGroupServer{
		requests: make(chan *tsopb.FindGroupByKeyspaceIDRequest, 1),
		response: response,
		err:      err,
	}
	tsopb.RegisterTSOServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)
	return "http://" + listener.Addr().String(), listener.Addr().String(), server
}

func newLegacyTSOServiceDiscovery(t *testing.T, clusterID uint64) *tsoServiceDiscovery {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	discovery := &tsoServiceDiscovery{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		option:    opt.NewOption(),
	}
	t.Cleanup(discovery.Close)
	return discovery
}

func TestFindGroupByKeyspaceIDLegacyProtocol(t *testing.T) {
	const (
		clusterID   = uint64(1)
		keyspaceID  = uint32(2)
		modRevision = uint64(10)
	)
	tests := []struct {
		name                  string
		response              *tsopb.FindGroupByKeyspaceIDResponse
		rpcErr                error
		wantGroup             *tsopb.KeyspaceGroup
		wantRevision          uint64
		wantErr               string
		wantConnectionRemoved bool
	}{
		{
			name: "success",
			response: &tsopb.FindGroupByKeyspaceIDResponse{
				Header:        &tsopb.ResponseHeader{},
				KeyspaceGroup: &tsopb.KeyspaceGroup{Id: 3},
				ModRevision:   11,
			},
			wantGroup:    &tsopb.KeyspaceGroup{Id: 3},
			wantRevision: 11,
		},
		{
			name:    "rpc error",
			rpcErr:  status.Error(codes.Unavailable, "tso unavailable"),
			wantErr: "tso unavailable",
		},
		{
			name: "callee mismatch",
			response: &tsopb.FindGroupByKeyspaceIDResponse{
				Header: &tsopb.ResponseHeader{
					Error: &tsopb.Error{Message: errs.MismatchCalleeIDErr},
				},
			},
			wantErr:               errs.MismatchCalleeIDErr,
			wantConnectionRemoved: true,
		},
		{
			name: "missing keyspace group",
			response: &tsopb.FindGroupByKeyspaceIDResponse{
				Header:      &tsopb.ResponseHeader{},
				ModRevision: modRevision,
			},
			wantErr: "no keyspace group found",
		},
		{
			name: "stale revision",
			response: &tsopb.FindGroupByKeyspaceIDResponse{
				Header:        &tsopb.ResponseHeader{},
				KeyspaceGroup: &tsopb.KeyspaceGroup{Id: 3},
				ModRevision:   modRevision - 1,
			},
			wantErr: "response mod revision less than the given mod revision",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			serverURL, calleeID, server := startLegacyFindGroupServer(t, test.response, test.rpcErr)
			discovery := newLegacyTSOServiceDiscovery(t, clusterID)

			group, revision, err := discovery.findGroupByKeyspaceID(
				keyspaceID,
				serverURL,
				time.Second,
				modRevision,
			)

			request := <-server.requests
			re.Equal(clusterID, request.GetHeader().GetClusterId())
			re.IsType(&tsopb.RequestHeader_KeyspaceId{}, request.GetHeader().GetKeyspace())
			re.Equal(keyspaceID, request.GetHeader().GetKeyspaceId())
			re.Equal(constants.DefaultKeyspaceGroupID, request.GetHeader().GetKeyspaceGroupId())
			re.Equal(calleeID, request.GetHeader().GetCalleeId())
			re.IsType(&tsopb.FindGroupByKeyspaceIDRequest_KeyspaceId{}, request.GetKeyspace())
			re.Equal(keyspaceID, request.GetKeyspaceId())
			re.Equal(modRevision, request.GetModRevision())

			if test.wantErr == "" {
				re.NoError(err)
				re.Equal(test.wantGroup, group)
				re.Equal(test.wantRevision, revision)
			} else {
				re.ErrorContains(err, test.wantErr)
				re.Nil(group)
				re.Zero(revision)
			}
			_, connectionExists := discovery.clientConns.Load(serverURL)
			re.Equal(!test.wantConnectionRemoved, connectionExists)
		})
	}
}

func TestKeyspaceGroupSvcDiscoveryUpdateKeepsPrimary(t *testing.T) {
	re := require.New(t)
	originalGroup := &tsopb.KeyspaceGroup{Id: 1}
	newGroup := &tsopb.KeyspaceGroup{Id: 2}
	k := &keyspaceGroupSvcDiscovery{
		group:         originalGroup,
		primaryURL:    "http://primary-1",
		secondaryURLs: []string{"http://secondary-1"},
		urls:          []string{"http://primary-1", "http://secondary-1"},
		modRevision:   atomic.Uint64{},
	}
	k.modRevision.Store(10)

	oldPrimaryURL, primarySwitched, metaChanged := k.update(
		newGroup,
		"http://primary-2",
		[]string{"http://secondary-2"},
		[]string{"http://primary-2", "http://secondary-2"},
		9,
	)

	re.Empty(oldPrimaryURL)
	re.False(primarySwitched)
	re.False(metaChanged)
	re.Equal("http://primary-1", k.primaryURL)
	re.Equal([]string{"http://secondary-1"}, k.secondaryURLs)
	re.Equal([]string{"http://primary-1", "http://secondary-1"}, k.urls)
	re.Same(originalGroup, k.group)
	re.Equal(uint64(10), k.getModRevision())
}

func TestKeyspaceGroupSvcDiscoveryUpdateUpdatesAllFieldsOnFreshRevision(t *testing.T) {
	re := require.New(t)
	newGroup := &tsopb.KeyspaceGroup{Id: 2}
	k := &keyspaceGroupSvcDiscovery{
		primaryURL:    "http://primary-1",
		secondaryURLs: []string{"http://secondary-1"},
		urls:          []string{"http://primary-1", "http://secondary-1"},
		modRevision:   atomic.Uint64{},
	}
	k.modRevision.Store(10)

	oldPrimaryURL, primarySwitched, metaChanged := k.update(
		newGroup,
		"http://primary-2",
		[]string{"http://secondary-2"},
		[]string{"http://primary-2", "http://secondary-2"},
		11,
	)

	re.Equal("http://primary-1", oldPrimaryURL)
	re.True(primarySwitched)
	re.True(metaChanged)
	re.Equal("http://primary-2", k.primaryURL)
	re.Equal([]string{"http://secondary-2"}, k.secondaryURLs)
	re.Equal([]string{"http://primary-2", "http://secondary-2"}, k.urls)
	re.Same(newGroup, k.group)
	re.Equal(uint64(11), k.getModRevision())
}
