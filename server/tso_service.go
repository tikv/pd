// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"context"
	"strings"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TsoServer wraps GrpcServer to provide TSO service.
type TsoServer struct {
	*GrpcServer
}

// LoadTimestamp loads the timestamp from etcd according to the given key.
func (s *TsoServer) LoadTimestamp(_ context.Context, request *tsopb.LoadTimestampRequest) (*tsopb.LoadTimestampResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	client := s.GetClient()
	if client == nil {
		return &tsopb.LoadTimestampResponse{Header: s.notBootstrappedHeader()}, nil
	}

	resp, err := etcdutil.EtcdKVGet(
		client,
		request.Key,
		clientv3.WithPrefix())
	if err != nil {
		return &tsopb.LoadTimestampResponse{Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error())}, nil
	}
	maxTSWindow := typeutil.ZeroTime
	for _, kv := range resp.Kvs {
		key := strings.TrimSpace(string(kv.Key))
		if !strings.HasSuffix(key, "timestamp") {
			continue
		}
		tsWindow, err := typeutil.ParseTimestamp(kv.Value)
		if err != nil {
			log.Error("parse timestamp window that from etcd failed", zap.String("ts-window-key", key), zap.Time("max-ts-window", maxTSWindow), zap.Error(err))
			continue
		}
		if typeutil.SubRealTimeByWallClock(tsWindow, maxTSWindow) > 0 {
			maxTSWindow = tsWindow
		}
	}

	return &tsopb.LoadTimestampResponse{
		Header:    s.header(),
		Timestamp: uint64(maxTSWindow.UnixNano()),
	}, nil
}

// SaveTimestamp saves the timestamp to etcd according to the given key.
func (s *TsoServer) SaveTimestamp(_ context.Context, request *tsopb.SaveTimestampRequest) (*tsopb.SaveTimestampResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	client := s.GetClient()
	if client == nil {
		return &tsopb.SaveTimestampResponse{Header: s.notBootstrappedHeader()}, nil
	}
	key := request.GetKey()
	data := typeutil.Uint64ToBytes(request.GetTimestamp())
	var cmps []clientv3.Cmp
	if !request.GetSkipCheck() {
		lastData := typeutil.Uint64ToBytes(request.GetLastTimestamp())
		cmps = append(cmps, clientv3.Compare(clientv3.Value(key), "=", string(lastData)))
	}
	txn := kv.NewSlowLogTxn(client)
	resp, err := txn.
		If(cmps...).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return &tsopb.SaveTimestampResponse{Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error())}, nil
	}
	if !resp.Succeeded {
		return &tsopb.SaveTimestampResponse{Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, "failed to save timestamp")}, nil
	}
	return &tsopb.SaveTimestampResponse{Header: s.header()}, nil
}
