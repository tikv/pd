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

package endpoint

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func newGCBatchTestStorage(t *testing.T, interceptor grpc.UnaryClientInterceptor) *StorageEndpoint {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
		ClientCfgModifier: func(config *clientv3.Config) {
			config.DialOptions = append(config.DialOptions, grpc.WithChainUnaryInterceptor(interceptor))
		},
	})
	t.Cleanup(clean)
	return NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
}

func TestLoadGCSafePointPairs(t *testing.T) {
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	re := require.New(t)
	provider := se.GetGCStateProvider()
	fixtures := []struct {
		id      uint32
		txn, gc string
	}{
		{constant.NullKeyspaceID, "84", "2a"},
		{7, "126", `{"keyspace_id":999,"safe_point":63}`},
		{0, "", ""},
		{11, "invalid", `{"safe_point":71}`},
		{12, "144", `{"safe_point":`},
		{14, "18446744073709551615", `{"safe_point":18446744073709551615}`},
	}
	for _, fixture := range fixtures {
		re.NoError(se.Save(keypath.TxnSafePointPath(fixture.id), fixture.txn))
		re.NoError(se.Save(keypath.GCSafePointPath(fixture.id), fixture.gc))
	}
	re.NoError(se.Save(keypath.TxnSafePointPath(13), "156"))
	ids := []uint32{11, constant.NullKeyspaceID, 9, 7, 12, 0, 13, 14}
	results, err := provider.LoadGCSafePointPairs(context.Background(), ids)
	re.NoError(err)
	re.Len(results, len(ids))
	expectedTxn := []uint64{0, 84, 0, 126, 0, 0, 156, math.MaxUint64}
	expectedGC := []uint64{0, 42, 0, 63, 0, 0, 0, math.MaxUint64}
	for i, result := range results {
		re.Equal(ids[i], result.KeyspaceID)
		if ids[i] == 11 || ids[i] == 12 {
			re.Error(result.Err)
			continue
		}
		re.NoError(result.Err)
		re.Equal(expectedTxn[i], result.TxnSafePoint)
		re.Equal(expectedGC[i], result.GCSafePoint)
		txn, err := provider.LoadTxnSafePoint(ids[i])
		re.NoError(err)
		gc, err := provider.LoadGCSafePoint(ids[i])
		re.NoError(err)
		re.Equal(txn, result.TxnSafePoint)
		re.Equal(gc, result.GCSafePoint)
	}

	// All three encodings preserve empty values and reject nonempty malformed values.
	for _, value := range []string{"", "not-a-safe-point", "18446744073709551616"} {
		t.Run(fmt.Sprintf("null/%q", value), func(t *testing.T) {
			re := require.New(t)
			re.NoError(se.Save(keypath.GCSafePointPath(constant.NullKeyspaceID), value))
			results, err := provider.LoadGCSafePointPairs(context.Background(), []uint32{constant.NullKeyspaceID, 7})
			re.NoError(err)
			re.Len(results, 2)
			if value == "" {
				re.NoError(results[0].Err)
				re.Zero(results[0].GCSafePoint)
			} else {
				re.Error(results[0].Err)
			}
			re.NoError(results[1].Err)
			re.Equal(uint64(63), results[1].GCSafePoint)
		})
	}
	re.NoError(se.Remove(keypath.GCSafePointPath(constant.NullKeyspaceID)))
	re.NoError(se.Remove(keypath.TxnSafePointPath(constant.NullKeyspaceID)))
	results, err = provider.LoadGCSafePointPairs(context.Background(), []uint32{constant.NullKeyspaceID})
	re.NoError(err)
	re.Equal([]GCSafePointReadResult{{KeyspaceID: constant.NullKeyspaceID}}, results)
	re.NoError(se.Save(keypath.TxnSafePointPath(constant.NullKeyspaceID), ""))
	results, err = provider.LoadGCSafePointPairs(context.Background(), []uint32{constant.NullKeyspaceID})
	re.NoError(err)
	re.Equal([]GCSafePointReadResult{{KeyspaceID: constant.NullKeyspaceID}}, results)
}

func TestLoadGCSafePointPairsBatchBounds(t *testing.T) {
	re := require.New(t)
	var methods []string
	var requests []*etcdserverpb.TxnRequest
	se := newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		methods = append(methods, method)
		if request, ok := req.(*etcdserverpb.TxnRequest); ok {
			requests = append(requests, request)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	provider := se.GetGCStateProvider()
	re.Equal(60, MaxGCSafePointBatchSize)
	ids := make([]uint32, 60)
	for i := range ids {
		ids[i] = uint32(i * 101)
		re.NoError(se.Save(keypath.TxnSafePointPath(ids[i]), strconv.Itoa(i+100)))
		re.NoError(se.Save(keypath.GCSafePointPath(ids[i]), fmt.Sprintf(`{"safe_point":%d}`, i+50)))
	}
	methods, requests = nil, nil
	for _, input := range [][]uint32{nil, {}} {
		results, err := provider.LoadGCSafePointPairs(context.Background(), input)
		re.NoError(err)
		re.Empty(results)
		re.Empty(methods)
	}
	for _, input := range [][]uint32{append(append([]uint32{}, ids...), 6001), {7, 7}} {
		results, err := provider.LoadGCSafePointPairs(context.Background(), input)
		re.Error(err)
		re.Empty(results)
		re.Empty(methods)
	}
	results, err := provider.LoadGCSafePointPairs(context.Background(), ids)
	re.NoError(err)
	re.Len(results, 60)
	for i, result := range results {
		re.NoError(result.Err)
		re.Equal(ids[i], result.KeyspaceID)
		re.Equal(uint64(i+100), result.TxnSafePoint)
		re.Equal(uint64(i+50), result.GCSafePoint)
	}
	re.Equal([]string{"/etcdserverpb.KV/Txn"}, methods)
	re.Len(requests, 1)
	re.Empty(requests[0].Compare)
	re.Empty(requests[0].Failure)
	re.Len(requests[0].Success, 120)
	for _, op := range requests[0].Success {
		re.NotNil(op.GetRequestRange())
		re.Empty(op.GetRequestRange().RangeEnd)
	}
}

func TestLoadGCSafePointPairsUnsupported(t *testing.T) {
	provider := NewStorageEndpoint(kv.NewMemoryKV(), nil).GetGCStateProvider()
	results, err := provider.LoadGCSafePointPairs(context.Background(), []uint32{1})
	require.ErrorContains(t, err, "context")
	require.Empty(t, results)
}

func TestLoadGCSafePointPairsContext(t *testing.T) {
	started := make(chan struct{}, 1)
	se := newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method != "/etcdserverpb.KV/Txn" {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})
	provider := se.GetGCStateProvider()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := provider.LoadGCSafePointPairs(ctx, []uint32{1})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, results)
	select {
	case <-started:
	default:
	}

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := provider.LoadGCSafePointPairs(ctx, []uint32{1})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("read ignored cancellation")
	}
}

func TestLoadGCSafePointPairsInvalidResponse(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*etcdserverpb.TxnResponse)
	}{
		{"unsuccessful", func(resp *etcdserverpb.TxnResponse) { resp.Succeeded = false }},
		{"missing response", func(resp *etcdserverpb.TxnResponse) { resp.Responses = resp.Responses[:1] }},
		{"extra response", func(resp *etcdserverpb.TxnResponse) { resp.Responses = append(resp.Responses, resp.Responses[0]) }},
		{"wrong key", func(resp *etcdserverpb.TxnResponse) {
			resp.Responses[0].GetResponseRange().Kvs[0].Key = []byte("wrong-key")
		}},
		{"multiple keys", func(resp *etcdserverpb.TxnResponse) {
			r := resp.Responses[0].GetResponseRange()
			r.Kvs = append(r.Kvs, r.Kvs[0])
		}},
		{"wrong response type", func(resp *etcdserverpb.TxnResponse) {
			resp.Responses[0].Response = &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{}}
		}},
	}
	var corrupt func(*etcdserverpb.TxnResponse)
	se := newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && method == "/etcdserverpb.KV/Txn" && corrupt != nil {
			corrupt(reply.(*etcdserverpb.TxnResponse))
		}
		return err
	})
	require.NoError(t, se.Save(keypath.TxnSafePointPath(1), "10"))
	require.NoError(t, se.Save(keypath.GCSafePointPath(1), `{"safe_point":5}`))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			corrupt = tc.corrupt
			results, err := se.GetGCStateProvider().LoadGCSafePointPairs(context.Background(), []uint32{1})
			require.Error(t, err)
			require.Empty(t, results)
		})
	}
}
