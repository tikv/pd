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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestGlobalGCBarrierTSFloor(t *testing.T) {
	re := require.New(t)
	var reads []*etcdserverpb.TxnRequest
	se := newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn, ok := req.(*etcdserverpb.TxnRequest); ok && len(txn.Success) > 0 && txn.Success[0].GetRequestRange() != nil {
			reads = append(reads, txn)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	provider := se.GetGCStateProvider()
	local, groups := keypath.LegacyLocalTSOPrefix(), keypath.TSOMicroservicePrefix()
	for _, unrelated := range []string{
		keypath.KeyspaceMetaPath(3), keypath.TxnSafePointPath(7),
		fmt.Sprintf("/pd/%d/timestamp", keypath.ClusterID()+1),
		local + "dc1//extra/timestamp", local + "/timestamp", local + "dc1/timestamp_metadata",
		local + "region/./zone/timestamp", local + "region/../zone/timestamp",
		groups + "00000/gta/timestamp", groups + "000+1/gta/timestamp",
		groups + "100000/gta/timestamp", groups + "00342/other/timestamp",
		groups + "00342/lta//timestamp", groups + "00342/lta/dc//a/timestamp",
		groups + "00342/lta/dc/../zone/timestamp", groups + "00342/gta/dc/zone/timestamp",
		groups + "keyspace_groups/election/00342/primary",
	} {
		re.NoError(se.Save(unrelated, "unrelated value"))
	}
	unifiedKey := keypath.TxnSafePointPath(constant.NullKeyspaceID)
	re.NoError(se.Save(unifiedKey, "1"))
	reads = nil
	_, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
	re.ErrorContains(err, "no persisted TSO window")
	// One snapshot scans only the two window ranges, independent of keyspace count.
	re.Len(reads, 1)
	var ranges [][2]string
	for _, op := range reads[0].Success {
		r := op.GetRequestRange()
		re.NotNil(r)
		re.False(r.Serializable)
		if len(r.RangeEnd) > 0 {
			ranges = append(ranges, [2]string{string(r.Key), string(r.RangeEnd)})
		}
	}
	re.ElementsMatch([][2]string{{local, clientv3.GetPrefixRangeEnd(local)}, {groups + "00001/", groups + ":"}}, ranges)

	ms := uint64(time.Millisecond)
	for _, fixture := range []struct {
		key   string
		nanos uint64
		want  int64
	}{
		{keypath.TimestampPath(0), ms, 1},
		{local + "east/timestamp", 3 * ms, 3},
		{groups + "00342/lta/west/timestamp", 4*ms - 1, 4},
		{local + "region/east/zone/timestamp", 5 * ms, 5},
		{groups + "00342/lta/region/zone/timestamp", 5*ms + 1, 6},
		// A deleted group has only its original window key left. It can still set the floor.
		{keypath.TimestampPath(4), math.MaxInt64, math.MaxInt64/int64(time.Millisecond) + 1},
	} {
		re.NoError(se.Save(fixture.key, string(typeutil.Uint64ToBytes(fixture.nanos))))
		floor, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
		re.NoError(err)
		re.Equal(tsoutil.ComposeTS(fixture.want, 0), floor)
	}
	for key, invalid := range map[string]string{
		keypath.TimestampPath(0):            "",
		keypath.TimestampPath(4):            "bad",
		local + "east/timestamp":            string(typeutil.Uint64ToBytes(0)),
		groups + "00342/lta/west/timestamp": string(typeutil.Uint64ToBytes(math.MaxUint64)),
	} {
		re.NoError(se.Save(key, invalid))
		floor, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
		re.ErrorContains(err, key)
		re.Zero(floor)
		re.NoError(se.Remove(key))
	}
	re.NoError(se.Save(unifiedKey, "bad"))
	_, err = provider.LoadGlobalGCBarrierTSFloor(t.Context())
	re.ErrorContains(err, unifiedKey)
}
