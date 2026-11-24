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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
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
	// One read-only transaction covers the point keys and the two narrow TSO
	// ranges. In particular, keyspace metadata and membership are never scanned.
	re.Len(reads, 1)
	re.Empty(reads[0].Compare)
	re.Len(reads[0].Success, 5)
	for i, bounds := range [][2]string{
		{unifiedKey, ""}, {keypath.TimestampPath(0), ""}, {keypath.RetiredTSOWindowPath(), ""},
		{local, clientv3.GetPrefixRangeEnd(local)}, {groups + "00001/", groups + ":"},
	} {
		r := reads[0].Success[i].GetRequestRange()
		re.NotNil(r)
		re.Equal(bounds[0], string(r.Key))
		re.Equal(bounds[1], string(r.RangeEnd))
		re.False(r.Serializable)
	}

	ms := uint64(time.Millisecond)
	for _, fixture := range []struct {
		key   string
		nanos uint64
		want  int64
	}{
		{keypath.TimestampPath(0), ms, 1},
		{keypath.TimestampPath(4), ms + 1, 2},
		{local + "east/timestamp", 3 * ms, 3},
		{groups + "00342/lta/west/timestamp", 4*ms - 1, 4},
		{local + "region/east/zone/timestamp", 5 * ms, 5},
		{groups + "00342/lta/region/zone/timestamp", 5*ms + 1, 6},
		{keypath.RetiredTSOWindowPath(), math.MaxInt64, math.MaxInt64/int64(time.Millisecond) + 1},
	} {
		re.NoError(se.Save(fixture.key, string(typeutil.Uint64ToBytes(fixture.nanos))))
		floor, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
		re.NoError(err)
		re.Equal(tsoutil.ComposeTS(fixture.want, 0), floor)
	}
	for _, key := range []string{
		keypath.TimestampPath(0), keypath.RetiredTSOWindowPath(), keypath.TimestampPath(4),
		local + "region/east/zone/timestamp", groups + "00342/lta/region/zone/timestamp",
	} {
		for _, value := range []string{"", "bad", string(typeutil.Uint64ToBytes(0)), string(typeutil.Uint64ToBytes(math.MaxUint64))} {
			re.NoError(se.Save(key, value))
			floor, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
			re.ErrorContains(err, key)
			re.Zero(floor)
		}
		re.NoError(se.Remove(key))
	}
	re.NoError(se.Save(unifiedKey, "bad"))
	_, err = provider.LoadGlobalGCBarrierTSFloor(t.Context())
	re.ErrorContains(err, unifiedKey)
}

func TestDeleteTimestampRetainsWindowOnConflict(t *testing.T) {
	re := require.New(t)
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	se := newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		txn, ok := req.(*etcdserverpb.TxnRequest)
		// Pause both commits after reading the same absent retired window.
		if ok && len(txn.Compare) > 0 && len(txn.Success) > 0 && txn.Success[len(txn.Success)-1].GetRequestDeleteRange() != nil {
			select {
			case <-release:
			default:
				arrived <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	provider := se.GetGCStateProvider()
	errsByGroup := make(chan error, 2)
	for group := uint32(1); group <= 2; group++ {
		re.NoError(se.Save(keypath.TimestampPath(group), string(typeutil.Uint64ToBytes(uint64(group)*uint64(time.Millisecond)))))
		go func(group uint32) { errsByGroup <- se.DeleteTimestamp(t.Context(), group) }(group)
	}
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent timestamp deletions did not reach commit")
		}
	}
	unblock()
	var conflicts int
	for range 2 {
		if err := <-errsByGroup; err != nil {
			re.True(errors.ErrorEqual(err, errs.ErrEtcdTxnConflict), "unexpected deletion error %v", err)
			conflicts++
		}
	}
	re.Equal(1, conflicts)
	floor, err := provider.LoadGlobalGCBarrierTSFloor(t.Context())
	re.NoError(err)
	re.Equal(tsoutil.ComposeTS(2, 0), floor)
	for _, id := range []uint32{1, 2, 1, 2} {
		re.NoError(se.DeleteTimestamp(t.Context(), id))
		window, err := se.LoadTimestamp(id)
		re.NoError(err)
		re.True(window.IsZero())
	}
	floor, err = provider.LoadGlobalGCBarrierTSFloor(t.Context())
	re.NoError(err)
	re.Equal(tsoutil.ComposeTS(2, 0), floor)
}

func TestDeleteTimestampRejectsStaleSourceWindow(t *testing.T) {
	re := require.New(t)
	const groupID = 1
	groupKey, retiredKey := keypath.TimestampPath(groupID), keypath.RetiredTSOWindowPath()
	newWindow := string(typeutil.Uint64ToBytes(uint64(2 * time.Millisecond)))
	var se *StorageEndpoint
	advanceWindow := sync.OnceValue(func() error { return se.Save(groupKey, newWindow) })
	se = newGCBatchTestStorage(t, func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		txn, _ := req.(*etcdserverpb.TxnRequest)
		for _, op := range txn.GetSuccess() {
			if string(op.GetRequestDeleteRange().GetKey()) == groupKey {
				// Advance the source after DeleteTimestamp's reads and before etcd evaluates its comparisons.
				if err := advanceWindow(); err != nil {
					return err
				}
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	checkWindows := func(presentKey, absentKey string) {
		t.Helper()
		value, err := se.Load(presentKey)
		re.NoError(err)
		re.Equal(newWindow, value)
		keys, _, err := se.LoadRange(absentKey, absentKey+"\x00", 1)
		re.NoError(err)
		re.Empty(keys)
	}

	re.NoError(se.Save(groupKey, string(typeutil.Uint64ToBytes(uint64(time.Millisecond)))))
	re.ErrorIs(se.DeleteTimestamp(t.Context(), groupID), errs.ErrEtcdTxnConflict)
	checkWindows(groupKey, retiredKey)

	re.NoError(se.DeleteTimestamp(t.Context(), groupID))
	checkWindows(retiredKey, groupKey)
}

func TestDeleteTimestampRejectsInvalidWindow(t *testing.T) {
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	re := require.New(t)
	groupKey, retiredKey := keypath.TimestampPath(2), keypath.RetiredTSOWindowPath()
	valid := string(typeutil.Uint64ToBytes(uint64(time.Millisecond)))
	for _, key := range []string{groupKey, retiredKey} {
		for _, invalid := range []string{"", "bad"} {
			re.NoError(se.Save(groupKey, valid))
			re.NoError(se.Save(retiredKey, valid))
			re.NoError(se.Save(key, invalid))
			re.ErrorContains(se.DeleteTimestamp(t.Context(), 2), key)
			for _, wantKey := range []string{groupKey, retiredKey} {
				keys, values, err := se.LoadRange(wantKey, wantKey+"\x00", 1)
				re.NoError(err)
				re.Equal([]string{wantKey}, keys)
				if wantKey == key {
					re.Equal([]string{invalid}, values)
				} else {
					re.Equal([]string{valid}, values)
				}
			}
		}
	}
}
