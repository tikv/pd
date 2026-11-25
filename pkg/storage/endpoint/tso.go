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

package endpoint

import (
	"context"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// TSOStorage is the interface for timestamp storage.
type TSOStorage interface {
	LoadTimestamp(groupID uint32) (time.Time, error)
	SaveTimestamp(ctx context.Context, groupID uint32, ts time.Time, leadership *election.Leadership) error
	DeleteTimestamp(ctx context.Context, groupID uint32) error
}

var _ TSOStorage = (*StorageEndpoint)(nil)

// LoadTimestamp retrieves the last saved TSO timestamp from etcd.
// Before switching back from the TSO microservice to the PD leader,
// we must ensure that all keyspace groups are merged into the default
// keyspace group. This guarantees the monotonicity of the TSO by loading
// the timestamp from a single key.
func (se *StorageEndpoint) LoadTimestamp(groupID uint32) (time.Time, error) {
	key := keypath.TimestampPath(groupID)
	value, err := se.Load(key)
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(value) == 0 {
		return typeutil.ZeroTime, nil
	}
	logFields := []zap.Field{
		zap.String("ts-window-key", key),
		zap.String("ts-window-value", value),
	}
	tsWindow, err := typeutil.ParseTimestamp([]byte(value))
	if err != nil {
		log.Error("parse timestamp window that from etcd failed", append(logFields, zap.Error(err))...)
		return typeutil.ZeroTime, err
	}
	log.Info("load timestamp window successfully", append(logFields, zap.Time("ts-window", tsWindow))...)
	return tsWindow, nil
}

// SaveTimestamp saves the timestamp to the storage. The leadership is used to check if the current server is leader
// before saving the timestamp to ensure a strong consistency for persistence of the TSO timestamp window.
func (se *StorageEndpoint) SaveTimestamp(ctx context.Context, groupID uint32, ts time.Time, leadership *election.Leadership) error {
	logFilds := []zap.Field{
		zap.Uint32("group-id", groupID),
		zap.Time("ts", ts),
		zap.String("leader-key", leadership.GetLeaderKey()),
		zap.String("expected-leader-value", leadership.GetLeaderValue()),
	}
	log.Debug("saving timestamp to the storage", logFilds...)
	// The PD leadership or TSO primary will always be granted first before the TSO timestamp window is saved.
	// So we here check whether the leader value is filled to see if the requirement is met.
	if len(leadership.GetLeaderValue()) == 0 {
		return errors.Errorf("%s due to leadership has not been granted yet", errs.NotLeaderErr)
	}
	return se.RunInTxn(ctx, func(txn kv.Txn) error {
		// Ensure the current server is leader by reading and comparing the leader value.
		leaderValue, err := txn.Load(leadership.GetLeaderKey())
		if err != nil {
			return err
		}
		if expected := leadership.GetLeaderValue(); leaderValue != expected {
			log.Error("leader value does not match", append(logFilds, zap.String("current-leader-value", leaderValue))...)
			return errors.Errorf("%s due to leader value does not match, current: %s, expected: %s", errs.NotLeaderErr, leaderValue, expected)
		}

		value, err := txn.Load(keypath.TimestampPath(groupID))
		if err != nil {
			return err
		}

		previousTS := typeutil.ZeroTime
		if value != "" {
			previousTS, err = typeutil.ParseTimestamp([]byte(value))
			if err != nil {
				log.Error("parse timestamp failed", append(logFilds, zap.String("value", value), zap.Error(err))...)
				return err
			}
		}
		if previousTS != typeutil.ZeroTime && typeutil.SubRealTimeByWallClock(ts, previousTS) <= 0 {
			return errors.Errorf("saving timestamp %d is less than or equal to the previous one %d", ts.UnixNano(), previousTS.UnixNano())
		}
		data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
		return txn.Save(keypath.TimestampPath(groupID), string(data))
	})
}

// DeleteTimestamp retires the group window atomically so that a global GC barrier
// still covers timestamps issued by a group after its allocator has been removed.
func (se *StorageEndpoint) DeleteTimestamp(ctx context.Context, groupID uint32) error {
	groupKey, retiredKey := keypath.TimestampPath(groupID), keypath.RetiredTSOWindowPath()
	return se.RunInTxn(ctx, func(txn kv.Txn) error {
		window, err := loadTSOWindowInTxn(txn, groupKey)
		if err != nil || window == 0 {
			return err
		}
		retired, err := loadTSOWindowInTxn(txn, retiredKey)
		if err != nil {
			return err
		}
		if window > retired {
			if err := txn.Save(retiredKey, string(typeutil.Uint64ToBytes(window))); err != nil {
				return err
			}
		}
		return txn.Remove(groupKey)
	})
}

func loadTSOWindowInTxn(txn kv.Txn, key string) (uint64, error) {
	// Load pins the value (or its absence) until commit. Its string result alone
	// cannot distinguish a missing record from a malformed, present empty record.
	value, err := txn.Load(key)
	if err != nil {
		return 0, err
	}
	if value == "" {
		keys, _, err := txn.LoadRange(key, key+"\x00", 1)
		if err != nil || len(keys) == 0 {
			return 0, err
		}
	}
	return decodeTSOWindow(key, value)
}

// Window records store positive Unix nanoseconds as eight big-endian bytes.
func decodeTSOWindow(key, value string) (uint64, error) {
	nanos, err := typeutil.BytesToUint64([]byte(value))
	if err != nil {
		return 0, errors.Annotatef(err, "invalid TSO window at %q", key)
	}
	if nanos == 0 || nanos > math.MaxInt64 {
		return 0, errors.Errorf("invalid TSO window at %q: expected positive Unix nanoseconds", key)
	}
	return nanos, nil
}

// LoadGlobalGCBarrierTSFloor returns the greater of the persisted TSO window
// bound (including retired groups and legacy per-DC allocators) and the unified
// txn safe point, which can outlive the group that issued its timestamp.
func (p GCStateProvider) LoadGlobalGCBarrierTSFloor(ctx context.Context) (uint64, error) {
	defaultKey, retiredKey := keypath.TimestampPath(constant.DefaultKeyspaceGroupID), keypath.RetiredTSOWindowPath()
	unifiedKey := keypath.TxnSafePointPath(constant.NullKeyspaceID)
	localPrefix, groupPrefix := keypath.LegacyLocalTSOPrefix(), keypath.TSOMicroservicePrefix()
	groupStart, groupEnd := keypath.TSOGroupWindowRange()
	// Read the safe point and windows at the same etcd revision.
	ops := []kv.RawTxnOp{
		{OpType: kv.RawTxnOpGet, Key: unifiedKey},
		{OpType: kv.RawTxnOpGet, Key: defaultKey},
		{OpType: kv.RawTxnOpGet, Key: retiredKey},
		{OpType: kv.RawTxnOpGetRange, Key: localPrefix, EndKey: "\x00"},
		{OpType: kv.RawTxnOpGetRange, Key: groupStart, EndKey: groupEnd},
	}
	txn, err := p.storage.createRawTxnWithContext(ctx)
	if err != nil {
		return 0, err
	}
	result, err := txn.Then(ops...).Commit()
	if err != nil {
		return 0, err
	}
	if !result.Succeeded || len(result.Responses) != len(ops) {
		return 0, errors.New("invalid TSO window snapshot response")
	}
	var unifiedTxnSafePoint uint64
	if pairs := result.Responses[0].KeyValuePairs; len(pairs) > 0 {
		unifiedTxnSafePoint, err = decodeTxnSafePoint(pairs[0].Value)
		if err != nil {
			return 0, errors.Annotatef(err, "invalid unified txn safe point at %q", unifiedKey)
		}
	}
	var largest uint64
	for _, item := range result.Responses[1:] {
		for _, pair := range item.KeyValuePairs {
			isWindow := pair.Key == defaultKey || pair.Key == retiredKey ||
				keypath.IsLocalTSOWindowPath(strings.TrimPrefix(pair.Key, localPrefix)) ||
				keypath.IsTSOGroupWindowPath(strings.TrimPrefix(pair.Key, groupPrefix))
			if !isWindow {
				continue
			}
			nanos, err := decodeTSOWindow(pair.Key, pair.Value)
			if err != nil {
				return 0, err
			}
			largest = max(largest, nanos)
		}
	}
	if largest == 0 {
		return 0, errors.New("no persisted TSO window available for a global GC barrier")
	}
	// Round the maximal physical window up to a millisecond boundary before
	// composing logical zero. Avoid overflowing when rounding a large UnixNano.
	millis := (largest-1)/uint64(time.Millisecond) + 1
	return max(tsoutil.ComposeTS(int64(millis), 0), unifiedTxnSafePoint), nil
}
