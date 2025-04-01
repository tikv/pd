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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// TSOStorage is the interface for timestamp storage.
type TSOStorage interface {
	LoadTimestamp(groupID uint32) (time.Time, error)
	SaveTimestamp(groupID uint32, ts time.Time) error
	DeleteTimestamp(groupID uint32) error
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

// SaveTimestamp saves the timestamp to the storage.
func (se *StorageEndpoint) SaveTimestamp(groupID uint32, ts time.Time) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		value, err := txn.Load(keypath.TimestampPath(groupID))
		if err != nil {
			return err
		}

		previousTS := typeutil.ZeroTime
		if value != "" {
			previousTS, err = typeutil.ParseTimestamp([]byte(value))
			if err != nil {
				log.Error("parse timestamp failed", zap.Uint32("group-id", groupID), zap.String("value", value), zap.Error(err))
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

// DeleteTimestamp deletes the timestamp from the storage.
func (se *StorageEndpoint) DeleteTimestamp(groupID uint32) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return txn.Remove(keypath.TimestampPath(groupID))
	})
}
