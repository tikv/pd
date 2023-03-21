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
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TSOStorage is the interface for timestamp storage.
type TSOStorage interface {
	LoadTimestamp(prefix string) (time.Time, error)
	SaveTimestamp(prefix string, key string, ts time.Time) error
}

var _ TSOStorage = (*StorageEndpoint)(nil)

// LoadTimestamp will get all time windows of Local/Global TSOs from etcd and return the biggest one.
// For the Global TSO, loadTimestamp will get all Local and Global TSO time windows persisted in etcd and choose the biggest one.
// For the Local TSO, loadTimestamp will only get its own dc-location time window persisted before.
func (se *StorageEndpoint) LoadTimestamp(prefix string) (time.Time, error) {
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(keys) == 0 {
		return typeutil.ZeroTime, nil
	}

	maxTSWindow := typeutil.ZeroTime
	for i, key := range keys {
		key := strings.TrimSpace(key)
		if !strings.HasSuffix(key, timestampKey) {
			continue
		}
		tsWindow, err := typeutil.ParseTimestamp([]byte(values[i]))
		if err != nil {
			log.Error("parse timestamp window that from etcd failed", zap.String("ts-window-key", key), zap.Time("max-ts-window", maxTSWindow), zap.Error(err))
			continue
		}
		if typeutil.SubRealTimeByWallClock(tsWindow, maxTSWindow) > 0 {
			maxTSWindow = tsWindow
		}
	}
	return maxTSWindow, nil
}

// SaveTimestamp saves the timestamp to the storage.
func (se *StorageEndpoint) SaveTimestamp(prefix string, key string, ts time.Time) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
		keys, values, err := txn.LoadRange(prefix, prefixEnd, 0)
		if err != nil {
			return err
		}

		previousTS := typeutil.ZeroTime
		for i, key := range keys {
			key := strings.TrimSpace(key)
			if !strings.HasSuffix(key, timestampKey) {
				continue
			}
			tsWindow, err := typeutil.ParseTimestamp([]byte(values[i]))
			if err != nil {
				continue
			}
			if typeutil.SubRealTimeByWallClock(tsWindow, previousTS) > 0 {
				previousTS = tsWindow
			}
		}
		if previousTS != typeutil.ZeroTime && typeutil.SubRealTimeByWallClock(ts, previousTS) <= 0 {
			log.Warn("save timestamp failed, the timestamp is not bigger than the previous one", zap.Time("previous", previousTS), zap.Time("current", ts))
			return nil
		}
		data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
		return txn.Save(key, string(data))
	})
}
