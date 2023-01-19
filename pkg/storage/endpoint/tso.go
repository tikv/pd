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
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TSOStorage defines the storage operations on the TSO.
// global tso: key is `/microservice/tso/{keyspaceGroupName}/timestamp`
// local tso: key is `/microservice/tso/{keyspaceGroupName}/lts/{dcLocation}/timestamp`
// FIXME: When we upgrade from the old version, there may be compatibility issues.
type TSOStorage interface {
	LoadTimestamp(keyspaceGroupName string, dcLocationKey ...string) (time.Time, error)
	SaveTimestamp(keyspaceGroupName string, ts time.Time, dcLocationKey ...string) error
}

var _ TSOStorage = (*StorageEndpoint)(nil)

// LoadTimestamp loads a timestamp from the storage according to the keyspaceGroupName and dcLocation.
func (se *StorageEndpoint) LoadTimestamp(keyspaceGroupName string, dcLocationKey ...string) (time.Time, error) {
	var prefix string
	if len(dcLocationKey) != 0 {
		prefix = timestampPrefix(keyspaceGroupName) + dcLocationKey[0] + "/"
	} else {
		prefix = timestampPrefix(keyspaceGroupName)
	}
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return time.Time{}, err
	}
	if len(keys) == 0 {
		return time.Time{}, nil
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
func (se *StorageEndpoint) SaveTimestamp(keyspaceGroupName string, ts time.Time, dcLocationKey ...string) error {
	key := timestampPath(keyspaceGroupName, dcLocationKey...)
	data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
	return se.Save(key, string(data))
}
