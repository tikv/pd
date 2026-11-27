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

package testutil

import (
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

const testKeyspaceID = 1

// TxnKeyspaceKey encodes a test key in a transaction keyspace in NextGen.
func TxnKeyspaceKey(key []byte) []byte {
	if !kerneltype.IsNextGen() {
		return key
	}
	return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, testKeyspaceID), key...))
}

// TxnKeyspaceRange encodes a test range in NextGen, using the keyspace boundaries for empty endpoints.
func TxnKeyspaceRange(start, end []byte) keyutil.KeyRange {
	if kerneltype.IsNextGen() && len(end) == 0 {
		upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, testKeyspaceID+1)
		return keyutil.KeyRange{StartKey: TxnKeyspaceKey(start), EndKey: upper[:]}
	}
	return keyutil.KeyRange{StartKey: TxnKeyspaceKey(start), EndKey: TxnKeyspaceKey(end)}
}
