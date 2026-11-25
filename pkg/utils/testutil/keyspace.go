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

// TxnKeyspaceRange encodes a test range in NextGen, using the keyspace boundaries for empty endpoints.
func TxnKeyspaceRange(start, end []byte) keyutil.KeyRange {
	if !kerneltype.IsNextGen() {
		return keyutil.KeyRange{StartKey: start, EndKey: end}
	}
	prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, testKeyspaceID)
	start = codec.EncodeBytes(append(prefix, start...))
	if len(end) == 0 {
		upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, testKeyspaceID+1)
		end = upper[:]
	} else {
		end = codec.EncodeBytes(append(prefix, end...))
	}
	return keyutil.KeyRange{StartKey: start, EndKey: end}
}
