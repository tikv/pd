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

package tso

import (
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
)

const (
	serverCount                 = 3
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 300
	tsoCount                    = 10
)

type tsoResponse interface {
	GetCount() uint32
	GetTimestamp() *pdpb.Timestamp
}

func checkAndReturnTimestampResponse[T tsoResponse](re *require.Assertions, resp T) *pdpb.Timestamp {
	re.Equal(uint32(tsoCount), resp.GetCount())
	timestamp := resp.GetTimestamp()
	re.Positive(timestamp.GetPhysical())
	re.GreaterOrEqual(uint32(timestamp.GetLogical()), uint32(tsoCount))
	return timestamp
}
