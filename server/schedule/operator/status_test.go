// Copyright 2019 TiKV Project Authors.
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

package operator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsEndStatus(t *testing.T) {
	re := require.New(t)
	for st := OpStatus(0); st < firstEndStatus; st++ {
		re.False(IsEndStatus(st))
	}
	for st := firstEndStatus; st < statusCount; st++ {
		re.True(IsEndStatus(st))
	}
	for st := statusCount; st < statusCount+100; st++ {
		re.False(IsEndStatus(st))
	}
}
