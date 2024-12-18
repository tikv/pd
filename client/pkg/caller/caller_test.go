// Copyright 2024 TiKV Project Authors.
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

package caller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetComponent(t *testing.T) {
	re := require.New(t)

	re.Equal(Component("github.com/tikv/pd/client/pkg/caller"), GetComponent(0))
	re.Equal(Component("testing"), GetComponent(1))
	re.Equal(Component("runtime"), GetComponent(2))
	re.Equal(Component("unknown"), GetComponent(3))
}

func TestGetCallerID(t *testing.T) {
	re := require.New(t)

	re.Equal(ID("caller.test"), GetCallerID())
}
