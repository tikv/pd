// Copyright 2021 TiKV Project Authors.
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

package assertutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNilFail(t *testing.T) {
	re := require.New(t)
	var failErr error
	checker := NewChecker()
	checker.FailNow = func() {
		failErr = errors.New("called assert func not exist")
	}
	re.Nil(checker.IsNil)
	checker.AssertNil(nil)
	re.Error(failErr)
}
