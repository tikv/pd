// Copyright 2016 TiKV Project Authors.
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

package pd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	"github.com/tikv/pd/client/pkg/utils/tsoutil"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestTSLessEqual(t *testing.T) {
	re := require.New(t)
	re.True(tsoutil.TSLessEqual(9, 9, 9, 9))
	re.True(tsoutil.TSLessEqual(8, 9, 9, 8))
	re.False(tsoutil.TSLessEqual(9, 8, 8, 9))
	re.False(tsoutil.TSLessEqual(9, 8, 9, 6))
	re.True(tsoutil.TSLessEqual(9, 6, 9, 8))
}

const testClientURL = "tmp://test.url:5255"

func TestClientCtx(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	_, err := NewClientWithContext(ctx, caller.TestComponent,
		[]string{testClientURL}, SecurityOption{})
	re.Error(err)
	re.Less(time.Since(start), time.Second*5)
}

func TestClientWithRetry(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	_, err := NewClientWithContext(context.TODO(), caller.TestComponent,
		[]string{testClientURL}, SecurityOption{}, opt.WithMaxErrorRetry(5))
	re.Error(err)
	re.Less(time.Since(start), time.Second*10)
}
