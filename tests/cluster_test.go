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

package tests

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestClassifyStartServersError(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.Equal(startServersRetryCurrent, classifyStartServersError(errors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.Equal(startServersRetryCurrent, classifyStartServersError(errors.New("[PD:server:ErrStartEtcd]start etcd failed")))
	re.Equal(startServersRetryRecreate, classifyStartServersError(errors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyStartServersError(errors.New("Etcd cluster ID mismatch")))
	re.Equal(startServersNoRetry, classifyStartServersError(errors.New("some other error")))
	re.Equal(startServersNoRetry, classifyStartServersError(nil))
}
