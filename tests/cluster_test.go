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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	perrors "github.com/pingcap/errors"
)

func TestShouldRetryCurrentServers(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.True(shouldRetryCurrentServers(perrors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.True(shouldRetryCurrentServers(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.True(shouldRetryCurrentServers(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(perrors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(perrors.New("Etcd cluster ID mismatch")))
	re.False(shouldRetryCurrentServers(perrors.New("some other error")))
	re.False(shouldRetryCurrentServers(nil))
}

func TestClassifyInitialServersError(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.Equal(startServersRetryCurrent, classifyInitialServersError(perrors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.Equal(startServersRetryCurrent, classifyInitialServersError(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("Etcd cluster ID mismatch")))
	re.Equal(startServersNoRetry, classifyInitialServersError(perrors.New("some other error")))
	re.Equal(startServersNoRetry, classifyInitialServersError(nil))
}

func TestRunTasksFastErrorReturnsLaterFailureWithoutWaitingForEarlierTask(t *testing.T) {
	re := require.New(t)

	blocked := make(chan struct{})

	cleanupCalled := make(chan struct{}, 1)
	start := time.Now()
	err := runTasksFastError([]func() error{
		func() error {
			<-blocked
			return nil
		},
		func() error {
			return errors.New("address already in use")
		},
	}, func() {
		close(blocked)
		cleanupCalled <- struct{}{}
	})

	re.ErrorContains(err, "address already in use")
	re.Less(time.Since(start), 500*time.Millisecond)

	select {
	case <-cleanupCalled:
	default:
		t.Fatal("cleanup callback was not triggered")
	}
}
