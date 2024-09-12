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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
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

func TestUpdateURLs(t *testing.T) {
	re := require.New(t)
	members := []*pdpb.Member{
		{Name: "pd4", ClientUrls: []string{"tmp://pd4"}},
		{Name: "pd1", ClientUrls: []string{"tmp://pd1"}},
		{Name: "pd3", ClientUrls: []string{"tmp://pd3"}},
		{Name: "pd2", ClientUrls: []string{"tmp://pd2"}},
	}
	getURLs := func(ms []*pdpb.Member) (urls []string) {
		for _, m := range ms {
			urls = append(urls, m.GetClientUrls()[0])
		}
		return
	}
	cli := &pdServiceDiscovery{option: newOption()}
	cli.urls.Store([]string{})
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members)
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2], members[0]}), cli.GetServiceURLs())
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[2:])
	re.Equal(getURLs([]*pdpb.Member{members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[3:])
	re.Equal(getURLs([]*pdpb.Member{members[3]}), cli.GetServiceURLs())
}

const testClientURL = "tmp://test.url:5255"

func TestClientCtx(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	_, err := NewClientWithContext(ctx, []string{testClientURL}, SecurityOption{})
	re.Error(err)
	re.Less(time.Since(start), time.Second*5)
}

func TestClientWithRetry(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	_, err := NewClientWithContext(context.TODO(), []string{testClientURL}, SecurityOption{}, WithMaxErrorRetry(5))
	re.Error(err)
	re.Less(time.Since(start), time.Second*10)
}

func TestGRPCDialOption(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
	defer cancel()
	cli := &pdServiceDiscovery{
		checkMembershipCh: make(chan struct{}, 1),
		ctx:               ctx,
		cancel:            cancel,
		tlsCfg:            nil,
		option:            newOption(),
	}
	cli.urls.Store([]string{testClientURL})
	cli.option.gRPCDialOptions = []grpc.DialOption{grpc.WithBlock()}
	err := cli.updateMember()
	re.Error(err)
	re.Greater(time.Since(start), 500*time.Millisecond)
}

func TestTsoRequestWait(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	req := &tsoRequest{
		done:       make(chan error, 1),
		physical:   0,
		logical:    0,
		requestCtx: context.TODO(),
		clientCtx:  ctx,
	}
	cancel()
	_, _, err := req.Wait()
	re.ErrorIs(errors.Cause(err), context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	req = &tsoRequest{
		done:       make(chan error, 1),
		physical:   0,
		logical:    0,
		requestCtx: ctx,
		clientCtx:  context.TODO(),
	}
	cancel()
	_, _, err = req.Wait()
	re.ErrorIs(errors.Cause(err), context.Canceled)
}
