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

package tikvclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"golang.org/x/net/context"
)

func TestGetConn(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	client := NewRPCClient()

	url1 := tempurl.Alloc()
	url2 := tempurl.Alloc()

	conn1, err := client.getClientConn(ctx, url1)
	re.NoError(err)
	conn2, err := client.getClientConn(ctx, url2)
	re.NoError(err)
	re.False(conn1.Get() == conn2.Get())

	conn3, err := client.getClientConn(ctx, url1)
	re.NoError(err)
	conn4, err := client.getClientConn(ctx, url2)
	re.NoError(err)
	re.False(conn3.Get() == conn4.Get())

	re.True(conn1.Get() == conn3.Get())
	re.True(conn2.Get() == conn4.Get())
}

func TestGetConnAfterClose(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	client := NewRPCClient()

	url := tempurl.Alloc()

	conn1, err := client.getClientConn(ctx, url)
	re.NoError(err)
	err = conn1.Get().Close()
	re.NoError(err)

	conn2, err := client.getClientConn(ctx, url)
	re.NoError(err)
	re.False(conn1.Get() == conn2.Get())

	conn3, err := client.getClientConn(ctx, url)
	re.NoError(err)
	re.True(conn2.Get() == conn3.Get())
}
