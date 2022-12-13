// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package client

import (
	"context"
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestClientBasic(t *testing.T) {
	re := require.New(t)
	cli := newClient("127.0.0.1:33790")
	resp, err := cli.GetResourceGroup(context.Background(), &rmpb.GetResourceGroupRequest{})
	re.ErrorContains(err, "connection refused")
	re.Nil(resp)
}
