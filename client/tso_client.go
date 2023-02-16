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

package pd

import "context"

// TSOClient manages resource group info and token request.
type TSOClient interface {
	// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
	GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error)
	// GetTSWithinKeyspaceAsync gets a timestamp within the given keyspace from the TSO service,
	// without block the caller.
	GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) TSFuture
	// GetLocalTSWithinKeyspace gets a local timestamp within the given keyspace from the TSO service
	GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (int64, int64, error)
	// GetLocalTSWithinKeyspaceAsync gets a local timestamp within the given keyspace from the TSO service,
	// without block the caller.
	GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) TSFuture
}

// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
// TODO: Refactor and share the TSO streaming framework in the PD client. The implementation
// here is in a basic manner and only for testing and integration purpose -- no batching,
// no async, no pooling, no forwarding, no retry and no deliberate error handling.
func (c *client) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

// GetLocalTSWithinKeyspace gets a local timestamp within the given keyspace from the TSO service
// nolint
func (c *client) GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (physical int64, logical int64, err error) {
	resp := c.GetLocalTSWithinKeyspaceAsync(ctx, dcLocation, keyspaceID)
	return resp.Wait()
}

// GetTSWithinKeyspaceAsync gets a timestamp within the given keyspace from the TSO service,
// without block the caller.
func (c *client) GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) TSFuture {
	return c.GetLocalTSWithinKeyspaceAsync(ctx, globalDCLocation, keyspaceID)
}

// GetLocalTSWithinKeyspaceAsync gets a local timestamp within the given keyspace from the TSO service,
// without block the caller.
// TODO: implement the following API
func (c *client) GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) TSFuture {
	return nil
}
