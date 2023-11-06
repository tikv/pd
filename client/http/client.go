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

package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const defaultTimeout = 30 * time.Second

// Client is a PD (Placement Driver) HTTP client.
type Client interface {
	GetRegionByID(context.Context, uint64) (*RegionInfo, error)
	GetRegionByKey(context.Context, []byte) (*RegionInfo, error)
	GetRegions(context.Context) (*RegionsInfo, error)
	GetRegionsByKey(context.Context, []byte, []byte, int) (*RegionsInfo, error)
	GetRegionsByStoreID(context.Context, uint64) (*RegionsInfo, error)
	GetHotReadRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHotWriteRegions(context.Context) (*StoreHotPeersInfos, error)
	GetStores(context.Context) (*StoresInfo, error)
	GetMinResolvedTSByStoresIDs(context.Context, []string) (uint64, map[uint64]uint64, error)
	Close()
}

var _ Client = (*client)(nil)

type client struct {
	pdAddrs []string
	tlsConf *tls.Config
	cli     *http.Client
}

// ClientOption configures the HTTP client.
type ClientOption func(c *client)

// WithHTTPClient configures the client with the given initialized HTTP client.
func WithHTTPClient(cli *http.Client) ClientOption {
	return func(c *client) {
		c.cli = cli
	}
}

// WithTLSConfig configures the client with the given TLS config.
// This option won't work if the client is configured with WithHTTPClient.
func WithTLSConfig(tlsConf *tls.Config) ClientOption {
	return func(c *client) {
		c.tlsConf = tlsConf
	}
}

// NewClient creates a PD HTTP client with the given PD addresses and TLS config.
func NewClient(
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	c := &client{}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	// Normalize the addresses with correct scheme prefix.
	for i, addr := range pdAddrs {
		if !strings.HasPrefix(addr, "http") {
			if c.tlsConf != nil {
				addr = "https://" + addr
			} else {
				addr = "http://" + addr
			}
			pdAddrs[i] = addr
		}
	}
	c.pdAddrs = pdAddrs
	// Init the HTTP client if it's not configured.
	if c.cli == nil {
		c.cli = &http.Client{Timeout: defaultTimeout}
		if c.tlsConf != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = c.tlsConf
			c.cli.Transport = transport
		}
	}

	return c
}

// Close closes the HTTP client.
func (c *client) Close() {
	if c.cli != nil {
		c.cli.CloseIdleConnections()
	}
	log.Info("[pd] http client closed")
}

func (c *client) pdAddr() string {
	// TODO: support the customized PD address selection strategy.
	return c.pdAddrs[0]
}

func (c *client) request(
	ctx context.Context,
	name, uri string,
	res interface{},
) error {
	reqURL := fmt.Sprintf("%s%s", c.pdAddr(), uri)
	logFields := []zap.Field{
		zap.String("name", name),
		zap.String("url", reqURL),
	}
	log.Debug("[pd] request the http url", logFields...)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		log.Error("[pd] create http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	// TODO: integrate the metrics.
	resp, err := c.cli.Do(req)
	if err != nil {
		log.Error("[pd] do http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Warn("[pd] close http response body failed", append(logFields, zap.Error(err))...)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		logFields = append(logFields, zap.String("status", resp.Status))

		bs, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			logFields = append(logFields, zap.NamedError("read-body-error", err))
		} else {
			logFields = append(logFields, zap.ByteString("body", bs))
		}

		log.Error("[pd] request failed with a non-200 status", logFields...)
		return errors.Errorf("request pd http api failed with status: '%s'", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetRegionByID gets the region info by ID.
func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx, "GetRegionByID", RegionByID(regionID), &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegionByKey gets the region info by key.
func (c *client) GetRegionByKey(ctx context.Context, key []byte) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx, "GetRegionByKey", RegionByKey(key), &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegions gets the regions info.
func (c *client) GetRegions(ctx context.Context) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, "GetRegions", Regions, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByKey gets the regions info by key range. If the limit is -1, it will return all regions within the range.
func (c *client) GetRegionsByKey(ctx context.Context, startKey, endKey []byte, limit int) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, "GetRegionsByKey", RegionsByKey(startKey, endKey, limit), &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByStoreID gets the regions info by store ID.
func (c *client) GetRegionsByStoreID(ctx context.Context, storeID uint64) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, "GetRegionsByStoreID", RegionsByStoreID(storeID), &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetHotReadRegions gets the hot read region statistics info.
func (c *client) GetHotReadRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotReadRegions StoreHotPeersInfos
	err := c.request(ctx, "GetHotReadRegions", HotRead, &hotReadRegions)
	if err != nil {
		return nil, err
	}
	return &hotReadRegions, nil
}

// GetHotWriteRegions gets the hot write region statistics info.
func (c *client) GetHotWriteRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotWriteRegions StoreHotPeersInfos
	err := c.request(ctx, "GetHotWriteRegions", HotWrite, &hotWriteRegions)
	if err != nil {
		return nil, err
	}
	return &hotWriteRegions, nil
}

// GetStores gets the stores info.
func (c *client) GetStores(ctx context.Context) (*StoresInfo, error) {
	var stores StoresInfo
	err := c.request(ctx, "GetStores", Stores, &stores)
	if err != nil {
		return nil, err
	}
	return &stores, nil
}

// GetMinResolvedTSByStoresIDs get min-resolved-ts by stores IDs.
func (c *client) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []string) (uint64, map[uint64]uint64, error) {
	uri := MinResolvedTSPrefix
	// scope is an optional parameter, it can be `cluster` or specified store IDs.
	// - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
	// - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
	// - When scope given a list of stores, min_resolved_ts will be provided for each store
	//      and the scope-specific min_resolved_ts will be returned.
	if len(storeIDs) != 0 {
		uri = fmt.Sprintf("%s?scope=%s", uri, strings.Join(storeIDs, ","))
	}
	resp := struct {
		MinResolvedTS       uint64            `json:"min_resolved_ts"`
		IsRealTime          bool              `json:"is_real_time,omitempty"`
		StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
	}{}
	err := c.request(ctx, "GetMinResolvedTSByStoresIDs", uri, &resp)
	if err != nil {
		return 0, nil, err
	}
	if !resp.IsRealTime {
		return 0, nil, errors.Trace(errors.New("min resolved ts is not enabled"))
	}
	return resp.MinResolvedTS, resp.StoresMinResolvedTS, nil
}
