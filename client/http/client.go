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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	httpScheme         = "http"
	httpsScheme        = "https"
	networkErrorStatus = "network error"

	defaultTimeout = 30 * time.Second
)

// Client is a PD (Placement Driver) HTTP client.
type Client interface {
	/* Meta-related interfaces */
	GetRegionByID(context.Context, uint64) (*RegionInfo, error)
	GetRegionByKey(context.Context, []byte) (*RegionInfo, error)
	GetRegions(context.Context) (*RegionsInfo, error)
	GetRegionsByKeyRange(context.Context, []byte, []byte, int) (*RegionsInfo, error)
	GetRegionsByStoreID(context.Context, uint64) (*RegionsInfo, error)
	GetHotReadRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHotWriteRegions(context.Context) (*StoreHotPeersInfos, error)
	GetRegionStatusByKeyRange(context.Context, []byte, []byte) (*RegionStats, error)
	GetStores(context.Context) (*StoresInfo, error)
	/* Rule-related interfaces */
	GetAllPlacementRuleBundles(context.Context) ([]*GroupBundle, error)
	GetPlacementRuleBundleByGroup(context.Context, string) (*GroupBundle, error)
	GetPlacementRulesByGroup(context.Context, string) ([]*Rule, error)
	SetPlacementRule(context.Context, *Rule) error
	SetPlacementRuleBundles(context.Context, []*GroupBundle, bool) error
	DeletePlacementRule(context.Context, string, string) error
	GetAllRegionLabelRules(context.Context) ([]*LabelRule, error)
	GetRegionLabelRulesByIDs(context.Context, []string) ([]*LabelRule, error)
	SetRegionLabelRule(context.Context, *LabelRule) error
	PatchRegionLabelRules(context.Context, *LabelRulePatch) error
	/* Scheduling-related interfaces */
	AccelerateSchedule(context.Context, []byte, []byte) error
	/* Other interfaces */
	GetMinResolvedTSByStoresIDs(context.Context, []uint64) (uint64, map[uint64]uint64, error)

	/* Client-related methods */
	// WithRespHandler sets and returns a new client with the given HTTP response handler.
	// This allows the caller to customize how the response is handled, including error handling logic.
	// Additionally, it is important for the caller to handle the content of the response body properly
	// in order to ensure that it can be read and marshaled correctly into `res`.
	WithRespHandler(func(resp *http.Response, res interface{}) error) Client
	Close()
}

var _ Client = (*client)(nil)

type client struct {
	pdAddrs []string
	tlsConf *tls.Config
	cli     *http.Client

	respHandler func(resp *http.Response, res interface{}) error

	requestCounter    *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
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

// WithMetrics configures the client with metrics.
func WithMetrics(
	requestCounter *prometheus.CounterVec,
	executionDuration *prometheus.HistogramVec,
) ClientOption {
	return func(c *client) {
		c.requestCounter = requestCounter
		c.executionDuration = executionDuration
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
		if !strings.HasPrefix(addr, httpScheme) {
			var scheme string
			if c.tlsConf != nil {
				scheme = httpsScheme
			} else {
				scheme = httpScheme
			}
			pdAddrs[i] = fmt.Sprintf("%s://%s", scheme, addr)
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

// WithRespHandler sets and returns a new client with the given HTTP response handler.
func (c *client) WithRespHandler(
	handler func(resp *http.Response, res interface{}) error,
) Client {
	newClient := *c
	newClient.respHandler = handler
	return &newClient
}

func (c *client) reqCounter(name, status string) {
	if c.requestCounter == nil {
		return
	}
	c.requestCounter.WithLabelValues(name, status).Inc()
}

func (c *client) execDuration(name string, duration time.Duration) {
	if c.executionDuration == nil {
		return
	}
	c.executionDuration.WithLabelValues(name).Observe(duration.Seconds())
}

// At present, we will use the retry strategy of polling by default to keep
// it consistent with the current implementation of some clients (e.g. TiDB).
func (c *client) requestWithRetry(
	ctx context.Context,
	name, uri, method string,
	body io.Reader, res interface{},
) error {
	var (
		err  error
		addr string
	)
	for idx := 0; idx < len(c.pdAddrs); idx++ {
		addr = c.pdAddrs[idx]
		err = c.request(ctx, name, fmt.Sprintf("%s%s", addr, uri), method, body, res)
		if err == nil {
			break
		}
		log.Debug("[pd] request one addr failed",
			zap.Int("idx", idx), zap.String("addr", addr), zap.Error(err))
	}
	return err
}

func (c *client) request(
	ctx context.Context,
	name, url, method string,
	body io.Reader, res interface{},
) error {
	logFields := []zap.Field{
		zap.String("name", name),
		zap.String("url", url),
	}
	log.Debug("[pd] request the http url", logFields...)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		log.Error("[pd] create http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	start := time.Now()
	resp, err := c.cli.Do(req)
	if err != nil {
		c.reqCounter(name, networkErrorStatus)
		log.Error("[pd] do http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	c.execDuration(name, time.Since(start))
	c.reqCounter(name, resp.Status)

	// Give away the response handling to the caller if the handler is set.
	if c.respHandler != nil {
		return c.respHandler(resp, res)
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

	if res == nil {
		return nil
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
	err := c.requestWithRetry(ctx,
		"GetRegionByID", RegionByID(regionID),
		http.MethodGet, http.NoBody, &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegionByKey gets the region info by key.
func (c *client) GetRegionByKey(ctx context.Context, key []byte) (*RegionInfo, error) {
	var region RegionInfo
	err := c.requestWithRetry(ctx,
		"GetRegionByKey", RegionByKey(key),
		http.MethodGet, http.NoBody, &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegions gets the regions info.
func (c *client) GetRegions(ctx context.Context) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.requestWithRetry(ctx,
		"GetRegions", Regions,
		http.MethodGet, http.NoBody, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByKeyRange gets the regions info by key range. If the limit is -1, it will return all regions within the range.
func (c *client) GetRegionsByKeyRange(ctx context.Context, startKey, endKey []byte, limit int) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.requestWithRetry(ctx,
		"GetRegionsByKeyRange", RegionsByKey(startKey, endKey, limit),
		http.MethodGet, http.NoBody, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByStoreID gets the regions info by store ID.
func (c *client) GetRegionsByStoreID(ctx context.Context, storeID uint64) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.requestWithRetry(ctx,
		"GetRegionsByStoreID", RegionsByStoreID(storeID),
		http.MethodGet, http.NoBody, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetHotReadRegions gets the hot read region statistics info.
func (c *client) GetHotReadRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotReadRegions StoreHotPeersInfos
	err := c.requestWithRetry(ctx,
		"GetHotReadRegions", HotRead,
		http.MethodGet, http.NoBody, &hotReadRegions)
	if err != nil {
		return nil, err
	}
	return &hotReadRegions, nil
}

// GetHotWriteRegions gets the hot write region statistics info.
func (c *client) GetHotWriteRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotWriteRegions StoreHotPeersInfos
	err := c.requestWithRetry(ctx,
		"GetHotWriteRegions", HotWrite,
		http.MethodGet, http.NoBody, &hotWriteRegions)
	if err != nil {
		return nil, err
	}
	return &hotWriteRegions, nil
}

// GetRegionStatusByKeyRange gets the region status by key range.
func (c *client) GetRegionStatusByKeyRange(ctx context.Context, startKey, endKey []byte) (*RegionStats, error) {
	var regionStats RegionStats
	err := c.requestWithRetry(ctx,
		"GetRegionStatusByKeyRange", RegionStatsByKeyRange(startKey, endKey),
		http.MethodGet, http.NoBody, &regionStats,
	)
	if err != nil {
		return nil, err
	}
	return &regionStats, nil
}

// GetStores gets the stores info.
func (c *client) GetStores(ctx context.Context) (*StoresInfo, error) {
	var stores StoresInfo
	err := c.requestWithRetry(ctx,
		"GetStores", Stores,
		http.MethodGet, http.NoBody, &stores)
	if err != nil {
		return nil, err
	}
	return &stores, nil
}

// GetAllPlacementRuleBundles gets all placement rules bundles.
func (c *client) GetAllPlacementRuleBundles(ctx context.Context) ([]*GroupBundle, error) {
	var bundles []*GroupBundle
	err := c.requestWithRetry(ctx,
		"GetPlacementRuleBundle", PlacementRuleBundle,
		http.MethodGet, http.NoBody, &bundles)
	if err != nil {
		return nil, err
	}
	return bundles, nil
}

// GetPlacementRuleBundleByGroup gets the placement rules bundle by group.
func (c *client) GetPlacementRuleBundleByGroup(ctx context.Context, group string) (*GroupBundle, error) {
	var bundle GroupBundle
	err := c.requestWithRetry(ctx,
		"GetPlacementRuleBundleByGroup", PlacementRuleBundleByGroup(group),
		http.MethodGet, http.NoBody, &bundle)
	if err != nil {
		return nil, err
	}
	return &bundle, nil
}

// GetPlacementRulesByGroup gets the placement rules by group.
func (c *client) GetPlacementRulesByGroup(ctx context.Context, group string) ([]*Rule, error) {
	var rules []*Rule
	err := c.requestWithRetry(ctx,
		"GetPlacementRulesByGroup", PlacementRulesByGroup(group),
		http.MethodGet, http.NoBody, &rules)
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// SetPlacementRule sets the placement rule.
func (c *client) SetPlacementRule(ctx context.Context, rule *Rule) error {
	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetPlacementRule", PlacementRule,
		http.MethodPost, bytes.NewBuffer(ruleJSON), nil)
}

// SetPlacementRuleBundles sets the placement rule bundles.
// If `partial` is false, all old configurations will be over-written and dropped.
func (c *client) SetPlacementRuleBundles(ctx context.Context, bundles []*GroupBundle, partial bool) error {
	bundlesJSON, err := json.Marshal(bundles)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetPlacementRuleBundles", PlacementRuleBundleWithPartialParameter(partial),
		http.MethodPost, bytes.NewBuffer(bundlesJSON), nil)
}

// DeletePlacementRule deletes the placement rule.
func (c *client) DeletePlacementRule(ctx context.Context, group, id string) error {
	return c.requestWithRetry(ctx,
		"DeletePlacementRule", PlacementRuleByGroupAndID(group, id),
		http.MethodDelete, http.NoBody, nil)
}

// GetAllRegionLabelRules gets all region label rules.
func (c *client) GetAllRegionLabelRules(ctx context.Context) ([]*LabelRule, error) {
	var labelRules []*LabelRule
	err := c.requestWithRetry(ctx,
		"GetAllRegionLabelRules", RegionLabelRules,
		http.MethodGet, http.NoBody, &labelRules)
	if err != nil {
		return nil, err
	}
	return labelRules, nil
}

// GetRegionLabelRulesByIDs gets the region label rules by IDs.
func (c *client) GetRegionLabelRulesByIDs(ctx context.Context, ruleIDs []string) ([]*LabelRule, error) {
	idsJSON, err := json.Marshal(ruleIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var labelRules []*LabelRule
	err = c.requestWithRetry(ctx,
		"GetRegionLabelRulesByIDs", RegionLabelRulesByIDs,
		http.MethodGet, bytes.NewBuffer(idsJSON), &labelRules)
	if err != nil {
		return nil, err
	}
	return labelRules, nil
}

// SetRegionLabelRule sets the region label rule.
func (c *client) SetRegionLabelRule(ctx context.Context, labelRule *LabelRule) error {
	labelRuleJSON, err := json.Marshal(labelRule)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetRegionLabelRule", RegionLabelRule,
		http.MethodPost, bytes.NewBuffer(labelRuleJSON), nil)
}

// PatchRegionLabelRules patches the region label rules.
func (c *client) PatchRegionLabelRules(ctx context.Context, labelRulePatch *LabelRulePatch) error {
	labelRulePatchJSON, err := json.Marshal(labelRulePatch)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"PatchRegionLabelRules", RegionLabelRules,
		http.MethodPatch, bytes.NewBuffer(labelRulePatchJSON), nil)
}

// AccelerateSchedule accelerates the scheduling of the regions within the given key range.
func (c *client) AccelerateSchedule(ctx context.Context, startKey, endKey []byte) error {
	input := map[string]string{
		"start_key": url.QueryEscape(string(startKey)),
		"end_key":   url.QueryEscape(string(endKey)),
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"AccelerateSchedule", AccelerateSchedule,
		http.MethodPost, bytes.NewBuffer(inputJSON), nil)
}

// GetMinResolvedTSByStoresIDs get min-resolved-ts by stores IDs.
func (c *client) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []uint64) (uint64, map[uint64]uint64, error) {
	uri := MinResolvedTSPrefix
	// scope is an optional parameter, it can be `cluster` or specified store IDs.
	// - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
	// - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
	// - When scope given a list of stores, min_resolved_ts will be provided for each store
	//      and the scope-specific min_resolved_ts will be returned.
	if len(storeIDs) != 0 {
		storeIDStrs := make([]string, len(storeIDs))
		for idx, id := range storeIDs {
			storeIDStrs[idx] = fmt.Sprintf("%d", id)
		}
		uri = fmt.Sprintf("%s?scope=%s", uri, strings.Join(storeIDStrs, ","))
	}
	resp := struct {
		MinResolvedTS       uint64            `json:"min_resolved_ts"`
		IsRealTime          bool              `json:"is_real_time,omitempty"`
		StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
	}{}
	err := c.requestWithRetry(ctx,
		"GetMinResolvedTSByStoresIDs", uri,
		http.MethodGet, http.NoBody, &resp)
	if err != nil {
		return 0, nil, err
	}
	if !resp.IsRealTime {
		return 0, nil, errors.Trace(errors.New("min resolved ts is not enabled"))
	}
	return resp.MinResolvedTS, resp.StoresMinResolvedTS, nil
}
