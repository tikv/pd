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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultCallerID    = "pd-http-client"
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
	GetRegionsByKeyRange(context.Context, *KeyRange, int) (*RegionsInfo, error)
	GetRegionsByStoreID(context.Context, uint64) (*RegionsInfo, error)
	GetRegionsReplicatedStateByKeyRange(context.Context, *KeyRange) (string, error)
	GetHotReadRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHotWriteRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHistoryHotRegions(context.Context, *HistoryHotRegionsRequest) (*HistoryHotRegions, error)
	GetRegionStatusByKeyRange(context.Context, *KeyRange, bool) (*RegionStats, error)
	GetStores(context.Context) (*StoresInfo, error)
	GetStore(context.Context, uint64) (*StoreInfo, error)
	SetStoreLabels(context.Context, int64, map[string]string) error
	GetMembers(context.Context) (*MembersInfo, error)
	GetLeader(context.Context) (*pdpb.Member, error)
	TransferLeader(context.Context, string) error
	/* Config-related interfaces */
	GetScheduleConfig(context.Context) (map[string]interface{}, error)
	SetScheduleConfig(context.Context, map[string]interface{}) error
	GetClusterVersion(context.Context) (string, error)
	/* Scheduler-related interfaces */
	GetSchedulers(context.Context) ([]string, error)
	CreateScheduler(ctx context.Context, name string, storeID uint64) error
	SetSchedulerDelay(context.Context, string, int64) error
	/* Rule-related interfaces */
	GetAllPlacementRuleBundles(context.Context) ([]*GroupBundle, error)
	GetPlacementRuleBundleByGroup(context.Context, string) (*GroupBundle, error)
	GetPlacementRulesByGroup(context.Context, string) ([]*Rule, error)
	SetPlacementRule(context.Context, *Rule) error
	SetPlacementRuleInBatch(context.Context, []*RuleOp) error
	SetPlacementRuleBundles(context.Context, []*GroupBundle, bool) error
	DeletePlacementRule(context.Context, string, string) error
	GetAllPlacementRuleGroups(context.Context) ([]*RuleGroup, error)
	GetPlacementRuleGroupByID(context.Context, string) (*RuleGroup, error)
	SetPlacementRuleGroup(context.Context, *RuleGroup) error
	DeletePlacementRuleGroupByID(context.Context, string) error
	GetAllRegionLabelRules(context.Context) ([]*LabelRule, error)
	GetRegionLabelRulesByIDs(context.Context, []string) ([]*LabelRule, error)
	SetRegionLabelRule(context.Context, *LabelRule) error
	PatchRegionLabelRules(context.Context, *LabelRulePatch) error
	/* Scheduling-related interfaces */
	AccelerateSchedule(context.Context, *KeyRange) error
	AccelerateScheduleInBatch(context.Context, []*KeyRange) error
	/* Other interfaces */
	GetMinResolvedTSByStoresIDs(context.Context, []uint64) (uint64, map[uint64]uint64, error)
	/* Micro Service interfaces */
	GetMicroServiceMembers(context.Context, string) ([]string, error)

	/* Client-related methods */
	// WithCallerID sets and returns a new client with the given caller ID.
	WithCallerID(string) Client
	// WithRespHandler sets and returns a new client with the given HTTP response handler.
	// This allows the caller to customize how the response is handled, including error handling logic.
	// Additionally, it is important for the caller to handle the content of the response body properly
	// in order to ensure that it can be read and marshaled correctly into `res`.
	WithRespHandler(func(resp *http.Response, res interface{}) error) Client
	Close()
}

var _ Client = (*client)(nil)

// clientInner is the inner implementation of the PD HTTP client, which will
// implement some internal logics, such as HTTP client, service discovery, etc.
type clientInner struct {
	pdAddrs []string
	tlsConf *tls.Config
	cli     *http.Client
}

type client struct {
	// Wrap this struct is to make sure the inner implementation
	// won't be exposed and cloud be consistent during the copy.
	inner *clientInner

	callerID    string
	respHandler func(resp *http.Response, res interface{}) error

	requestCounter    *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
}

// ClientOption configures the HTTP client.
type ClientOption func(c *client)

// WithHTTPClient configures the client with the given initialized HTTP client.
func WithHTTPClient(cli *http.Client) ClientOption {
	return func(c *client) {
		c.inner.cli = cli
	}
}

// WithTLSConfig configures the client with the given TLS config.
// This option won't work if the client is configured with WithHTTPClient.
func WithTLSConfig(tlsConf *tls.Config) ClientOption {
	return func(c *client) {
		c.inner.tlsConf = tlsConf
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
	c := &client{inner: &clientInner{}, callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	// Normalize the addresses with correct scheme prefix.
	for i, addr := range pdAddrs {
		if !strings.HasPrefix(addr, httpScheme) {
			var scheme string
			if c.inner.tlsConf != nil {
				scheme = httpsScheme
			} else {
				scheme = httpScheme
			}
			pdAddrs[i] = fmt.Sprintf("%s://%s", scheme, addr)
		}
	}
	c.inner.pdAddrs = pdAddrs
	// Init the HTTP client if it's not configured.
	if c.inner.cli == nil {
		c.inner.cli = &http.Client{Timeout: defaultTimeout}
		if c.inner.tlsConf != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = c.inner.tlsConf
			c.inner.cli.Transport = transport
		}
	}

	return c
}

// Close closes the HTTP client.
func (c *client) Close() {
	if c.inner == nil {
		return
	}
	if c.inner.cli != nil {
		c.inner.cli.CloseIdleConnections()
	}
	log.Info("[pd] http client closed")
}

// WithCallerID sets and returns a new client with the given caller ID.
func (c *client) WithCallerID(callerID string) Client {
	newClient := *c
	newClient.callerID = callerID
	return &newClient
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

// Header key definition constants.
const (
	pdAllowFollowerHandleKey = "PD-Allow-Follower-Handle"
	xCallerIDKey             = "X-Caller-ID"
)

// HeaderOption configures the HTTP header.
type HeaderOption func(header http.Header)

// WithAllowFollowerHandle sets the header field to allow a PD follower to handle this request.
func WithAllowFollowerHandle() HeaderOption {
	return func(header http.Header) {
		header.Set(pdAllowFollowerHandleKey, "true")
	}
}

// At present, we will use the retry strategy of polling by default to keep
// it consistent with the current implementation of some clients (e.g. TiDB).
func (c *client) requestWithRetry(
	ctx context.Context,
	name, uri, method string,
	body []byte, res interface{},
	headerOpts ...HeaderOption,
) error {
	var (
		err  error
		addr string
	)
	for idx := 0; idx < len(c.inner.pdAddrs); idx++ {
		addr = c.inner.pdAddrs[idx]
		err = c.request(ctx, name, fmt.Sprintf("%s%s", addr, uri), method, body, res, headerOpts...)
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
	body []byte, res interface{},
	headerOpts ...HeaderOption,
) error {
	logFields := []zap.Field{
		zap.String("name", name),
		zap.String("url", url),
		zap.String("method", method),
		zap.String("caller-id", c.callerID),
	}
	log.Debug("[pd] request the http url", logFields...)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		log.Error("[pd] create http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	for _, opt := range headerOpts {
		opt(req.Header)
	}
	req.Header.Set(xCallerIDKey, c.callerID)

	start := time.Now()
	resp, err := c.inner.cli.Do(req)
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
		http.MethodGet, nil, &region)
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
		http.MethodGet, nil, &region)
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
		http.MethodGet, nil, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByKeyRange gets the regions info by key range. If the limit is -1, it will return all regions within the range.
// The keys in the key range should be encoded in the UTF-8 bytes format.
func (c *client) GetRegionsByKeyRange(ctx context.Context, keyRange *KeyRange, limit int) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.requestWithRetry(ctx,
		"GetRegionsByKeyRange", RegionsByKeyRange(keyRange, limit),
		http.MethodGet, nil, &regions)
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
		http.MethodGet, nil, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsReplicatedStateByKeyRange gets the regions replicated state info by key range.
// The keys in the key range should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) GetRegionsReplicatedStateByKeyRange(ctx context.Context, keyRange *KeyRange) (string, error) {
	var state string
	err := c.requestWithRetry(ctx,
		"GetRegionsReplicatedStateByKeyRange", RegionsReplicatedByKeyRange(keyRange),
		http.MethodGet, nil, &state)
	if err != nil {
		return "", err
	}
	return state, nil
}

// GetHotReadRegions gets the hot read region statistics info.
func (c *client) GetHotReadRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotReadRegions StoreHotPeersInfos
	err := c.requestWithRetry(ctx,
		"GetHotReadRegions", HotRead,
		http.MethodGet, nil, &hotReadRegions)
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
		http.MethodGet, nil, &hotWriteRegions)
	if err != nil {
		return nil, err
	}
	return &hotWriteRegions, nil
}

// GetHistoryHotRegions gets the history hot region statistics info.
func (c *client) GetHistoryHotRegions(ctx context.Context, req *HistoryHotRegionsRequest) (*HistoryHotRegions, error) {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var historyHotRegions HistoryHotRegions
	err = c.requestWithRetry(ctx,
		"GetHistoryHotRegions", HotHistory,
		http.MethodGet, reqJSON, &historyHotRegions,
		WithAllowFollowerHandle())
	if err != nil {
		return nil, err
	}
	return &historyHotRegions, nil
}

// GetRegionStatusByKeyRange gets the region status by key range.
// If the `onlyCount` flag is true, the result will only include the count of regions.
// The keys in the key range should be encoded in the UTF-8 bytes format.
func (c *client) GetRegionStatusByKeyRange(ctx context.Context, keyRange *KeyRange, onlyCount bool) (*RegionStats, error) {
	var regionStats RegionStats
	err := c.requestWithRetry(ctx,
		"GetRegionStatusByKeyRange", RegionStatsByKeyRange(keyRange, onlyCount),
		http.MethodGet, nil, &regionStats,
	)
	if err != nil {
		return nil, err
	}
	return &regionStats, nil
}

// SetStoreLabels sets the labels of a store.
func (c *client) SetStoreLabels(ctx context.Context, storeID int64, storeLabels map[string]string) error {
	jsonInput, err := json.Marshal(storeLabels)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx, "SetStoreLabel", LabelByStoreID(storeID),
		http.MethodPost, jsonInput, nil)
}

func (c *client) GetMembers(ctx context.Context) (*MembersInfo, error) {
	var members MembersInfo
	err := c.requestWithRetry(ctx,
		"GetMembers", membersPrefix,
		http.MethodGet, nil, &members)
	if err != nil {
		return nil, err
	}
	return &members, nil
}

// GetLeader gets the leader of PD cluster.
func (c *client) GetLeader(ctx context.Context) (*pdpb.Member, error) {
	var leader pdpb.Member
	err := c.requestWithRetry(ctx, "GetLeader", leaderPrefix,
		http.MethodGet, nil, &leader)
	if err != nil {
		return nil, err
	}
	return &leader, nil
}

// TransferLeader transfers the PD leader.
func (c *client) TransferLeader(ctx context.Context, newLeader string) error {
	return c.requestWithRetry(ctx, "TransferLeader", TransferLeaderByID(newLeader),
		http.MethodPost, nil, nil)
}

// GetScheduleConfig gets the schedule configurations.
func (c *client) GetScheduleConfig(ctx context.Context) (map[string]interface{}, error) {
	var config map[string]interface{}
	err := c.requestWithRetry(ctx,
		"GetScheduleConfig", ScheduleConfig,
		http.MethodGet, nil, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// SetScheduleConfig sets the schedule configurations.
func (c *client) SetScheduleConfig(ctx context.Context, config map[string]interface{}) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetScheduleConfig", ScheduleConfig,
		http.MethodPost, configJSON, nil)
}

// GetStores gets the stores info.
func (c *client) GetStores(ctx context.Context) (*StoresInfo, error) {
	var stores StoresInfo
	err := c.requestWithRetry(ctx,
		"GetStores", Stores,
		http.MethodGet, nil, &stores)
	if err != nil {
		return nil, err
	}
	return &stores, nil
}

// GetStore gets the store info by ID.
func (c *client) GetStore(ctx context.Context, storeID uint64) (*StoreInfo, error) {
	var store StoreInfo
	err := c.requestWithRetry(ctx,
		"GetStore", StoreByID(storeID),
		http.MethodGet, nil, &store)
	if err != nil {
		return nil, err
	}
	return &store, nil
}

// GetClusterVersion gets the cluster version.
func (c *client) GetClusterVersion(ctx context.Context) (string, error) {
	var version string
	err := c.requestWithRetry(ctx,
		"GetClusterVersion", ClusterVersion,
		http.MethodGet, nil, &version)
	if err != nil {
		return "", err
	}
	return version, nil
}

// GetAllPlacementRuleBundles gets all placement rules bundles.
func (c *client) GetAllPlacementRuleBundles(ctx context.Context) ([]*GroupBundle, error) {
	var bundles []*GroupBundle
	err := c.requestWithRetry(ctx,
		"GetPlacementRuleBundle", PlacementRuleBundle,
		http.MethodGet, nil, &bundles)
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
		http.MethodGet, nil, &bundle)
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
		http.MethodGet, nil, &rules)
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
		http.MethodPost, ruleJSON, nil)
}

// SetPlacementRuleInBatch sets the placement rules in batch.
func (c *client) SetPlacementRuleInBatch(ctx context.Context, ruleOps []*RuleOp) error {
	ruleOpsJSON, err := json.Marshal(ruleOps)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetPlacementRuleInBatch", PlacementRulesInBatch,
		http.MethodPost, ruleOpsJSON, nil)
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
		http.MethodPost, bundlesJSON, nil)
}

// DeletePlacementRule deletes the placement rule.
func (c *client) DeletePlacementRule(ctx context.Context, group, id string) error {
	return c.requestWithRetry(ctx,
		"DeletePlacementRule", PlacementRuleByGroupAndID(group, id),
		http.MethodDelete, nil, nil)
}

// GetAllPlacementRuleGroups gets all placement rule groups.
func (c *client) GetAllPlacementRuleGroups(ctx context.Context) ([]*RuleGroup, error) {
	var ruleGroups []*RuleGroup
	err := c.requestWithRetry(ctx,
		"GetAllPlacementRuleGroups", placementRuleGroups,
		http.MethodGet, nil, &ruleGroups)
	if err != nil {
		return nil, err
	}
	return ruleGroups, nil
}

// GetPlacementRuleGroupByID gets the placement rule group by ID.
func (c *client) GetPlacementRuleGroupByID(ctx context.Context, id string) (*RuleGroup, error) {
	var ruleGroup RuleGroup
	err := c.requestWithRetry(ctx,
		"GetPlacementRuleGroupByID", PlacementRuleGroupByID(id),
		http.MethodGet, nil, &ruleGroup)
	if err != nil {
		return nil, err
	}
	return &ruleGroup, nil
}

// SetPlacementRuleGroup sets the placement rule group.
func (c *client) SetPlacementRuleGroup(ctx context.Context, ruleGroup *RuleGroup) error {
	ruleGroupJSON, err := json.Marshal(ruleGroup)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetPlacementRuleGroup", placementRuleGroup,
		http.MethodPost, ruleGroupJSON, nil)
}

// DeletePlacementRuleGroupByID deletes the placement rule group by ID.
func (c *client) DeletePlacementRuleGroupByID(ctx context.Context, id string) error {
	return c.requestWithRetry(ctx,
		"DeletePlacementRuleGroupByID", PlacementRuleGroupByID(id),
		http.MethodDelete, nil, nil)
}

// GetAllRegionLabelRules gets all region label rules.
func (c *client) GetAllRegionLabelRules(ctx context.Context) ([]*LabelRule, error) {
	var labelRules []*LabelRule
	err := c.requestWithRetry(ctx,
		"GetAllRegionLabelRules", RegionLabelRules,
		http.MethodGet, nil, &labelRules)
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
		http.MethodGet, idsJSON, &labelRules)
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
		http.MethodPost, labelRuleJSON, nil)
}

// PatchRegionLabelRules patches the region label rules.
func (c *client) PatchRegionLabelRules(ctx context.Context, labelRulePatch *LabelRulePatch) error {
	labelRulePatchJSON, err := json.Marshal(labelRulePatch)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"PatchRegionLabelRules", RegionLabelRules,
		http.MethodPatch, labelRulePatchJSON, nil)
}

// GetSchedulers gets the schedulers from PD cluster.
func (c *client) GetSchedulers(ctx context.Context) ([]string, error) {
	var schedulers []string
	err := c.requestWithRetry(ctx, "GetSchedulers", Schedulers,
		http.MethodGet, nil, &schedulers)
	if err != nil {
		return nil, err
	}
	return schedulers, nil
}

// CreateScheduler creates a scheduler to PD cluster.
func (c *client) CreateScheduler(ctx context.Context, name string, storeID uint64) error {
	inputJSON, err := json.Marshal(map[string]interface{}{
		"name":     name,
		"store_id": storeID,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"CreateScheduler", Schedulers,
		http.MethodPost, inputJSON, nil)
}

// AccelerateSchedule accelerates the scheduling of the regions within the given key range.
// The keys in the key range should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) AccelerateSchedule(ctx context.Context, keyRange *KeyRange) error {
	startKey, endKey := keyRange.EscapeAsHexStr()
	inputJSON, err := json.Marshal(map[string]string{
		"start_key": startKey,
		"end_key":   endKey,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"AccelerateSchedule", AccelerateSchedule,
		http.MethodPost, inputJSON, nil)
}

// AccelerateScheduleInBatch accelerates the scheduling of the regions within the given key ranges in batch.
// The keys in the key ranges should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) AccelerateScheduleInBatch(ctx context.Context, keyRanges []*KeyRange) error {
	input := make([]map[string]string, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		startKey, endKey := keyRange.EscapeAsHexStr()
		input = append(input, map[string]string{
			"start_key": startKey,
			"end_key":   endKey,
		})
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"AccelerateScheduleInBatch", AccelerateScheduleInBatch,
		http.MethodPost, inputJSON, nil)
}

// SetSchedulerDelay sets the delay of given scheduler.
func (c *client) SetSchedulerDelay(ctx context.Context, scheduler string, delaySec int64) error {
	m := map[string]int64{
		"delay": delaySec,
	}
	inputJSON, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return c.requestWithRetry(ctx,
		"SetSchedulerDelay", SchedulerByName(scheduler),
		http.MethodPost, inputJSON, nil)
}

// GetMinResolvedTSByStoresIDs get min-resolved-ts by stores IDs.
// - When storeIDs has zero length, it will return (cluster-level's min_resolved_ts, nil, nil) when no error.
// - When storeIDs is {"cluster"}, it will return (cluster-level's min_resolved_ts, stores_min_resolved_ts, nil) when no error.
// - When storeID is specified to ID lists, it will return (min_resolved_ts of given stores, stores_min_resolved_ts, nil) when no error.
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
		http.MethodGet, nil, &resp)
	if err != nil {
		return 0, nil, err
	}
	if !resp.IsRealTime {
		return 0, nil, errors.Trace(errors.New("min resolved ts is not enabled"))
	}
	return resp.MinResolvedTS, resp.StoresMinResolvedTS, nil
}

// GetMicroServiceMembers gets the members of the microservice.
func (c *client) GetMicroServiceMembers(ctx context.Context, service string) ([]string, error) {
	var members []string
	err := c.requestWithRetry(ctx,
		"GetMicroServiceMembers", MicroServiceMembers(service),
		http.MethodGet, nil, &members)
	if err != nil {
		return nil, err
	}
	return members, nil
}
