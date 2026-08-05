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

package regionmeta

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

const (
	callerID       = "pd-ctl"
	redirectorName = "pd-ctl-region-meta-consistency"
)

type rateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func (l *rateLimiter) wait(ctx context.Context, cost int) error {
	if cost < 1 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	start := now
	if l.next.After(start) {
		start = l.next
	}
	l.next = start.Add(time.Duration(cost) * l.interval)
	l.mu.Unlock()
	if !start.After(now) {
		return nil
	}
	timer := time.NewTimer(start.Sub(now))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type httpClient struct {
	endpoint      string
	client        *http.Client
	limiter       *rateLimiter
	timeout       time.Duration
	retries       int
	authorization string
	requests      atomic.Int64
	responseBytes atomic.Int64
}

type httpStatusError struct {
	status  int
	message string
}

func (e *httpStatusError) Error() string {
	return e.message
}

func isHTTPStatus(err error, status int) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && statusErr.status == status
}

func newSharedHTTPClient(tlsConfig *tls.Config) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig.Clone()
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *httpClient) getJSON(
	ctx context.Context,
	path string,
	query url.Values,
	local bool,
	rateLimitReserved bool,
	destination any,
) error {
	target := c.endpoint + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 || !rateLimitReserved {
			if err := c.limiter.wait(ctx, 1); err != nil {
				return err
			}
		}
		requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target, nil)
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set(apiutil.XCallerIDHeader, callerID)
		if local {
			req.Header.Set(apiutil.PDAllowFollowerHandleHeader, "true")
			req.Header.Set(apiutil.PDRedirectorHeader, redirectorName)
		}
		if c.authorization != "" {
			req.Header.Set("Authorization", c.authorization)
		}
		c.requests.Add(1)
		resp, err := c.client.Do(req)
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("%s%s: %w", c.endpoint, path, err)
		} else {
			payload, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
			_ = resp.Body.Close()
			cancel()
			c.responseBytes.Add(int64(len(payload)))
			if readErr != nil {
				lastErr = fmt.Errorf("%s%s: %w", c.endpoint, path, readErr)
			} else if len(payload) > maxResponseBytes {
				return fmt.Errorf("%s%s: response exceeds 8 MiB", c.endpoint, path)
			} else if resp.StatusCode == http.StatusOK {
				if err := json.Unmarshal(payload, destination); err != nil {
					return fmt.Errorf("%s%s: invalid JSON: %w", c.endpoint, path, err)
				}
				return nil
			} else {
				message := strings.TrimSpace(string(payload))
				if len(message) > 512 {
					message = message[:512]
				}
				lastErr = &httpStatusError{
					status: resp.StatusCode,
					message: fmt.Sprintf("%s%s: unexpected HTTP %d%s",
						c.endpoint, path, resp.StatusCode, errorSuffix(message)),
				}
				if !retryableStatus(resp.StatusCode) {
					return lastErr
				}
				if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
					if err := waitContext(ctx, min(time.Duration(seconds)*time.Second, 5*time.Second)); err != nil {
						return err
					}
				}
			}
		}
		if attempt < c.retries {
			if err := waitContext(ctx, min(100*time.Millisecond<<attempt, time.Second)); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func errorSuffix(message string) string {
	if message == "" {
		return ""
	}
	return ": " + message
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusInternalServerError ||
		status == http.StatusBadGateway || status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type requestCall struct {
	client        *httpClient
	path          string
	query         url.Values
	local         bool
	allowNotFound bool
	destination   any
}

type batchRequester struct {
	limiter *rateLimiter
}

func (r *batchRequester) getJSON(ctx context.Context, calls []requestCall) error {
	if len(calls) == 0 {
		return nil
	}
	seen := make(map[*httpClient]struct{}, len(calls))
	for _, call := range calls {
		if _, ok := seen[call.client]; ok {
			return errors.New("a PD member cannot receive concurrent checker requests")
		}
		seen[call.client] = struct{}{}
	}
	if err := r.limiter.wait(ctx, len(calls)); err != nil {
		return err
	}
	if len(calls) == 1 {
		call := calls[0]
		err := call.client.getJSON(ctx, call.path, call.query, call.local, true, call.destination)
		if call.allowNotFound && isHTTPStatus(err, http.StatusNotFound) {
			return nil
		}
		return err
	}

	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := make(chan struct{})
	ready := sync.WaitGroup{}
	ready.Add(len(calls))
	errorsByIndex := make([]error, len(calls))
	wg := sync.WaitGroup{}
	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			select {
			case <-start:
			case <-groupCtx.Done():
				errorsByIndex[i] = groupCtx.Err()
				return
			}
			err := call.client.getJSON(groupCtx, call.path, call.query, call.local, true, call.destination)
			if call.allowNotFound && isHTTPStatus(err, http.StatusNotFound) {
				err = nil
			}
			errorsByIndex[i] = err
			if err != nil {
				cancel()
			}
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()
	for _, err := range errorsByIndex {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	if err := groupCtx.Err(); err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

type wireMember struct {
	Name       string   `json:"name"`
	MemberID   uint64   `json:"member_id"`
	ClientURLs []string `json:"client_urls"`
}

type wireHeader struct {
	ClusterID uint64 `json:"cluster_id"`
}

type membership struct {
	Header  wireHeader   `json:"header"`
	Members []wireMember `json:"members"`
	Leader  wireMember   `json:"leader"`
}

type memberSignature struct {
	ClusterID uint64
	LeaderID  uint64
	Members   []string
}

func (m membership) signature() (memberSignature, error) {
	if len(m.Members) < 2 || m.Leader.MemberID == 0 {
		return memberSignature{}, errors.New("members response does not identify at least two members and the PD leader")
	}
	values := make([]string, 0, len(m.Members))
	leaderFound := false
	for _, member := range m.Members {
		if member.MemberID == 0 || len(member.ClientURLs) == 0 {
			return memberSignature{}, errors.New("/members contains a member without id or client_urls")
		}
		if member.MemberID == m.Leader.MemberID {
			leaderFound = true
		}
		urls := make([]string, 0, len(member.ClientURLs))
		for _, raw := range member.ClientURLs {
			value, err := normalizeURL(raw)
			if err != nil {
				return memberSignature{}, err
			}
			urls = append(urls, value)
		}
		slices.Sort(urls)
		values = append(values, fmt.Sprintf("%d\x00%s\x00%s", member.MemberID, member.Name, strings.Join(urls, "\x00")))
	}
	if !leaderFound {
		return memberSignature{}, errors.New("current PD leader is not present in the member list")
	}
	slices.Sort(values)
	return memberSignature{ClusterID: m.Header.ClusterID, LeaderID: m.Leader.MemberID, Members: values}, nil
}

func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", fmt.Errorf("invalid PD URL %q", raw)
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("supplied PD URL must not include credentials, path, query, or fragment: %q", raw)
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else if value, err := strconv.ParseUint(port, 10, 16); err != nil || value == 0 {
		return "", fmt.Errorf("invalid PD URL port in %q", raw)
	}
	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port), nil
}

func readAuthorization(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(payload))
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("authorization file must contain exactly one non-empty line")
	}
	return value, nil
}

func discoverNodes(
	ctx context.Context,
	seed *httpClient,
	supplied []string,
) ([]node, membership, error) {
	var current membership
	if err := seed.getJSON(ctx, "/pd/api/v1/members", nil, true, false, &current); err != nil {
		return nil, membership{}, err
	}
	if _, err := current.signature(); err != nil {
		return nil, membership{}, err
	}
	chosen := make(map[uint64]string, len(current.Members))
	if len(supplied) == 1 {
		for _, member := range current.Members {
			endpoint, err := normalizeURL(member.ClientURLs[0])
			if err != nil {
				return nil, membership{}, err
			}
			chosen[member.MemberID] = endpoint
		}
	} else {
		for _, member := range current.Members {
			matches := make([]string, 0, 1)
			for _, raw := range member.ClientURLs {
				endpoint, err := normalizeURL(raw)
				if err != nil {
					return nil, membership{}, err
				}
				if slices.Contains(supplied, endpoint) {
					matches = append(matches, endpoint)
				}
			}
			if len(matches) != 1 {
				return nil, membership{}, fmt.Errorf("direct endpoint for PD member %q is missing or ambiguous", member.Name)
			}
			chosen[member.MemberID] = matches[0]
		}
		advertised := make(map[string]struct{}, len(chosen))
		for _, endpoint := range chosen {
			advertised[endpoint] = struct{}{}
		}
		for _, endpoint := range supplied {
			if _, ok := advertised[endpoint]; !ok {
				return nil, membership{}, fmt.Errorf("supplied endpoint is not a PD member client URL: %s", endpoint)
			}
		}
	}
	members := slices.Clone(current.Members)
	slices.SortFunc(members, func(a, b wireMember) int {
		if (a.MemberID == current.Leader.MemberID) != (b.MemberID == current.Leader.MemberID) {
			if a.MemberID == current.Leader.MemberID {
				return -1
			}
			return 1
		}
		return compareUint64(a.MemberID, b.MemberID)
	})
	names := make(map[string]struct{}, len(members))
	endpoints := make(map[string]struct{}, len(members))
	nodes := make([]node, 0, len(members))
	for _, member := range members {
		name := member.Name
		if name == "" {
			name = fmt.Sprintf("member-%d", member.MemberID)
		}
		if _, exists := names[name]; exists {
			return nil, membership{}, errors.New("member names in the PD response must be unique")
		}
		names[name] = struct{}{}
		endpoint := chosen[member.MemberID]
		if _, exists := endpoints[endpoint]; exists {
			return nil, membership{}, errors.New("members in the PD response must advertise distinct client URLs")
		}
		endpoints[endpoint] = struct{}{}
		role := "follower"
		if member.MemberID == current.Leader.MemberID {
			role = "leader"
		}
		nodes = append(nodes, node{MemberID: member.MemberID, Name: name, URL: endpoint, Role: role})
	}
	return nodes, current, nil
}
