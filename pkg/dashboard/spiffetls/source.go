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

// Package spiffetls provides rotating SPIFFE credentials for Dashboard SQL clients.
package spiffetls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

// Config selects a Workload API endpoint and the client and server identities
// allowed for SQL TLS. Patterns accept exact SPIFFE IDs or whole wildcard path
// segments; trust domains must always be explicit.
type Config struct {
	Endpoint        string
	Timeout         time.Duration
	ClientIDPattern string
	ServerIDPattern string
}

// Validate checks a complete, enabled SPIFFE configuration. Endpoint must be an
// absolute Unix socket URI and Timeout must bound initial Workload API discovery.
func (c Config) Validate() error {
	endpoint, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("parse Workload API endpoint: %w", err)
	}
	if !strings.HasPrefix(c.Endpoint, "unix://") || endpoint.Scheme != "unix" ||
		endpoint.Host != "" || endpoint.User != nil || endpoint.Opaque != "" ||
		!path.IsAbs(endpoint.Path) || endpoint.RawQuery != "" || endpoint.ForceQuery || strings.Contains(c.Endpoint, "#") {
		return errors.New("workload API endpoint must be an absolute Unix socket URI without authority, query, or fragment")
	}
	if err := workloadapi.ValidateAddress(c.Endpoint); err != nil {
		return fmt.Errorf("validate Workload API endpoint: %w", err)
	}
	if c.Timeout <= 0 {
		return errors.New("workload API timeout must be positive")
	}
	if _, err := parsePattern(c.ClientIDPattern); err != nil {
		return fmt.Errorf("validate client SPIFFE ID pattern: %w", err)
	}
	if _, err := parsePattern(c.ServerIDPattern); err != nil {
		return fmt.Errorf("validate server SPIFFE ID pattern: %w", err)
	}
	return nil
}

// Source owns a rotating Workload API source. Close it after all SQL connections
// using its TLS configurations have been closed.
type Source struct {
	source    *workloadapi.X509Source
	tlsConfig *tls.Config
	closeOnce sync.Once
	closeErr  error
}

// New waits for initial credentials and a trusted server bundle, bounded by
// config.Timeout and ctx. The rotating watch remains active until Close, even
// when the initialization context is canceled after New returns.
func New(ctx context.Context, config Config) (*Source, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate SPIFFE TLS configuration: %w", err)
	}
	clientPattern, _ := parsePattern(config.ClientIDPattern)
	serverPattern, _ := parsePattern(config.ServerIDPattern)
	initCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	source, err := workloadapi.NewX509Source(initCtx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(config.Endpoint)),
		workloadapi.WithDefaultX509SVIDPicker(clientPattern.pickSVID))
	if err != nil {
		return nil, fmt.Errorf("initialize Workload API X509 source: %w", err)
	}
	result := &Source{source: source}
	matchingSource := matchingSource{source: source, pattern: clientPattern}
	if _, err := matchingSource.GetX509SVID(); err != nil {
		return nil, errors.Join(fmt.Errorf("initialize client SPIFFE identity: %w", err), result.Close())
	}
	bundle, err := source.GetX509BundleForTrustDomain(serverPattern.trustDomain)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("initialize server SPIFFE trust bundle: %w", err), result.Close())
	}
	if bundle == nil || len(bundle.X509Authorities()) == 0 {
		return nil, errors.Join(errors.New("server SPIFFE trust bundle has no authorities"), result.Close())
	}
	configTLS := tlsconfig.MTLSClientConfig(matchingSource, source,
		func(id spiffeid.ID, _ [][]*x509.Certificate) error {
			if !serverPattern.matches(id) {
				return fmt.Errorf("server SPIFFE ID %q does not match pattern %q", id, config.ServerIDPattern)
			}
			return nil
		})
	configTLS.MinVersion = tls.VersionTLS13
	result.tlsConfig = configTLS
	return result, nil
}

// TLSConfig returns an independent TLS configuration whose callbacks read the
// current credentials and trust bundles. Cloning it preserves rotation.
func (s *Source) TLSConfig() *tls.Config {
	return s.tlsConfig.Clone()
}

// Close stops the Workload API watch and makes future TLS handshakes fail.
// Repeated and concurrent calls return the same result.
func (s *Source) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if err := s.source.Close(); err != nil {
			s.closeErr = fmt.Errorf("close Workload API X509 source: %w", err)
		}
	})
	return s.closeErr
}

type idPattern struct {
	trustDomain spiffeid.TrustDomain
	segments    []string
}

func parsePattern(raw string) (idPattern, error) {
	segments := strings.Split(raw, "/")
	for i, segment := range segments {
		if strings.Contains(segment, "*") && (i < 3 || segment != "*") {
			return idPattern{}, errors.New("wildcards must occupy whole SPIFFE path segments")
		}
	}
	validated, err := spiffeid.FromString(strings.ReplaceAll(raw, "*", "wildcard"))
	if err != nil {
		return idPattern{}, fmt.Errorf("parse SPIFFE ID pattern %q: %w", raw, err)
	}
	return idPattern{trustDomain: validated.TrustDomain(), segments: segments[3:]}, nil
}

func (p idPattern) matches(id spiffeid.ID) bool {
	if id.TrustDomain() != p.trustDomain {
		return false
	}
	segments := strings.Split(id.Path(), "/")[1:]
	if len(segments) != len(p.segments) {
		return false
	}
	for i, segment := range p.segments {
		if segment != "*" && segment != segments[i] {
			return false
		}
	}
	return true
}

func (p idPattern) validateSVID(svid *x509svid.SVID) error {
	if svid == nil || len(svid.Certificates) == 0 || svid.Certificates[0] == nil {
		return errors.New("workload API has no unique matching client X509-SVID")
	}
	leaf := svid.Certificates[0]
	id, err := x509svid.IDFromCert(leaf)
	if err != nil {
		return fmt.Errorf("read client certificate SPIFFE ID: %w", err)
	}
	if id != svid.ID || !p.matches(id) {
		return errors.New("client X509-SVID does not match the configured SPIFFE identity")
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return errors.New("client X509-SVID is outside its validity period")
	}
	return nil
}

func (p idPattern) pickSVID(svids []*x509svid.SVID) *x509svid.SVID {
	var match *x509svid.SVID
	for _, svid := range svids {
		if svid == nil || !p.matches(svid.ID) {
			continue
		}
		if match != nil || p.validateSVID(svid) != nil {
			return nil
		}
		match = svid
	}
	return match
}

type matchingSource struct {
	source  x509svid.Source
	pattern idPattern
}

// GetX509SVID returns the current SVID only while its identity and validity
// period match the configured client policy.
func (s matchingSource) GetX509SVID() (*x509svid.SVID, error) {
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return nil, fmt.Errorf("read client X509-SVID: %w", err)
	}
	if err := s.pattern.validateSVID(svid); err != nil {
		return nil, fmt.Errorf("validate client X509-SVID: %w", err)
	}
	return svid, nil
}
