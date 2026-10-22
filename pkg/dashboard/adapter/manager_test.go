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

package adapter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb-dashboard/pkg/config"

	"github.com/tikv/pd/pkg/dashboard/spiffetls"
)

func TestManagerSQLTLSLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		staticTLS  bool
		initError  bool
		startError bool
		stopError  bool
		closeError bool
		restart    bool
		wantEvents []string
	}{
		{
			name:       "static TLS is unchanged",
			staticTLS:  true,
			wantEvents: []string{"start", "stop"},
		},
		{
			name:       "stop closes source after service",
			wantEvents: []string{"new1", "start", "stop", "close1"},
		},
		{
			name:       "initialization failure retries without starting plaintext",
			initError:  true,
			wantEvents: []string{"new1", "new2", "start", "stop", "close2"},
		},
		{
			name:       "service failure closes source before retry",
			startError: true,
			wantEvents: []string{"new1", "start", "close1", "new2", "start", "stop", "close2"},
		},
		{
			name:       "shutdown error still closes source",
			stopError:  true,
			wantEvents: []string{"new1", "start", "stop", "close1"},
		},
		{
			name:       "migration back creates a fresh source",
			restart:    true,
			wantEvents: []string{"new1", "start", "stop", "close1", "new2", "start", "stop", "close2"},
		},
		{
			name:       "close error does not retain source on restart",
			closeError: true,
			restart:    true,
			wantEvents: []string{"new1", "start", "stop", "close1", "new2", "start", "stop", "close2"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.Default()
			staticTLS := &tls.Config{MinVersion: tls.VersionTLS13}
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			m := &Manager{ctx: ctx, config: cfg}
			if test.staticTLS {
				cfg.TiDBTLSConfig = staticTLS
			} else {
				m.spiffeConfig = spiffetls.Config{Endpoint: "unix:///test/spire.sock", Timeout: time.Second}
			}

			var events []string
			var activeSource *managerTestSource
			var previousTLS *tls.Config
			newCalls := 0
			m.newSPIFFESource = func(ctx context.Context, _ spiffetls.Config) (tlsSource, error) {
				newCalls++
				events = append(events, fmt.Sprintf("new%d", newCalls))
				_, ok := ctx.Deadline()
				require.True(t, ok)
				if test.initError && newCalls == 1 {
					return nil, errors.New("agent unavailable")
				}
				sourceID := newCalls
				source := &managerTestSource{config: &tls.Config{MinVersion: tls.VersionTLS13}}
				source.close = func() error {
					events = append(events, fmt.Sprintf("close%d", sourceID))
					if test.closeError {
						return errors.New("close failed")
					}
					return nil
				}
				activeSource = source
				return source, nil
			}
			startCalls := 0
			service := &managerTestService{
				start: func(ctx context.Context) error {
					events = append(events, "start")
					require.NoError(t, ctx.Err())
					require.NotNil(t, cfg.TiDBTLSConfig)
					if !test.staticTLS {
						require.Same(t, activeSource.config, cfg.TiDBTLSConfig)
						if previousTLS != nil {
							require.NotSame(t, previousTLS, cfg.TiDBTLSConfig)
						}
						previousTLS = cfg.TiDBTLSConfig
					}
					startCalls++
					if test.startError && startCalls == 1 {
						return errors.New("dashboard startup failed")
					}
					return nil
				},
				stop: func(ctx context.Context) error {
					events = append(events, "stop")
					require.NoError(t, ctx.Err())
					_, ok := ctx.Deadline()
					require.True(t, ok)
					if activeSource != nil {
						require.False(t, activeSource.closed)
					}
					if test.stopError {
						return errors.New("dashboard shutdown failed")
					}
					return nil
				},
			}
			m.service = service

			m.startService()
			m.startService()
			if test.restart {
				m.stopService()
				m.startService()
			}
			cancel()
			m.stopService()
			m.stopService()

			state := struct {
				Events      []string
				Running     bool
				HasSource   bool
				HasTLS      bool
				StaticIsSet bool
			}{events, service.running, m.tlsSource != nil, cfg.TiDBTLSConfig != nil, cfg.TiDBTLSConfig == staticTLS}
			want := state
			want.Events = test.wantEvents
			want.Running = false
			want.HasSource = false
			want.HasTLS = test.staticTLS
			want.StaticIsSet = test.staticTLS
			require.Empty(t, cmp.Diff(want, state))
		})
	}
}

func TestManagerSQLTLSCancellation(t *testing.T) {
	for _, test := range []struct {
		name          string
		cancelOnFetch bool
		returnSource  bool
	}{
		{name: "initial fetch timeout"},
		{name: "manager canceled during fetch", cancelOnFetch: true},
		{name: "manager canceled after fetch", cancelOnFetch: true, returnSource: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			source := &managerTestSource{config: &tls.Config{MinVersion: tls.VersionTLS13}}
			service := &managerTestService{start: func(context.Context) error {
				t.Fatal("dashboard must not start after TLS initialization is canceled")
				return nil
			}}
			m := &Manager{
				ctx:          ctx,
				service:      service,
				config:       config.Default(),
				spiffeConfig: spiffetls.Config{Endpoint: "unix:///test/spire.sock", Timeout: time.Millisecond},
				newSPIFFESource: func(ctx context.Context, _ spiffetls.Config) (tlsSource, error) {
					if test.cancelOnFetch {
						cancel()
					}
					if test.returnSource {
						return source, nil
					}
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}
			m.startService()
			require.Nil(t, m.tlsSource)
			require.Nil(t, m.config.TiDBTLSConfig)
			require.Equal(t, test.returnSource, source.closed)
		})
	}
}

type managerTestService struct {
	running bool
	start   func(context.Context) error
	stop    func(context.Context) error
}

func (s *managerTestService) IsRunning() bool {
	return s.running
}

func (s *managerTestService) Start(ctx context.Context) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	s.running = true
	return nil
}

func (s *managerTestService) Stop(ctx context.Context) error {
	s.running = false
	return s.stop(ctx)
}

type managerTestSource struct {
	config *tls.Config
	closed bool
	close  func() error
}

func (s *managerTestSource) TLSConfig() *tls.Config {
	return s.config
}

func (s *managerTestSource) Close() error {
	s.closed = true
	if s.close != nil {
		return s.close()
	}
	return nil
}
