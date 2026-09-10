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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

type options struct {
	addresses  []string
	keyspaceID uint32
	security   pd.SecurityOption
	timeout    time.Duration
}

type connectFunc func(context.Context, options) (gc.GCStatesClient, func(), error)

func main() {
	// The release's client logger defaults to stdout; reserve stdout for JSON.
	sink := zapcore.Lock(zapcore.AddSync(os.Stderr))
	logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "warn"}, sink, sink)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.ReplaceGlobals(logger, props)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	cmd := newCommand(connectPD)
	err = cmd.ExecuteContext(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func connectPD(ctx context.Context, opts options) (gc.GCStatesClient, func(), error) {
	client, err := pd.NewClientWithContext(ctx, "pd-gc-barrier", opts.addresses, opts.security,
		opt.WithCustomTimeoutOption(opts.timeout))
	if err != nil {
		return nil, nil, err
	}
	return client.GetGCStatesClient(opts.keyspaceID), client.Close, nil
}

func newCommand(connect connectFunc) *cobra.Command {
	var opts options
	root := &cobra.Command{
		Use:           "pd-gc-barrier",
		Short:         "Inspect and manually manage keyspace GC barriers",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	flags := root.PersistentFlags()
	flags.StringSliceVar(&opts.addresses, "pd", nil, "PD endpoints, separated by commas")
	flags.Uint32Var(&opts.keyspaceID, "keyspace-id", 0, "Keyspace ID (0..16777215); must use keyspace-level GC")
	flags.DurationVar(&opts.timeout, "timeout", 30*time.Second, "Timeout for the entire command, including connection")
	flags.StringVar(&opts.security.CAPath, "cacert", "", "Trusted CA certificate path")
	flags.StringVar(&opts.security.CertPath, "cert", "", "Client certificate path")
	flags.StringVar(&opts.security.KeyPath, "key", "", "Client private key path")
	// A missing flag here is a programming error in command registration.
	if err := root.MarkPersistentFlagRequired("pd"); err != nil {
		panic(err)
	}
	if err := root.MarkPersistentFlagRequired("keyspace-id"); err != nil {
		panic(err)
	}

	withClient := func(cmd *cobra.Command, action func(context.Context, gc.GCStatesClient, gc.GCState) (any, error)) error {
		if opts.keyspaceID > constants.MaxKeyspaceID {
			return fmt.Errorf("keyspace ID must be in [0, %d]; unified and global GC are unsupported", constants.MaxKeyspaceID)
		}
		if opts.timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		if len(opts.addresses) == 0 {
			return errors.New("at least one PD endpoint is required")
		}
		for i, address := range opts.addresses {
			opts.addresses[i] = strings.TrimSpace(address)
			if opts.addresses[i] == "" {
				return errors.New("PD endpoints must not be empty")
			}
			if strings.HasPrefix(strings.ToLower(opts.addresses[i]), "https://") && opts.security.CertPath == "" {
				return errors.New("https requires cert and key with this version of the PD client")
			}
		}
		if (opts.security.CertPath == "") != (opts.security.KeyPath == "") {
			return errors.New("cert and key must be supplied together")
		}
		if opts.security.CAPath != "" && opts.security.CertPath == "" {
			return errors.New("cacert requires cert and key with this version of the PD client")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
		defer cancel()
		client, closeClient, err := connect(ctx, opts)
		if err != nil {
			return fmt.Errorf("connect to PD: %w", err)
		}
		defer closeClient()
		state, err := client.GetGCState(ctx)
		if err != nil {
			return fmt.Errorf("read GC state: %w", err)
		}
		if state.KeyspaceID != opts.keyspaceID {
			return fmt.Errorf("GC scope mismatch: requested keyspace %d, PD returned %d; refusing operation", opts.keyspaceID, state.KeyspaceID)
		}
		result, err := action(ctx, client, state)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	show := &cobra.Command{
		Use:   "show",
		Short: "Show safe points and keyspace barriers (not physical GC completion or global barriers)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(cmd, func(_ context.Context, _ gc.GCStatesClient, state gc.GCState) (any, error) {
				barriers := make([]*barrierOutput, 0, len(state.GCBarriers))
				sort.Slice(state.GCBarriers, func(i, j int) bool {
					if state.GCBarriers[i].BarrierTS == state.GCBarriers[j].BarrierTS {
						return state.GCBarriers[i].BarrierID < state.GCBarriers[j].BarrierID
					}
					return state.GCBarriers[i].BarrierTS < state.GCBarriers[j].BarrierTS
				})
				for _, b := range state.GCBarriers {
					barriers = append(barriers, formatBarrier(b))
				}
				return struct {
					KeyspaceID   uint32           `json:"keyspace_id"`
					TxnSafePoint timestampOutput  `json:"txn_safe_point"`
					GCSafePoint  timestampOutput  `json:"gc_safe_point"`
					Barriers     []*barrierOutput `json:"barriers"`
				}{state.KeyspaceID, formatTimestamp(state.TxnSafePoint), formatTimestamp(state.GCSafePoint), barriers}, nil
			})
		},
	}
	var ttlInput string
	set := &cobra.Command{
		Use:   "set <barrier-id> <tso-or-rfc3339-time> --ttl <duration|never>",
		Short: "Create or update a barrier; time input requires a timezone and millisecond precision",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateBarrierID(args[0]); err != nil {
				return err
			}
			ts, err := parseTimestamp(args[1])
			if err != nil {
				return err
			}
			ttl := gc.TTLNeverExpire
			if ttlInput != "never" {
				ttl, err = time.ParseDuration(ttlInput)
				if err != nil || ttl <= 0 {
					return errors.New("ttl must be a positive duration such as 2h, or never")
				}
			}
			return withClient(cmd, func(ctx context.Context, client gc.GCStatesClient, state gc.GCState) (any, error) {
				if ts < state.TxnSafePoint {
					return nil, fmt.Errorf("barrier timestamp %d is behind transaction safe point %d", ts, state.TxnSafePoint)
				}
				b, err := client.SetGCBarrier(ctx, args[0], ts, ttl)
				if err != nil {
					return nil, fmt.Errorf("set barrier: %w; query state before retrying because the write may have succeeded", err)
				}
				return struct {
					KeyspaceID uint32         `json:"keyspace_id"`
					Barrier    *barrierOutput `json:"barrier"`
				}{state.KeyspaceID, formatBarrier(b)}, nil
			})
		},
	}
	set.Flags().StringVar(&ttlInput, "ttl", "", "Lifetime such as 2h, or never (requires manual deletion)")
	if err := set.MarkFlagRequired("ttl"); err != nil {
		panic(err)
	}
	deleteCmd := &cobra.Command{
		Use:   "delete <barrier-id>",
		Short: "Delete a barrier; verify the replacement barrier before removing an old one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateBarrierID(args[0]); err != nil {
				return err
			}
			return withClient(cmd, func(ctx context.Context, client gc.GCStatesClient, state gc.GCState) (any, error) {
				b, err := client.DeleteGCBarrier(ctx, args[0])
				if err != nil {
					return nil, fmt.Errorf("delete barrier: %w; query state before retrying because the write may have succeeded", err)
				}
				return struct {
					KeyspaceID     uint32         `json:"keyspace_id"`
					DeletedBarrier *barrierOutput `json:"deleted_barrier"`
				}{state.KeyspaceID, formatBarrier(b)}, nil
			})
		},
	}
	root.AddCommand(show, set, deleteCmd)
	return root
}

func validateBarrierID(id string) error {
	if strings.TrimSpace(id) == "" || id == "gc_worker" {
		return errors.New("barrier ID must be non-empty and must not be the reserved ID gc_worker")
	}
	return nil
}

func parseTimestamp(input string) (uint64, error) {
	if ts, err := strconv.ParseUint(input, 10, 64); err == nil && ts > 0 {
		return ts, nil
	}
	t, err := time.Parse(time.RFC3339Nano, input)
	if err != nil {
		return 0, fmt.Errorf("timestamp must be a positive decimal TSO or RFC3339 time with timezone: %q", input)
	}
	ms := t.UnixMilli()
	if ms <= 0 || uint64(ms) > math.MaxUint64>>18 || t.Nanosecond()%int(time.Millisecond) != 0 {
		return 0, errors.New("time must be after the Unix epoch, fit in a TSO, and have at most millisecond precision")
	}
	return tsoutil.ComposeTS(ms, 0), nil
}

type timestampOutput struct {
	TSO  string `json:"tso"`
	Time string `json:"time"`
}

func formatTimestamp(ts uint64) timestampOutput {
	physical, _ := tsoutil.ParseTS(ts)
	return timestampOutput{strconv.FormatUint(ts, 10), physical.UTC().Format(time.RFC3339Nano)}
}

type barrierOutput struct {
	BarrierID string          `json:"barrier_id"`
	BarrierTS timestampOutput `json:"barrier_ts"`
	TTL       string          `json:"ttl"`
}

func formatBarrier(b *gc.GCBarrierInfo) *barrierOutput {
	if b == nil {
		return nil
	}
	ttl := b.TTL.String()
	if b.TTL == gc.TTLNeverExpire {
		ttl = "never"
	}
	return &barrierOutput{b.BarrierID, formatTimestamp(b.BarrierTS), ttl}
}
