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

package command

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

type gcStateReader interface {
	getGCState(context.Context, uint32) (gc.GCState, error)
	getGlobalGCState(context.Context) (gc.ClusterGCStates, error)
	getAllKeyspacesGCStates(context.Context) (gc.ClusterGCStates, error)
	close()
}

type gcStateReaderFactory func(*cobra.Command) (gcStateReader, error)

const gcStateIncludeExpiredFlag = "include-expired"

type pdGCStateReader struct {
	client pd.Client
}

type clusterGCStatesClient interface {
	GetAllKeyspacesGCStates(
		context.Context,
		...gc.GCStatesAPIOption,
	) (gc.ClusterGCStates, error)
}

func (r *pdGCStateReader) getGCState(
	ctx context.Context,
	keyspaceID uint32,
) (gc.GCState, error) {
	return r.client.GetGCStatesClient(keyspaceID).GetGCState(
		ctx,
		gc.ExcludeGCBarriers(false),
	)
}

func (r *pdGCStateReader) getAllKeyspacesGCStates(
	ctx context.Context,
) (gc.ClusterGCStates, error) {
	return readClusterGCStates(
		ctx,
		r.client.GetGCStatesClient(constant.NullKeyspaceID),
		false,
	)
}

func (r *pdGCStateReader) getGlobalGCState(
	ctx context.Context,
) (gc.ClusterGCStates, error) {
	return readClusterGCStates(
		ctx,
		r.client.GetGCStatesClient(constant.NullKeyspaceID),
		true,
	)
}

func readClusterGCStates(
	ctx context.Context,
	client clusterGCStatesClient,
	excludeGCBarriers bool,
) (gc.ClusterGCStates, error) {
	return client.GetAllKeyspacesGCStates(
		ctx,
		gc.ExcludeGCBarriers(excludeGCBarriers),
		gc.ExcludeGlobalGCBarriers(false),
	)
}

func (r *pdGCStateReader) close() {
	r.client.Close()
}

func newPDGCStateReader(cmd *cobra.Command) (gcStateReader, error) {
	caPath, err := cmd.Flags().GetString("cacert")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	certPath, err := cmd.Flags().GetString("cert")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	keyPath, err := cmd.Flags().GetString("key")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client, err := pd.NewClientWithContext(
		cmd.Context(),
		caller.Component(PDControlCallerID),
		getEndpoints(cmd),
		pd.SecurityOption{
			CAPath:   caPath,
			CertPath: certPath,
			KeyPath:  keyPath,
		},
	)
	if err != nil {
		return nil, err
	}
	return &pdGCStateReader{client: client}, nil
}

type gcBarrierOutput struct {
	BarrierID  string `json:"barrier_id"`
	BarrierTS  uint64 `json:"barrier_ts"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type gcStateOutput struct {
	KeyspaceID        uint32            `json:"keyspace_id"`
	IsKeyspaceLevelGC bool              `json:"is_keyspace_level_gc"`
	TxnSafePoint      uint64            `json:"txn_safe_point"`
	GCSafePoint       uint64            `json:"gc_safe_point"`
	GCBarriers        []gcBarrierOutput `json:"gc_barriers"`
}

type keyspaceGCStateOutput struct {
	RequestedKeyspaceID uint32            `json:"requested_keyspace_id"`
	EffectiveKeyspaceID uint32            `json:"effective_keyspace_id"`
	IsKeyspaceLevelGC   bool              `json:"is_keyspace_level_gc"`
	TxnSafePoint        uint64            `json:"txn_safe_point"`
	GCSafePoint         uint64            `json:"gc_safe_point"`
	GCBarriers          []gcBarrierOutput `json:"gc_barriers"`
}

type allGCStatesOutput struct {
	GCStates         []gcStateOutput   `json:"gc_states"`
	GlobalGCBarriers []gcBarrierOutput `json:"global_gc_barriers"`
}

type globalGCStateOutput struct {
	GlobalGCBarriers []gcBarrierOutput `json:"global_gc_barriers"`
}

func parseGCStateKeyspaceID(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid keyspace ID %q", value)
	}
	keyspaceID := uint32(parsed)
	if keyspaceID > constant.MaxValidKeyspaceID && keyspaceID != constant.NullKeyspaceID {
		return 0, errors.Errorf(
			"invalid keyspace ID %q: expected 0 through %d or %d",
			value,
			constant.MaxValidKeyspaceID,
			constant.NullKeyspaceID,
		)
	}
	return keyspaceID, nil
}

func gcStateTTLSeconds(ttl time.Duration) int64 {
	if ttl == gc.TTLNeverExpire {
		return math.MaxInt64
	}
	return int64(ttl / time.Second)
}

func sortGCBarrierOutputs(barriers []gcBarrierOutput) {
	sort.Slice(barriers, func(i, j int) bool {
		if barriers[i].BarrierTS != barriers[j].BarrierTS {
			return barriers[i].BarrierTS < barriers[j].BarrierTS
		}
		return barriers[i].BarrierID < barriers[j].BarrierID
	})
}

func shouldIncludeGCBarrier(ttl time.Duration, includeExpired bool) bool {
	// Expired barriers are lazily deleted and may still be returned by PD with
	// a zero TTL. They no longer block GC, so omit them from the effective view
	// unless the caller explicitly requests the persisted entries.
	return includeExpired || ttl > 0
}

func newLocalGCBarrierOutputs(barriers []*gc.GCBarrierInfo, includeExpired bool) []gcBarrierOutput {
	result := make([]gcBarrierOutput, 0, len(barriers))
	for _, barrier := range barriers {
		if barrier == nil || !shouldIncludeGCBarrier(barrier.TTL, includeExpired) {
			continue
		}
		result = append(result, gcBarrierOutput{
			BarrierID:  barrier.BarrierID,
			BarrierTS:  barrier.BarrierTS,
			TTLSeconds: gcStateTTLSeconds(barrier.TTL),
		})
	}
	sortGCBarrierOutputs(result)
	return result
}

func newGlobalGCBarrierOutputs(barriers []*gc.GlobalGCBarrierInfo, includeExpired bool) []gcBarrierOutput {
	result := make([]gcBarrierOutput, 0, len(barriers))
	for _, barrier := range barriers {
		if barrier == nil || !shouldIncludeGCBarrier(barrier.TTL, includeExpired) {
			continue
		}
		result = append(result, gcBarrierOutput{
			BarrierID:  barrier.BarrierID,
			BarrierTS:  barrier.BarrierTS,
			TTLSeconds: gcStateTTLSeconds(barrier.TTL),
		})
	}
	sortGCBarrierOutputs(result)
	return result
}

func newKeyspaceGCStateOutput(
	requestedKeyspaceID uint32,
	state gc.GCState,
	includeExpired bool,
) (keyspaceGCStateOutput, error) {
	barriers, err := state.GetGCBarriers()
	if err != nil {
		return keyspaceGCStateOutput{}, errors.Annotatef(
			err,
			"failed to read GC barriers for keyspace %d",
			requestedKeyspaceID,
		)
	}
	return keyspaceGCStateOutput{
		RequestedKeyspaceID: requestedKeyspaceID,
		EffectiveKeyspaceID: state.KeyspaceID,
		IsKeyspaceLevelGC:   state.IsKeyspaceLevelGC,
		TxnSafePoint:        state.TxnSafePoint,
		GCSafePoint:         state.GCSafePoint,
		GCBarriers:          newLocalGCBarrierOutputs(barriers, includeExpired),
	}, nil
}

func newGCStateOutput(state gc.GCState, includeExpired bool) (gcStateOutput, error) {
	barriers, err := state.GetGCBarriers()
	if err != nil {
		return gcStateOutput{}, errors.Annotatef(
			err,
			"failed to read GC barriers for keyspace %d",
			state.KeyspaceID,
		)
	}
	return gcStateOutput{
		KeyspaceID:        state.KeyspaceID,
		IsKeyspaceLevelGC: state.IsKeyspaceLevelGC,
		TxnSafePoint:      state.TxnSafePoint,
		GCSafePoint:       state.GCSafePoint,
		GCBarriers:        newLocalGCBarrierOutputs(barriers, includeExpired),
	}, nil
}

func newAllGCStatesOutput(clusterState gc.ClusterGCStates, includeExpired bool) (allGCStatesOutput, error) {
	states := make([]gcStateOutput, 0, len(clusterState.GCStates))
	for _, state := range clusterState.GCStates {
		// Unified-GC keyspaces have marker entries, but their GC state is owned by
		// the NullKeyspace scope and must not be presented as a separate scope.
		if !state.IsKeyspaceLevelGC && state.KeyspaceID != constant.NullKeyspaceID {
			continue
		}
		converted, err := newGCStateOutput(state, includeExpired)
		if err != nil {
			return allGCStatesOutput{}, err
		}
		states = append(states, converted)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].KeyspaceID < states[j].KeyspaceID
	})

	globalOutput, err := newGlobalGCStateOutput(clusterState, includeExpired)
	if err != nil {
		return allGCStatesOutput{}, err
	}
	return allGCStatesOutput{
		GCStates:         states,
		GlobalGCBarriers: globalOutput.GlobalGCBarriers,
	}, nil
}

func newGlobalGCStateOutput(clusterState gc.ClusterGCStates, includeExpired bool) (globalGCStateOutput, error) {
	globalBarriers, err := clusterState.GetGlobalGCBarriers()
	if err != nil {
		return globalGCStateOutput{}, errors.Annotate(err, "failed to read global GC barriers")
	}
	return globalGCStateOutput{
		GlobalGCBarriers: newGlobalGCBarrierOutputs(globalBarriers, includeExpired),
	}, nil
}

func getGCStateIncludeExpired(cmd *cobra.Command) (bool, error) {
	includeExpired, err := cmd.Flags().GetBool(gcStateIncludeExpiredFlag)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return includeExpired, nil
}

// NewGCStateCommand returns the read-only GC state command.
func NewGCStateCommand() *cobra.Command {
	return buildGCStateCommand(newPDGCStateReader)
}

func buildGCStateCommand(factory gcStateReaderFactory) *cobra.Command {
	command := &cobra.Command{
		Use:   "gc-state",
		Short: "show keyspace and cluster-wide GC state",
		Long: "Show effective per-keyspace GC safe points and local barriers, " +
			"and cluster-wide GC state. Expired barriers awaiting lazy deletion " +
			"are hidden by default; use --include-expired to include zero-TTL " +
			"barriers returned by PD. Use keyspace for one effective GC " +
			"scope, global for cluster-wide state, or all for a combined view.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	command.PersistentFlags().Bool(
		gcStateIncludeExpiredFlag,
		false,
		"include zero-TTL barriers returned by PD, which normally represent expired barriers awaiting lazy deletion",
	)
	command.AddCommand(
		newGCStateKeyspaceCommand(factory),
		newGCStateGlobalCommand(factory),
		newGCStateAllCommand(factory),
	)
	return command
}

func newGCStateKeyspaceCommand(factory gcStateReaderFactory) *cobra.Command {
	nullKeyspaceID := strconv.FormatUint(uint64(constant.NullKeyspaceID), 10)
	return &cobra.Command{
		Use:   "keyspace <keyspace-id>",
		Short: "show one keyspace's effective GC state",
		Long: "Show one keyspace's effective GC safe points and local barriers. " +
			"Use gc-state global to inspect only cluster-wide state, or " +
			"gc-state all for a combined view. " +
			"The decimal NullKeyspace ID is " + nullKeyspaceID + ".",
		Example: "  pd-ctl gc-state keyspace 42\n" +
			"  pd-ctl gc-state keyspace " + nullKeyspaceID,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keyspaceID, err := parseGCStateKeyspaceID(args[0])
			if err != nil {
				return err
			}
			includeExpired, err := getGCStateIncludeExpired(cmd)
			if err != nil {
				return err
			}
			reader, err := factory(cmd)
			if err != nil {
				return errors.Annotate(err, "failed to create PD RPC client")
			}
			defer reader.close()

			state, err := reader.getGCState(cmd.Context(), keyspaceID)
			if err != nil {
				if status.Code(err) == codes.Unimplemented {
					return errors.Annotate(err,
						"gc-state requires a PD server that supports GetGCState")
				}
				return errors.Annotatef(err,
					"failed to get GC state for keyspace %d", keyspaceID)
			}
			output, err := newKeyspaceGCStateOutput(keyspaceID, state, includeExpired)
			if err != nil {
				return err
			}
			return writeGCStateJSON(cmd, output)
		},
	}
}

func newGCStateGlobalCommand(factory gcStateReaderFactory) *cobra.Command {
	return &cobra.Command{
		Use:     "global",
		Short:   "show cluster-wide GC state",
		Long:    "Show cluster-wide GC state without per-keyspace states. The current output contains global GC barriers.",
		Example: "  pd-ctl gc-state global",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			includeExpired, err := getGCStateIncludeExpired(cmd)
			if err != nil {
				return err
			}
			reader, err := factory(cmd)
			if err != nil {
				return errors.Annotate(err, "failed to create PD RPC client")
			}
			defer reader.close()

			clusterState, err := reader.getGlobalGCState(cmd.Context())
			if err != nil {
				if status.Code(err) == codes.Unimplemented {
					return errors.Annotate(err,
						"gc-state global requires a PD server that supports "+
							"GetAllKeyspacesGCStates")
				}
				return errors.Annotate(err, "failed to get global GC state")
			}
			output, err := newGlobalGCStateOutput(clusterState, includeExpired)
			if err != nil {
				return err
			}
			return writeGCStateJSON(cmd, output)
		},
	}
}

func newGCStateAllCommand(factory gcStateReaderFactory) *cobra.Command {
	return &cobra.Command{
		Use:   "all",
		Short: "show effective GC scopes and cluster-wide GC state",
		Long: "Show all effective GC scopes and local barriers, with " +
			"cluster-wide global barriers once at the top level. Use " +
			"gc-state global to inspect only cluster-wide state.",
		Example: "  pd-ctl gc-state all",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			includeExpired, err := getGCStateIncludeExpired(cmd)
			if err != nil {
				return err
			}
			reader, err := factory(cmd)
			if err != nil {
				return errors.Annotate(err, "failed to create PD RPC client")
			}
			defer reader.close()

			clusterState, err := reader.getAllKeyspacesGCStates(cmd.Context())
			if err != nil {
				if status.Code(err) == codes.Unimplemented {
					return errors.Annotate(err,
						"gc-state all requires a PD server that supports "+
							"GetAllKeyspacesGCStates")
				}
				return errors.Annotate(err, "failed to get all keyspaces GC states")
			}
			output, err := newAllGCStatesOutput(clusterState, includeExpired)
			if err != nil {
				return err
			}
			return writeGCStateJSON(cmd, output)
		},
	}
}

func writeGCStateJSON(cmd *cobra.Command, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return errors.Annotate(err, "failed to marshal GC state JSON")
	}
	data = append(data, '\n')
	if _, err := cmd.OutOrStdout().Write(data); err != nil {
		return errors.Annotate(err, "failed to write GC state JSON")
	}
	return nil
}
