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
	"errors"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/tools/pd-ctl/pdctl/command/regionmeta"
)

// NewRegionMetaConsistencyCommand returns the region meta consistency command.
func NewRegionMetaConsistencyCommand() *cobra.Command {
	defaults := regionmeta.DefaultConfig()
	var (
		batchSize           int
		interval            time.Duration
		timeout             time.Duration
		maxRuntime          time.Duration
		retries             int
		scanRetries         int
		confirmLimit        int
		workDir             string
		maxTemporaryDiskMiB int64
		maxOutputMiB        int64
		output              string
		authorizationFile   string
	)
	command := &cobra.Command{
		Use:   "meta-consistency",
		Short: "compare region meta across all PD members",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.NoArgs(cmd, args); err != nil {
				cmd.Root().SilenceUsage = true
				return newCommandExitError(2, err, false)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Root().SilenceUsage = true
			if maxTemporaryDiskMiB <= 0 || maxOutputMiB <= 0 ||
				maxTemporaryDiskMiB > math.MaxInt64/(1024*1024) || maxOutputMiB > math.MaxInt64/(1024*1024) {
				return newCommandExitError(2, errors.New("temporary disk and output limits must be positive MiB values"), false)
			}
			pdAddress, err := cmd.Flags().GetString("pd")
			if err != nil {
				return newCommandExitError(2, err, false)
			}
			tlsConfig, err := parseTLSConfig(cmd)
			if err != nil {
				return newCommandExitError(2, err, false)
			}
			cfg := regionmeta.Config{
				Endpoints:             strings.Split(pdAddress, ","),
				TLSConfig:             tlsConfig,
				AuthorizationFile:     authorizationFile,
				BatchSize:             batchSize,
				Interval:              interval,
				Timeout:               timeout,
				MaxRuntime:            maxRuntime,
				Retries:               retries,
				ScanRetries:           scanRetries,
				ConfirmLimit:          confirmLimit,
				ConfirmationDelay:     time.Second,
				WorkDir:               workDir,
				MaxTemporaryDiskBytes: maxTemporaryDiskMiB * 1024 * 1024,
				MaxOutputBytes:        maxOutputMiB * 1024 * 1024,
				Output:                output,
			}
			outcome, err := regionmeta.Run(cmd.Context(), cfg, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return newCommandExitError(2, err, false)
			}
			switch outcome.Status {
			case regionmeta.StatusConsistent:
				return nil
			case regionmeta.StatusInconsistent:
				return newCommandExitError(1, nil, true)
			default:
				return newCommandExitError(2, nil, true)
			}
		},
	}
	command.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Root().SilenceUsage = true
		return newCommandExitError(2, err, false)
	})
	flags := command.Flags()
	flags.IntVar(&batchSize, "batch-size", defaults.BatchSize, "maximum Regions per HTTP request")
	flags.DurationVar(&interval, "interval", defaults.Interval, "global interval per HTTP request")
	flags.DurationVar(&timeout, "timeout", defaults.Timeout, "timeout per HTTP request")
	flags.DurationVar(&maxRuntime, "max-runtime", defaults.MaxRuntime, "wall-clock limit for the complete check")
	flags.IntVar(&retries, "retries", defaults.Retries, "additional retries per HTTP request")
	flags.IntVar(&scanRetries, "scan-retries", defaults.ScanRetries, "whole-cluster retries after an unstable scan")
	flags.IntVar(&confirmLimit, "confirm-limit", defaults.ConfirmLimit, "maximum differing Regions to recheck")
	flags.StringVar(&workDir, "work-dir", defaults.WorkDir, "directory for temporary files")
	flags.Int64Var(&maxTemporaryDiskMiB, "max-temporary-disk-mib", defaults.MaxTemporaryDiskBytes/(1024*1024), "temporary JSON hard limit in MiB")
	flags.Int64Var(&maxOutputMiB, "max-output-mib", defaults.MaxOutputBytes/(1024*1024), "JSON report hard limit in MiB")
	flags.StringVar(&output, "output", defaults.Output, "JSON report path, or '-' for stdout")
	flags.StringVar(&authorizationFile, "authorization-file", "", "file containing one complete Authorization header value")
	return command
}
