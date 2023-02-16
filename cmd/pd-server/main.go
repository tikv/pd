// Copyright 2016 TiKV Project Authors.
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
	"flag"
	"os"
	"os/signal"
	"syscall"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/apiv2"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
	"github.com/tikv/pd/server/schedulers"
	"go.uber.org/zap"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "pd-server",
		Short: "Placement Driver server",
		Run:   createServerWrapper,
	}

	rootCmd.Flags().BoolP("version", "V", false, "print version information and exit")
	rootCmd.Flags().StringP("config", "", "", "config file")
	rootCmd.Flags().BoolP("config-check", "", false, "check config file validity and exit")
	rootCmd.Flags().StringP("name", "", "", "human-readable name for this pd member")
	rootCmd.Flags().StringP("data-dir", "", "", "path to the data directory (default 'default.${name}')")
	rootCmd.Flags().StringP("client-urls", "", "http://127.0.0.1:2379", "url for client traffic")
	rootCmd.Flags().StringP("advertise-client-urls", "", "", "advertise url for client traffic (default '${client-urls}')")
	rootCmd.Flags().StringP("peer-urls", "", "http://127.0.0.1:2380", "url for peer traffic")
	rootCmd.Flags().StringP("advertise-peer-urls", "", "", "advertise url for peer traffic (default '${peer-urls}')")
	rootCmd.Flags().StringP("initial-cluster", "", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	rootCmd.Flags().StringP("join", "", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")
	rootCmd.Flags().StringP("metrics-addr", "", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	rootCmd.Flags().StringP("log-level", "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	rootCmd.Flags().StringP("log-file", "", "", "log file path")
	rootCmd.Flags().StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	rootCmd.Flags().StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	rootCmd.Flags().StringP("key", "", "", "path of file that contains X509 key in PEM format")
	rootCmd.Flags().BoolP("force-new-cluster", "", false, "force to create a new one-member cluster")
	rootCmd.AddCommand(NewServiceCommand())

	rootCmd.SetOutput(os.Stdout)
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
		os.Exit(1)
	}
}

// NewServiceCommand returns the service command.
func NewServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service <tso>",
		Short: "Run a service",
	}
	cmd.AddCommand(NewTSOServiceCommand())
	return cmd
}

// NewTSOServiceCommand returns the unsafe remove failed stores command.
func NewTSOServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tso",
		Short: "Run the tso service",
		Run:   tso.CreateServerWrapper,
	}
	cmd.Flags().BoolP("version", "V", false, "print version information and exit")
	cmd.Flags().StringP("config", "", "", "config file")
	cmd.Flags().StringP("backend-endpoints", "", "http://127.0.0.1:2379", "url for etcd client")
	cmd.Flags().StringP("listen-addr", "", "", "listen address for tso service")
	cmd.Flags().StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	cmd.Flags().StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	cmd.Flags().StringP("key", "", "", "path of file that contains X509 key in PEM format")
	return cmd
}

func createServerWrapper(cmd *cobra.Command, args []string) {
	schedulers.Register()
	cfg := config.NewConfig()
	flagSet := cmd.Flags()
	flagSet.Parse(args)
	err := cfg.Parse(flagSet)
	if err != nil {
		cmd.Println(err)
		return
	}

	printVersion, err := flagSet.GetBool("version")
	if err != nil {
		cmd.Println(err)
		return
	}
	if printVersion {
		server.PrintPDInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", errs.ZapError(err))
	}

	configCheck, err := flagSet.GetBool("config-check")
	if err != nil {
		cmd.Println(err)
		return
	}

	if configCheck {
		server.PrintConfigCheckMsg(cfg)
		exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	err = join.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join meet error", errs.ZapError(err))
	}

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, apiv2.NewV2Handler, swaggerserver.NewHandler, autoscaling.NewHandler}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
	svr, err := server.CreateServer(ctx, cfg, serviceBuilders...)
	if err != nil {
		log.Fatal("create server failed", errs.ZapError(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
