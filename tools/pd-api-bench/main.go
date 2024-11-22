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

package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	flag "github.com/spf13/pflag"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/caller"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/utils/tlsutil"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-api-bench/cases"
	"github.com/tikv/pd/tools/pd-api-bench/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	qps, burst           int64
	httpCases, gRPCCases string
)

var (
	pdAPIExecutionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "api_bench",
			Name:      "pd_api_execution_duration_seconds",
			Help:      "Bucketed histogram of all pd api execution time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{"type"})

	pdAPIRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "api_bench",
			Name:      "pd_api_request_total",
			Help:      "Counter of the pd http api requests",
		}, []string{"type", "result"})
)

func main() {
	prometheus.MustRegister(pdAPIExecutionHistogram)
	prometheus.MustRegister(pdAPIRequestCounter)

	ctx, cancel := context.WithCancel(context.Background())
	flagSet := flag.NewFlagSet("api-bench", flag.ContinueOnError)
	flagSet.ParseErrorsWhitelist.UnknownFlags = true
	flagSet.Int64Var(&qps, "qps", 1, "qps")
	flagSet.Int64Var(&burst, "burst", 1, "burst")
	flagSet.StringVar(&httpCases, "http-cases", "", "http api cases")
	flagSet.StringVar(&gRPCCases, "grpc-cases", "", "grpc cases")
	cfg := config.NewConfig(flagSet)
	err := cfg.Parse(os.Args[1:])
	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, logutil.RedactInfoLogOFF)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
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

	if cfg.Client == 0 {
		log.Error("concurrency == 0, exit")
		return
	}
	pdClis := make([]pd.Client, cfg.Client)
	for i := range cfg.Client {
		pdClis[i] = newPDClient(ctx, cfg)
		pdClis[i].UpdateOption(opt.EnableFollowerHandle, true)
	}
	etcdClis := make([]*clientv3.Client, cfg.Client)
	for i := range cfg.Client {
		etcdClis[i] = newEtcdClient(cfg)
	}
	httpClis := make([]pdHttp.Client, cfg.Client)
	for i := range cfg.Client {
		sd := pdClis[i].GetServiceDiscovery()
		httpClis[i] = pdHttp.NewClientWithServiceDiscovery("tools-api-bench", sd, pdHttp.WithTLSConfig(loadTLSConfig(cfg)), pdHttp.WithMetrics(pdAPIRequestCounter, pdAPIExecutionHistogram))
	}
	err = cases.InitCluster(ctx, pdClis[0], httpClis[0])
	if err != nil {
		log.Fatal("InitCluster error", zap.Error(err))
	}

	coordinator := cases.NewCoordinator(ctx, httpClis, pdClis, etcdClis)

	hcaseStr := strings.Split(httpCases, ",")
	for _, str := range hcaseStr {
		name, cfg := parseCaseNameAndConfig(str)
		if len(name) == 0 {
			continue
		}
		coordinator.SetHTTPCase(name, cfg)
	}
	gcaseStr := strings.Split(gRPCCases, ",")
	for _, str := range gcaseStr {
		name, cfg := parseCaseNameAndConfig(str)
		if len(name) == 0 {
			continue
		}
		coordinator.SetGRPCCase(name, cfg)
	}
	cfg.InitCoordinator(coordinator)

	go runHTTPServer(cfg, coordinator)

	<-ctx.Done()
	for _, cli := range pdClis {
		cli.Close()
	}
	for _, cli := range httpClis {
		cli.Close()
	}
	for _, cli := range etcdClis {
		cli.Close()
	}
	log.Info("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	os.Exit(code)
}

func parseCaseNameAndConfig(str string) (string, *cases.Config) {
	var err error
	cfg := &cases.Config{}
	name := ""
	strs := strings.Split(str, "-")
	// to get case name
	strsa := strings.Split(strs[0], "+")
	name = strsa[0]
	// to get case Burst
	if len(strsa) > 1 {
		cfg.Burst, err = strconv.ParseInt(strsa[1], 10, 64)
		if err != nil {
			log.Error("parse burst failed for case", zap.String("case", name), zap.String("config", strsa[1]))
		}
	}
	// to get case qps
	if len(strs) > 1 {
		strsb := strings.Split(strs[1], "+")
		cfg.QPS, err = strconv.ParseInt(strsb[0], 10, 64)
		if err != nil {
			log.Error("parse qps failed for case", zap.String("case", name), zap.String("config", strsb[0]))
		}
		// to get case Burst
		if len(strsb) > 1 {
			cfg.Burst, err = strconv.ParseInt(strsb[1], 10, 64)
			if err != nil {
				log.Error("parse burst failed for case", zap.String("case", name), zap.String("config", strsb[1]))
			}
		}
	}
	if cfg.QPS == 0 && qps > 0 {
		cfg.QPS = qps
	}
	if cfg.Burst == 0 && burst > 0 {
		cfg.Burst = burst
	}
	return name, cfg
}

func runHTTPServer(cfg *config.Config, co *cases.Coordinator) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(cors.Default())
	engine.Use(gzip.Gzip(gzip.DefaultCompression))
	engine.GET("metrics", utils.PromHandler())
	// profile API
	pprof.Register(engine)

	getCfg := func(c *gin.Context) *cases.Config {
		var err error
		cfg := &cases.Config{}
		qpsStr := c.Query("qps")
		if len(qpsStr) > 0 {
			cfg.QPS, err = strconv.ParseInt(qpsStr, 10, 64)
			if err != nil {
				c.String(http.StatusBadRequest, err.Error())
			}
		}
		burstStr := c.Query("burst")
		if len(burstStr) > 0 {
			cfg.Burst, err = strconv.ParseInt(burstStr, 10, 64)
			if err != nil {
				c.String(http.StatusBadRequest, err.Error())
			}
		}
		return cfg
	}

	engine.POST("config/http/all", func(c *gin.Context) {
		var input map[string]cases.Config
		if err := c.ShouldBindJSON(&input); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		for name, cfg := range input {
			co.SetHTTPCase(name, &cfg)
		}
		c.String(http.StatusOK, "")
	})
	engine.POST("config/http/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg := getCfg(c)
		co.SetHTTPCase(name, cfg)
		c.String(http.StatusOK, "")
	})
	engine.POST("config/grpc/all", func(c *gin.Context) {
		var input map[string]cases.Config
		if err := c.ShouldBindJSON(&input); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		for name, cfg := range input {
			co.SetGRPCCase(name, &cfg)
		}
		c.String(http.StatusOK, "")
	})
	engine.POST("config/grpc/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg := getCfg(c)
		co.SetGRPCCase(name, cfg)
		c.String(http.StatusOK, "")
	})
	engine.POST("config/etcd/all", func(c *gin.Context) {
		var input map[string]cases.Config
		if err := c.ShouldBindJSON(&input); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		for name, cfg := range input {
			co.SetEtcdCase(name, &cfg)
		}
		c.String(http.StatusOK, "")
	})
	engine.POST("config/etcd/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg := getCfg(c)
		co.SetEtcdCase(name, cfg)
		c.String(http.StatusOK, "")
	})

	engine.GET("config/http/all", func(c *gin.Context) {
		all := co.GetAllHTTPCases()
		c.IndentedJSON(http.StatusOK, all)
	})
	engine.GET("config/http/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg, err := co.GetHTTPCase(name)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, cfg)
	})
	engine.GET("config/grpc/all", func(c *gin.Context) {
		all := co.GetAllGRPCCases()
		c.IndentedJSON(http.StatusOK, all)
	})
	engine.GET("config/grpc/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg, err := co.GetGRPCCase(name)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, cfg)
	})
	engine.GET("config/etcd/all", func(c *gin.Context) {
		all := co.GetAllEtcdCases()
		c.IndentedJSON(http.StatusOK, all)
	})
	engine.GET("config/etcd/:name", func(c *gin.Context) {
		name := c.Param("name")
		cfg, err := co.GetEtcdCase(name)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, cfg)
	})
	engine.Run(cfg.StatusAddr)
}

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

func newEtcdClient(cfg *config.Config) *clientv3.Client {
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	tlsCfg, err := tlsutil.TLSConfig{
		CAPath:   cfg.CaPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("fail to create etcd client", zap.Error(err))
		return nil
	}
	clientConfig := clientv3.Config{
		Endpoints:   []string{cfg.PDAddr},
		DialTimeout: keepaliveTimeout,
		TLS:         tlsCfg,
		LogConfig:   &lgc,
	}
	client, err := clientv3.New(clientConfig)
	if err != nil {
		log.Fatal("fail to create pd client", zap.Error(err))
	}
	return client
}

// newPDClient returns a pd client.
func newPDClient(ctx context.Context, cfg *config.Config) pd.Client {
	addrs := []string{cfg.PDAddr}
	pdCli, err := pd.NewClientWithContext(ctx, caller.TestComponent,
		addrs, pd.SecurityOption{
			CAPath:   cfg.CaPath,
			CertPath: cfg.CertPath,
			KeyPath:  cfg.KeyPath,
		},
		opt.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    keepaliveTime,
				Timeout: keepaliveTimeout,
			}),
		))
	if err != nil {
		log.Fatal("fail to create pd client", zap.Error(err))
	}
	return pdCli
}

func loadTLSConfig(cfg *config.Config) *tls.Config {
	if len(cfg.CaPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(cfg.CaPath)
	if err != nil {
		log.Error("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(cfg.CertPath)
	if err != nil {
		log.Error("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(cfg.KeyPath)
	if err != nil {
		log.Error("fail to read key file", zap.Error(err))
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("failed to load tlc config", zap.Error(err))
	}

	return tlsConf
}
