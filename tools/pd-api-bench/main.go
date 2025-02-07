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
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tools/pd-api-bench/cases"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr    = flag.String("pd", "127.0.0.1:2379", "pd address")
	debugFlag = flag.Bool("debug", false, "print the output of api response for debug")

	httpCases = flag.String("http-cases", "", "http api cases")
	gRPCCases = flag.String("grpc-cases", "", "grpc cases")

	client = flag.Int("client", 1, "client number")

	qps   = flag.Int64("qps", 1000, "qps")
	burst = flag.Int64("burst", 1, "burst")

	// http params
	httpParams = flag.String("params", "", "http params")

	// tls
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

var base = int64(time.Second) / int64(time.Microsecond)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

<<<<<<< HEAD
=======
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}
	err = logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, logutil.RedactInfoLogOFF)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}
>>>>>>> 0c43ce53a (*: fix default log file size (#9038))
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

	addr := trimHTTPPrefix(*pdAddr)
	protocol := "http"
	if len(*caPath) != 0 {
		protocol = "https"
	}
	cases.PDAddress = fmt.Sprintf("%s://%s", protocol, addr)
	cases.Debug = *debugFlag

	hcases := make([]cases.HTTPCase, 0)
	gcases := make([]cases.GRPCCase, 0)

	var err error
	hcaseStr := strings.Split(*httpCases, ",")
	for _, str := range hcaseStr {
		caseQPS := int64(0)
		caseBurst := int64(0)
		cStr := ""

		strs := strings.Split(str, "-")
		// to get case name
		strsa := strings.Split(strs[0], "+")
		cStr = strsa[0]
		// to get case Burst
		if len(strsa) > 1 {
			caseBurst, err = strconv.ParseInt(strsa[1], 10, 64)
			if err != nil {
				log.Printf("parse burst failed for case %s", str)
			}
		}
		// to get case qps
		if len(strs) > 1 {
			strsb := strings.Split(strs[1], "+")
			caseQPS, err = strconv.ParseInt(strsb[0], 10, 64)
			if err != nil {
				log.Printf("parse qps failed for case %s", str)
			}
			// to get case Burst
			if len(strsb) > 1 {
				caseBurst, err = strconv.ParseInt(strsb[1], 10, 64)
				if err != nil {
					log.Printf("parse burst failed for case %s", str)
				}
			}
		}
		if len(cStr) == 0 {
			continue
		}
		if cas, ok := cases.HTTPCaseMap[cStr]; ok {
			hcases = append(hcases, cas)
			if caseBurst > 0 {
				cas.SetBurst(caseBurst)
			} else if *burst > 0 {
				cas.SetBurst(*burst)
			}
			if caseQPS > 0 {
				cas.SetQPS(caseQPS)
			} else if *qps > 0 {
				cas.SetQPS(*qps)
			}
		} else {
			log.Printf("no this case: '%s'", str)
		}
	}
	gcaseStr := strings.Split(*gRPCCases, ",")
	for _, str := range gcaseStr {
		if len(str) == 0 {
			continue
		}
		if cas, ok := cases.GRPCCaseMap[str]; ok {
			gcases = append(gcases, cas)
		} else {
			log.Println("no this case", str)
		}
	}
	if *client == 0 {
		log.Println("concurrency == 0, exit")
		return
	}
	pdClis := make([]pd.Client, *client)
	for i := 0; i < *client; i++ {
		pdClis[i] = newPDClient(ctx)
	}
	httpClis := make([]*http.Client, *client)
	for i := 0; i < *client; i++ {
		httpClis[i] = newHTTPClient()
	}
	err = cases.InitCluster(ctx, pdClis[0], httpClis[0])
	if err != nil {
		log.Fatalf("InitCluster error %v", err)
	}

	for _, hcase := range hcases {
		handleHTTPCase(ctx, hcase, httpClis)
	}
	for _, gcase := range gcases {
		handleGRPCCase(ctx, gcase, pdClis)
	}

	<-ctx.Done()
	for _, cli := range pdClis {
		cli.Close()
	}
	log.Println("Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func handleGRPCCase(ctx context.Context, gcase cases.GRPCCase, clients []pd.Client) {
	qps := gcase.GetQPS()
	burst := gcase.GetBurst()
	tt := time.Duration(base/qps*burst*int64(*client)) * time.Microsecond
	log.Printf("begin to run gRPC case %s, with qps = %d and burst = %d, interval is %v", gcase.Name(), qps, burst, tt)
	for _, cli := range clients {
		go func(cli pd.Client) {
			var ticker = time.NewTicker(tt)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := int64(0); i < burst; i++ {
						err := gcase.Unary(ctx, cli)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleGetRegion")
					return
				}
			}
		}(cli)
	}
}

func handleHTTPCase(ctx context.Context, hcase cases.HTTPCase, httpClis []*http.Client) {
	qps := hcase.GetQPS()
	burst := hcase.GetBurst()
	tt := time.Duration(base/qps*burst*int64(*client)) * time.Microsecond
	log.Printf("begin to run http case %s, with qps = %d and burst = %d, interval is %v", hcase.Name(), qps, burst, tt)
	if *httpParams != "" {
		hcase.Params(*httpParams)
	}
	for _, hCli := range httpClis {
		go func(hCli *http.Client) {
			var ticker = time.NewTicker(tt)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for i := int64(0); i < burst; i++ {
						err := hcase.Do(ctx, hCli)
						if err != nil {
							log.Println(err)
						}
					}
				case <-ctx.Done():
					log.Println("Got signal to exit handleScanRegions")
					return
				}
			}
		}(hCli)
	}
}

func exit(code int) {
	os.Exit(code)
}

// newHTTPClient returns an HTTP(s) client.
func newHTTPClient() *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	tlsConf := loadTLSConfig()
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

// newPDClient returns a pd client.
func newPDClient(ctx context.Context) pd.Client {
	const (
		keepaliveTime    = 10 * time.Second
		keepaliveTimeout = 3 * time.Second
	)

	addrs := []string{trimHTTPPrefix(*pdAddr)}
	pdCli, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{
		CAPath:   *caPath,
		CertPath: *certPath,
		KeyPath:  *keyPath,
	},
		pd.WithGRPCDialOptions(
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

func loadTLSConfig() *tls.Config {
	if len(*caPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(*caPath)
	if err != nil {
		log.Println("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(*certPath)
	if err != nil {
		log.Println("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(*keyPath)
	if err != nil {
		log.Println("fail to read key file", zap.Error(err))
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
