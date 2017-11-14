package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/pd/pkg/faketikv"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"

	// Register schedulers.
	_ "github.com/pingcap/pd/server/schedulers"
	// Register namespace classifiers.
	_ "github.com/pingcap/pd/table"
)

func main() {
	_, local, clean := NewSingleServer()
	err := local.Run()
	if err != nil {
		log.Fatal("run server error:", err)
	}
	addr := local.GetAddr()
	driver := faketikv.NewDriver(addr)
	err = driver.Prepare()
	if err != nil {
		log.Fatal("simulator prepare error:", err)
	}
	tick := time.NewTicker(100 * time.Millisecond)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	for {
		select {
		case <-tick.C:
			driver.Tick()
		case <-sc:
			driver.Stop()
			clean()
			return
		}
	}
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer() (*server.Config, *server.Server, server.CleanupFunc) {
	cfg := server.NewTestSingleConfig()
	s, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		panic("create server failed")
	}

	cleanup := func() {
		s.Close()
		cleanServer(cfg)
	}
	return cfg, s, cleanup
}

func cleanServer(cfg *server.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}
