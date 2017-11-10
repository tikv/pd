package main

import (
	"os/signal"
	"strings"
	"time"

	"github.com/pingcap/pd/pkg/faketikv"
	"github.com/pingcap/pd/server"
)

func main() {
	_, local, clean := server.NewSingleServer()
	addr := local.GetAddr()
	addrs := strings.Split(addr, ",")
	driver := faketikv.NewDriver(addrs)
	driver.Prepare()
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
			clean()
			return
		}
	}
}
