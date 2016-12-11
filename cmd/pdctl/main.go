package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bobappleyard/readline"
	"github.com/pingcap/pd/pdctl"
)

var url string

func init() {
	flag.StringVar(&url, "u", "http://127.0.0.1:2379", "the pd address")
}
func main() {
	flag.Parse()
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, syscall.SIGINT)

	go func() {
		select {
		case <-sigint:
			fmt.Println("\nExited")
			readline.Cleanup()
			os.Exit(1)
		}
	}()

	loop()
}

func loop() {
	for {
		line, err := readline.String("> ")
		if err != nil {
			continue
		}

		if line == "exit" {
			os.Exit(0)
		}
		readline.AddHistory(line)
		args := strings.Split(line, " ")
		args = append(args, "-u", url)
		err, usage := pdctl.Start(args)
		if err != nil {
			fmt.Println(usage)
		}
	}
}
