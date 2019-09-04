package main

import (
	"flag"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/tools/pd-analysis/analysis"
	"go.uber.org/zap"
	"os"
)

var (
	input    = flag.String("input", "", "input pd log file, required.")
	output   = flag.String("output", "", "output file, default output to stdout.")
	logLevel = flag.String("logLevel", "info", "log level, default info.")
	style    = flag.String("style", "", "analysis style, example: transfer-region-counter")
)

// Logger is the global logger used for simulator.
var Logger *zap.Logger

// InitLogger initializes the Logger with -log level.
func InitLogger(l string) {
	conf := &log.Config{Level: l, File: log.FileLogConfig{}}
	lg, _, _ := log.InitLogger(conf)
	Logger = lg
}

func main() {
	flag.Parse()
	InitLogger(*logLevel)
	analysis.TransferRegionCounter.Init(0, 0)
	if *input == "" {
		Logger.Fatal("need to specify one input pd log")
	}
	if *output != "" {
		f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0755)
		if err != nil {
			Logger.Fatal(err.Error())
		} else {
			os.Stdout = f
		}
	}
	switch *style {
	case "transfer-region-counter":
		{
			analysis.TransferRegionCounter.ParseLog(*input)
			analysis.TransferRegionCounter.PrintResult()
			break
		}
	default:
		Logger.Fatal("Style is not exist.")
	}

}
