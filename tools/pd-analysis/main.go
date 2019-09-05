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
	operator = flag.String("operator", "", "operator style, example: balance-region, balance-leader, transfer-hot-read-leader, move-hot-read-region, transfer-hot-write-leader, move-hot-write-region")
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
	analysis.GetTransferRegionCounter().Init(0, 0)
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
			if *operator == "" {
				Logger.Fatal("need to specify one operator")
			}
			r := analysis.GetTransferRegionCounter().CompileRegex(*operator)
			analysis.GetTransferRegionCounter().ParseLog(*input, r)
			analysis.GetTransferRegionCounter().PrintResult()
			break
		}
	default:
		Logger.Fatal("Style is not exist.")
	}

}
