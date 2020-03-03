// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pdctl

import (
	"fmt"
	"github.com/pingcap/pd/v4/server"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/pd/v4/tools/pd-ctl/pdctl/command"
	"github.com/spf13/cobra"
)

// CommandFlags are flags that used in all Commands
type CommandFlags struct {
	URL      string
	CAPath   string
	CertPath string
	KeyPath  string
	Help     bool
}

var (
	commandFlags  = CommandFlags{}
	Detach       bool
	interact     bool
	version      bool
)

func init() {
	cobra.EnablePrefixMatching = true
}

func pdctlRun(cmd *cobra.Command, args []string) {
	if version {
		server.PrintPDInfo()
		os.Exit(0)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()

	if interact {
		loop()
	}
}

// Start run Command
func Start(args []string) {
	rootCmd := &cobra.Command{
		Use:   "pd-ctl",
		Short: "Placement Driver control",
		Run:   pdctlRun,
	}
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", "http://127.0.0.1:2379", "pd address")
	rootCmd.Flags().StringVar(&commandFlags.CAPath, "cacert", "", "path of file that contains list of trusted SSL CAs.")
	rootCmd.Flags().StringVar(&commandFlags.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format.")
	rootCmd.Flags().StringVar(&commandFlags.KeyPath, "key", "", "path of file that contains X509 key in PEM format.")
	rootCmd.PersistentFlags().BoolVarP(&commandFlags.Help, "help", "h", false, "Help message.")

	rootCmd.Flags().BoolVarP(&Detach, "detach", "d", true, "Run pdctl without readline.")
	rootCmd.Flags().BoolVarP(&interact, "interact", "i", false, "Run pdctl with readline.")
	rootCmd.Flags().BoolVarP(&version, "version", "V", false, "Print version information and exit.")

	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewRegionCommand(),
		command.NewStoreCommand(),
		command.NewStoresCommand(),
		command.NewMemberCommand(),
		command.NewExitCommand(),
		command.NewLabelCommand(),
		command.NewPingCommand(),
		command.NewOperatorCommand(),
		command.NewSchedulerCommand(),
		command.NewTSOCommand(),
		command.NewHotSpotCommand(),
		command.NewClusterCommand(),
		command.NewHealthCommand(),
		command.NewLogCommand(),
		command.NewPluginCommand(),
		command.NewComponentConfigCommand(),
	)

	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	rootCmd.ParseFlags(args)
	rootCmd.SetOutput(os.Stdout)

	if len(commandFlags.CAPath) != 0 {
		if err := command.InitHTTPSClient(commandFlags.CAPath, commandFlags.CertPath, commandFlags.KeyPath); err != nil {
			rootCmd.Println(err)
			return
		}
	}

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
	}
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args, err := shellwords.Parse(line)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}
		args = append(args, "-u", commandFlags.URL)
		if commandFlags.CAPath != "" && commandFlags.CertPath != "" && commandFlags.KeyPath != "" {
			args = append(args, "--cacert", commandFlags.CAPath, "--cert", commandFlags.CertPath, "--key", commandFlags.KeyPath)
		}
		Start(args)
	}
}
