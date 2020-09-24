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
// See the License for the specific language governing permissions and
// limitations under the License.

package pdctl

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
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
	commandFlags = CommandFlags{
		URL: "http://127.0.0.1:2379",
	}

	detach            bool
	interact          bool
	version           bool
	readlineCompleter *readline.PrefixCompleter
)

func init() {
	cobra.EnablePrefixMatching = true
}

func pdctlRun(cmd *cobra.Command, args []string) {
	if version {
		server.PrintPDInfo()
		return
	}
	if interact {
		loop()
	}
}

func getBasicCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pd-ctl",
		Short: "Placement Driver control",
	}

	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", commandFlags.URL, "address of pd")
	rootCmd.PersistentFlags().StringVar(&commandFlags.CAPath, "cacert", commandFlags.CAPath, "path of file that contains list of trusted SSL CAs")
	rootCmd.PersistentFlags().StringVar(&commandFlags.CertPath, "cert", commandFlags.CertPath, "path of file that contains X509 certificate in PEM format")
	rootCmd.PersistentFlags().StringVar(&commandFlags.KeyPath, "key", commandFlags.KeyPath, "path of file that contains X509 key in PEM format")
	rootCmd.PersistentFlags().BoolVarP(&commandFlags.Help, "help", "h", false, "help message")

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
		command.NewServiceGCSafepointCommand(),
		command.NewCompletionCommand(),
	)

	rootCmd.Flags().ParseErrorsWhitelist.UnknownFlags = true
	rootCmd.SilenceErrors = true

	return rootCmd
}

func getInteractCmd(args []string) *cobra.Command {
	rootCmd := getBasicCmd()

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOutput(os.Stdout)
	hiddenFlag(rootCmd)

	return rootCmd
}

func getMainCmd(args []string) *cobra.Command {
	rootCmd := getBasicCmd()

	rootCmd.Flags().BoolVarP(&detach, "detach", "d", true, "Run pdctl without readline.")
	rootCmd.Flags().BoolVarP(&interact, "interact", "i", false, "Run pdctl with readline.")
	rootCmd.Flags().BoolVarP(&version, "version", "V", false, "Print version information and exit.")
	rootCmd.Run = pdctlRun

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOutput(os.Stdout)

	readlineCompleter = readline.NewPrefixCompleter(genCompleter(rootCmd)...)
	return rootCmd
}

// Hide the flags in help and usage messages.
func hiddenFlag(cmd *cobra.Command) {
	cmd.LocalFlags().MarkHidden("pd")
	cmd.LocalFlags().MarkHidden("cacert")
	cmd.LocalFlags().MarkHidden("cert")
	cmd.LocalFlags().MarkHidden("key")
}

// MainStart start main command
func MainStart(args []string) {
	startCmd(getMainCmd, args)
}

// Start start interact command
func Start(args []string) {
	startCmd(getInteractCmd, args)
}

func startCmd(getCmd func([]string) *cobra.Command, args []string) {
	rootCmd := getCmd(args)
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
		AutoComplete:      readlineCompleter,
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
		Start(args)
	}
}

func genCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}

	for _, v := range cmd.Commands() {
		if v.HasFlags() {
			flagsPc := []readline.PrefixCompleterInterface{}
			flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\n")
			for i := 0; i < len(flagUsages)-1; i++ {
				flagsPc = append(flagsPc, readline.PcItem(strings.Split(strings.Trim(flagUsages[i], " "), " ")[0]))
			}
			flagsPc = append(flagsPc, genCompleter(v)...)
			pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagsPc...))
		} else {
			pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], genCompleter(v)...))
		}
	}
	return pc
}
