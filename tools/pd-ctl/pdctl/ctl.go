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

package pdctl

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
	shellwords "github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
)

func init() {
	cobra.EnablePrefixMatching = false
	cobra.EnableTraverseRunHooks = true
}

// GetRootCmd is exposed for integration tests. But it can be embedded into another suite, too.
func GetRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "pd-ctl",
		Short:             "Placement Driver control",
		PersistentPreRunE: command.RequireHTTPSClient,
		SilenceErrors:     true,
	}

	rootCmd.PersistentFlags().StringP("pd", "u", "http://127.0.0.1:2379", "address of PD")
	rootCmd.PersistentFlags().String("cacert", "", "path of file that contains list of trusted SSL CAs")
	rootCmd.PersistentFlags().String("cert", "", "path of file that contains X509 certificate in PEM format")
	rootCmd.PersistentFlags().String("key", "", "path of file that contains X509 key in PEM format")

	rootCmd.Flags().ParseErrorsWhitelist.UnknownFlags = true

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
		command.NewMinResolvedTSCommand(),
		command.NewCompletionCommand(),
		command.NewUnsafeCommand(),
		command.NewKeyspaceGroupCommand(),
		command.NewKeyspaceCommand(),
		command.NewResourceManagerCommand(),
	)

	return rootCmd
}

// MainStart start main command
func MainStart(args []string) {
	rootCmd := GetRootCmd()

	rootCmd.Flags().BoolP("interact", "i", false, "Run pdctl with readline.")
	rootCmd.Flags().BoolP("version", "V", false, "Print version information and exit.")
	// TODO: deprecated
	rootCmd.Flags().BoolP("detach", "d", true, "Run pdctl without readline.")

	rootCmd.Run = func(cmd *cobra.Command, _ []string) {
		if v, err := cmd.Flags().GetBool("version"); err == nil && v {
			versioninfo.Print()
			return
		}
		if v, err := cmd.Flags().GetBool("interact"); err == nil && v {
			readlineCompleter := readline.NewPrefixCompleter(genCompleter(cmd)...)
			loop(cmd.PersistentFlags(), readlineCompleter)
		}
	}

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
		os.Exit(1)
	}
}

func loop(persistentFlags *pflag.FlagSet, readlineCompleter readline.AutoCompleter) {
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

	getREPLCmd := func() *cobra.Command {
		rootCmd := GetRootCmd()
		persistentFlags.VisitAll(func(flag *pflag.Flag) {
			if flag.Changed {
				rootCmd.PersistentFlags().Set(flag.Name, flag.Value.String())
			}
		})
		rootCmd.LocalFlags().MarkHidden("pd")
		rootCmd.LocalFlags().MarkHidden("cacert")
		rootCmd.LocalFlags().MarkHidden("cert")
		rootCmd.LocalFlags().MarkHidden("key")
		rootCmd.SetOut(os.Stdout)
		return rootCmd
	}

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

		rootCmd := getREPLCmd()
		rootCmd.SetArgs(args)
		rootCmd.ParseFlags(args)
		if err := rootCmd.Execute(); err != nil {
			rootCmd.Println(err)
		}
	}
}

func genCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}

	for _, v := range cmd.Commands() {
		if v.HasFlags() {
			flagsPc := []readline.PrefixCompleterInterface{}
			flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\n")
			for i := range len(flagUsages) - 1 {
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

// ReadStdin convert stdin to string array
func ReadStdin(r io.Reader) (input []string, err error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if s := strings.TrimSpace(string(b)); len(s) > 0 {
		input = strings.Split(s, " ")
	}
	return input, nil
}
