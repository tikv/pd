package pdctl

import (
	"github.com/pingcap/pd/pdctl/command"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "pdctl",
		Short: "Placement Driver control",
	}
	commandFlags = command.CommandFlags{}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd_url", "u", "http://127.0.0.1:2379", "pd address")
	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewExitCommand(),
	)
	cobra.EnablePrefixMatching = true
}

func Start(args []string) (error, string) {
	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	rootCmd.SetUsageTemplate(command.UsageTemplate)
	if err := rootCmd.Execute(); err != nil {
		return err, rootCmd.UsageString()
	}
	return nil, ""
}
