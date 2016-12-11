package command

import (
	"os"

	"github.com/spf13/cobra"
)

func NewExitCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "exit",
		Short: "exit pdctl",
		Run:   exitCommandF,
	}
	return conf
}

func exitCommandF(cmd *cobra.Command, args []string) {
	os.Exit(0)
}
