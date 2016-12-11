package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	configPrefix   = "pd/api/v1/config"
	schedulePrefix = "pd/api/v1/config/schedule"
)

func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	conf.AddCommand(NewSetConfigCommand())
	return conf
}

func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show",
		Short: "show config fo PD",
		Run:   configShowCommandF,
	}
	return sc
}

func NewSetConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set <option> <value>",
		Short: "set the option with value",
		Run:   configSetCommandF,
	}
	return sc
}

func configShowCommandF(cmd *cobra.Command, args []string) {
	url := getAddressFromCmd(cmd, schedulePrefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get config:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		fmt.Println(r.StatusCode, "error")
		return
	}

	io.Copy(os.Stdout, r.Body)

}

func configSetCommandF(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	url := getAddressFromCmd(cmd, schedulePrefix)
	var value interface{}
	data := make(map[string]interface{})

	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to set config:[%s]\n", err)
		return
	}
	if r.StatusCode != http.StatusOK {
		fmt.Println(r.StatusCode, "error")
		return
	}

	json.NewDecoder(r.Body).Decode(&data)
	r.Body.Close()
	value, err = strconv.ParseFloat(args[1], 64)
	if err != nil {
		value = args[1]
	}
	data[args[0]] = value

	req, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Failed to set config:[%s]\n", err)
		return
	}

	url = getAddressFromCmd(cmd, configPrefix)
	r, err = http.Post(url, "application/json", bytes.NewBuffer(req))
	if err != nil {
		fmt.Printf("Failed to set config:[%s]\n", err)
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusOK {
		fmt.Println("Success!")
	} else {
		fmt.Println("Failed:")
		io.Copy(os.Stdout, r.Body)
	}
}
