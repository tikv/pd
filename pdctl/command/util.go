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

package command

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
)

var dailClient = &http.Client{}

func doRequest(cmd *cobra.Command, prefix string, method string) (string, error) {
	var res string
	if method == "" {
		method = "GET"
	}
	url := getAddressFromCmd(cmd, prefix)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return res, err
	}
	reps, err := dailClient.Do(req)
	if err != nil {
		return res, err
	}
	defer reps.Body.Close()
	if reps.StatusCode != http.StatusOK {
		return res, genResponseError(reps)
	}

	r, err := ioutil.ReadAll(reps.Body)
	if err != nil {
		return res, err
	}
	res = string(r)
	return res, nil
}

func doRequestWithData(cmd *cobra.Command, prefix string, method string, data interface{}) error {
	if method == "" {
		method = "GET"
	}
	url := getAddressFromCmd(cmd, prefix)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return err
	}
	reps, err := dailClient.Do(req)
	if err != nil {
		return err
	}
	defer reps.Body.Close()
	if reps.StatusCode != http.StatusOK {
		return genResponseError(reps)
	}

	return json.NewDecoder(reps.Body).Decode(data)
}

func getAddressFromCmd(cmd *cobra.Command, prefix string) string {
	p, err := cmd.Flags().GetString("pd")
	if err != nil {
		fmt.Println("Get pd address error,should set flag with '-u'")
		os.Exit(1)
	}

	u, err := url.Parse(p)
	if err != nil {
		fmt.Println("address is wrong format,should like 'http://127.0.0.1:2379'")
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	s := fmt.Sprintf("%s/%s", u, prefix)
	return s
}

func genResponseError(r *http.Response) error {
	res, _ := ioutil.ReadAll(r.Body)
	return errors.Errorf("[%d] %s", r.StatusCode, res)
}

func printResponseError(r *http.Response) {
	fmt.Printf("[%d]:", r.StatusCode)
	io.Copy(os.Stdout, r.Body)
}
