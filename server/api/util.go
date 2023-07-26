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

package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pingcap/errors"
)

var (
	errNoImplement    = errors.New("no implement")
	errOptionNotExist = func(name string) error { return errors.Errorf("the option %s does not exist", name) }
)

func collectEscapeStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}
		for _, c := range collectors {
			c(value)
		}
		return nil
	}
	return errOptionNotExist(option)
}

func collectStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		for _, c := range collectors {
			c(v)
		}
		return nil
	}
	return errOptionNotExist(option)
}

func readJSON(client *http.Client, url string, data interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.WithStack(err)
	}

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("http get url %s return code %d", url, resp.StatusCode)
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func postJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	res, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(string(res))
	}
	for _, opt := range checkOpts {
		opt(res, resp.StatusCode)
	}
	return nil
}

func doDelete(client *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	res.Body.Close()
	return res, nil
}
