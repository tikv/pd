// Copyright 2019 TiKV Project Authors.
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

package config

import (
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/errs"
)

const (
	// Label key consists of alphanumeric characters, '-', '_', '.' or '/', and must start and end with an
	// alphanumeric character. If can also contain an extra '$' at the beginning.
	keyFormat = "^[$]?[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?$"
	// Value key can be any combination of alphanumeric characters, '-', '_', '.' or '/'. It can also be empty to
	// mark the label as deleted.
	valueFormat = "^[-A-Za-z0-9_./]*$"
)

func validateFormat(s, format string) error {
	isValid, _ := regexp.MatchString(format, s)
	if !isValid {
		return errors.Errorf("%s does not match format %q", s, format)
	}
	return nil
}

// ValidateLabels checks the legality of the labels.
func ValidateLabels(labels []*metapb.StoreLabel) error {
	for _, label := range labels {
		if err := validateFormat(label.Key, keyFormat); err != nil {
			return err
		}
		if err := validateFormat(label.Value, valueFormat); err != nil {
			return err
		}
	}
	return nil
}

// ValidateLabelKey checks the legality of the label key.
func ValidateLabelKey(key string) error {
	return validateFormat(key, keyFormat)
}

// ValidateURLWithScheme checks the format of the URL.
func ValidateURLWithScheme(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return errors.Errorf("%s has no scheme", rawURL)
	}
	return nil
}

var schedulerMap sync.Map

// RegisterScheduler registers the scheduler type.
func RegisterScheduler(typ string) {
	schedulerMap.Store(typ, struct{}{})
}

// IsSchedulerRegistered checks if the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap.Load(name)
	return ok
}

// NewTestOptions creates default options for testing.
func NewTestOptions() *PersistOptions {
	// register default schedulers in case config check fail.
	for _, d := range DefaultSchedulers {
		RegisterScheduler(d.Type)
	}
	c := NewConfig()
	c.Adjust(nil, false)
	return NewPersistOptions(c)
}

// parseUrls parse a string into multiple urls.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errs.ErrURLParse.Wrap(err).GenWithStackByCause()
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

func parseMode(modes string) ([]ServiceMode, error) {
	items := strings.Split(modes, ",")
	rets := make([]ServiceMode, 0)
	for _, item := range items {
		ret, err := parseServiceMode(item)
		if err != nil {
			return nil, err
		}
		for _, r := range rets {
			if r == ret {
				return nil, errors.Errorf("duplicate service mode %s", item)
			}
		}
		rets = append(rets, ret)
	}
	return rets, nil
}

func parseServiceMode(mode string) (ServiceMode, error) {
	switch mode {
	case TSOService.String():
		return TSOService, nil
	case APIService.String():
		return APIService, nil
	case ResourceManagerService.String():
		return ResourceManagerService, nil
	case SchedulerService.String():
		return SchedulerService, nil
	default:
		return ServiceModeCount, errors.Errorf("invalid service mode %s", mode)
	}
}

func allModes() string {
	var modes []string
	for i := 0; i < int(ServiceModeCount); i++ {
		modes = append(modes, ServiceMode(i).String())
	}
	return strings.Join(modes, ",")
}
