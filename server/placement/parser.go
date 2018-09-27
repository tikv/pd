// Copyright 2018 PingCAP, Inc.
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

package placement

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// ParseConfig parses a user configuration string.
func ParseConfig(str string) (*Config, error) {
	var config Config
	for _, sub := range strings.Split(str, ";") {
		if len(sub) > 0 {
			c, err := parseConstraint(sub)
			if err != nil {
				return nil, err
			}
			config.Constraints = append(config.Constraints, c)
		}
	}
	return &config, nil
}

func parseConstraint(str string) (*Constraint, error) {
	re := regexp.MustCompile(`^(.+)\((.*)\)\s*([<>=]+)\s*([\+\-0-9]+)\s*$`)
	matches := re.FindStringSubmatch(str)
	if len(matches) != 5 {
		return nil, errors.Errorf("bad format constraint '%s'", str)
	}
	function, err := parserFunctionName(matches[1])
	if err != nil {
		return nil, err
	}
	filters, labels, err := parseArguments(matches[2])
	if err != nil {
		return nil, err
	}
	op, err := parseOp(matches[3])
	if err != nil {
		return nil, err
	}
	value, err := parseValue(matches[4])
	if err != nil {
		return nil, err
	}
	return &Constraint{
		Function: function,
		Filters:  filters,
		Labels:   labels,
		Op:       op,
		Value:    value,
	}, nil
}

func parserFunctionName(str string) (string, error) {
	str = strings.TrimSpace(str)
	for _, f := range functionList {
		if str == f {
			return str, nil
		}
	}
	return "", errors.Errorf("unexpected function name '%s'", str)
}

func parseArguments(str string) (filters []Filter, labels []string, err error) {
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return nil, nil, nil
	}
	for _, sub := range strings.Split(str, ",") {
		if idx := strings.Index(sub, ":"); idx != -1 {
			key, err := parseArgument(sub[:idx])
			if err != nil {
				return nil, nil, err
			}
			value, err := parseArgument(sub[idx+1:])
			if err != nil {
				return nil, nil, err
			}
			filters = append(filters, Filter{Key: key, Value: value})
		} else {
			label, err := parseArgument(sub)
			if err != nil {
				return nil, nil, err
			}
			labels = append(labels, label)
		}
	}
	return
}

func parseArgument(str string) (string, error) {
	str = strings.TrimSpace(str)
	if ok, _ := regexp.MatchString(`^[a-zA-Z0-9_\-.]+$`, str); !ok {
		return "", errors.Errorf("invalid argument '%s'", str)
	}
	return str, nil
}

func parseOp(str string) (string, error) {
	str = strings.TrimSpace(str)
	switch str {
	case "<", "<=", "=", ">=", ">":
		return str, nil
	}
	return "", errors.Errorf(`expect one of "<", "<=", "=", ">=", ">", but got '%s'`, str)
}

func parseValue(str string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(str))
	if err != nil {
		return 0, errors.Errorf("expected integer, but got '%s'", str)
	}
	return n, nil
}
