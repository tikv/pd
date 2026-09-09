// Copyright 2023 TiKV Project Authors.
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
	"regexp"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
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

// SyncStoreLimit applies the current configured rate to a v1 limiter and returns
// the store's limiter. StoreInfo clones share the limiter, which owns its locks.
// Configuration uses operations per minute; v1 limiters use operations per second.
// Unchanged rates neither reset the token budget nor allocate a new bucket.
func SyncStoreLimit(store *core.StoreInfo, conf SharedConfigProvider, typ storelimit.Type) storelimit.StoreLimit {
	limiter := store.GetStoreLimit()
	if limit, ok := limiter.(*storelimit.StoreRateLimit); ok {
		rate := conf.GetStoreLimitByType(store.GetID(), typ) / 60
		if limit.Rate(typ) != rate {
			limit.Reset(rate, typ)
		}
	}
	return limiter
}

// IsStoreLimitAvailable synchronizes a v1 limiter before checking its budget.
// Other limiter versions keep their own availability semantics.
func IsStoreLimitAvailable(store *core.StoreInfo, conf SharedConfigProvider, typ storelimit.Type, level constant.PriorityLevel) bool {
	limiter := store.GetStoreLimit()
	cost := storelimit.RegionInfluence[typ]
	if limit, ok := limiter.(*storelimit.StoreRateLimit); ok {
		return limit.AvailableWithRate(cost, typ, conf.GetStoreLimitByType(store.GetID(), typ)/60)
	}
	return limiter.Available(cost, typ, level)
}
