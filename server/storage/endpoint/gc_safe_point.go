// Copyright 2022 TiKV Project Authors.
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

package endpoint

import (
	"time"
)

// ServiceSafePoint is the safepoint for a specific service
type ServiceSafePoint struct {
	ServiceID string `json:"service_id"`
	ExpiredAt int64  `json:"expired_at"`
	SafePoint uint64 `json:"safe_point"`
}

// GCSafePointStorage defines the storage operations on the GC safe point.
type GCSafePointStorage interface {
	LoadGCSafePoint() (uint64, error)
	SaveGCSafePoint(safePoint uint64) error
	LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error)
	LoadAllServiceGCSafePoints() ([]*ServiceSafePoint, error)
	SaveServiceGCSafePoint(ssp *ServiceSafePoint) error
	RemoveServiceGCSafePoint(serviceID string) error
}
