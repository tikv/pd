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

package member

import "github.com/prometheus/client_golang/prometheus"

var (
	// ServiceMemberGauge is used to record the leader/primary of services.
	ServiceMemberGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "service",
			Subsystem: "member",
			Name:      "role",
			Help:      "The leader/primary of services",
		}, []string{"service"})
)

func init() {
	prometheus.MustRegister(ServiceMemberGauge)
}
