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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	tsoNamespaceGauge = "tso"
	groupLabelGauge    = "group"
)

var (
	// keyspaceGroupKeyspaceCountGauge records the keyspace list length of each keyspace group.
	keyspaceGroupKeyspaceCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: tsoNamespaceGauge,
			Subsystem: "keyspace_group",
			Name:      "keyspace_list_length",
			Help:      "The length of keyspace list in each TSO keyspace group.",
		}, []string{groupLabelGauge})
)

func init() {
	prometheus.MustRegister(keyspaceGroupKeyspaceCountGauge)
}

// SetKeyspaceGroupKeyspaceCountGauge sets the keyspace list length metric for the given keyspace group.
// It is used by PD API service when saveKeyspaceGroups is executed.
func SetKeyspaceGroupKeyspaceCountGauge(groupID uint32, length float64) {
	keyspaceGroupKeyspaceCountGauge.WithLabelValues(fmt.Sprintf("%d", groupID)).Set(length)
}
