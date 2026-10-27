// Copyright © 2026 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
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

package ethereum

import (
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricListenerCheckpointBlock = "ff_eventlistener_checkpoint_block"
	metricListenerCatchup         = "ff_eventlistener_catchup"

	metricLabelEventStream  = "eventstream"
	metricLabelListener     = "listener"
	metricLabelListenerName = "listener_name"
)

var listenerMetricLabels = []string{metricLabelEventStream, metricLabelListener, metricLabelListenerName}

// listenerCollector reports the position of every listener on every started event stream. It reads the
// live listener state at scrape time, so listeners that are removed (or streams that are stopped) simply
// stop being reported - rather than leaving a stale series behind that would look like a stalled listener.
type listenerCollector struct {
	c              *ethConnector
	checkpointDesc *prometheus.Desc
	catchupDesc    *prometheus.Desc
}

type listenerMetricsSnapshot struct {
	labels   []string
	hwmBlock int64
	catchup  bool
}

// RegisterListenerMetrics registers the per-listener metrics against the supplied registry.
func (c *ethConnector) RegisterListenerMetrics(registry metric.MetricsRegistry) {
	registry.MustRegisterCollector(newListenerCollector(c))
}

func newListenerCollector(c *ethConnector) *listenerCollector {
	return &listenerCollector{
		c: c,
		checkpointDesc: prometheus.NewDesc(metricListenerCheckpointBlock,
			"The block the listener has scanned up to - the block number of its checkpoint, which trails the chain head by events.checkpointBlockGap when the listener is up to date",
			listenerMetricLabels, nil),
		catchupDesc: prometheus.NewDesc(metricListenerCatchup,
			"Whether the listener is in catchup mode (1) because it is more than events.catchupThreshold blocks behind the chain head, or not (0)",
			listenerMetricLabels, nil),
	}
}

func (lc *listenerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lc.checkpointDesc
	ch <- lc.catchupDesc
}

func (lc *listenerCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range lc.snapshot() {
		catchup := 0.0
		if s.catchup {
			catchup = 1
		}
		ch <- prometheus.MustNewConstMetric(lc.checkpointDesc, prometheus.GaugeValue, float64(s.hwmBlock), s.labels...)
		ch <- prometheus.MustNewConstMetric(lc.catchupDesc, prometheus.GaugeValue, catchup, s.labels...)
	}
}

func (lc *listenerCollector) snapshot() []*listenerMetricsSnapshot {
	lc.c.mux.Lock()
	streams := make([]*eventStream, 0, len(lc.c.eventStreams))
	for _, es := range lc.c.eventStreams {
		streams = append(streams, es)
	}
	lc.c.mux.Unlock()

	var snapshots []*listenerMetricsSnapshot
	for _, es := range streams {
		es.mux.Lock()
		listeners := make([]*listener, 0, len(es.listeners))
		for _, l := range es.listeners {
			listeners = append(listeners, l)
		}
		streamCatchup := es.catchup
		es.mux.Unlock()

		for _, l := range listeners {
			snapshots = append(snapshots, &listenerMetricsSnapshot{
				labels:   []string{es.id.String(), l.id.String(), l.config.name},
				hwmBlock: l.getHWMBlock(),
				catchup:  l.catchup || streamCatchup, // dirty read, consistent with getListenerHWM
			})
		}
	}
	return snapshots
}
