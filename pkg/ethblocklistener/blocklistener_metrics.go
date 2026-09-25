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

package ethblocklistener

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
)

const (
	metricsSubsystem = "blocklistener"

	// metricTargetBlockHeight is the block height the endpoint we are connected to reports, via eth_blockNumber
	// or (in client filter polling mode) the latest block poll - always the value we last received from the node.
	metricTargetBlockHeight = "target_block_height"
	// metricCanonicalBlockHeight is the head of the chain this listener is tracking - in full chain tracking
	// mode the head of the in-memory canonical chain built from the block filter / latest block poll,
	// and in light mode the head we dispatch to consumers. It should track the target height very closely.
	metricCanonicalBlockHeight = "canonical_block_height"
	// metricPollFailures counts the JSON/RPC polls the listen loop makes that failed, labelled by method.
	metricPollFailures      = "poll_failures_total"
	metricLabelPollFailures = "method"
	// metricLightModeDrift counts, in light chain tracking mode, the observations that load-balanced nodes
	// are at different heights, labelled by kind:
	//  - head_behind: an eth_blockNumber reading below the highest head already observed (ignored)
	//  - range_ahead: an eth_getLogs page above the stable threshold was rejected (expected - retried)
	//  - drift_violation: an eth_getLogs page at/below the stable threshold was rejected, so a node is
	//    more than checkpointBlockGap behind the observed head, which is outside the tolerated drift
	metricLightModeDrift               = "light_mode_drift_total"
	metricLabelLightModeDrift          = "kind"
	metricLightModeDriftHeadBehind     = "head_behind"
	metricLightModeDriftRangeAhead     = "range_ahead"
	metricLightModeDriftDriftViolation = "drift_violation"
)

// InitMetrics registers the block listener metrics against the supplied registry.
func (bl *blockListener) InitMetrics(ctx context.Context, registry metric.MetricsRegistry) error {
	mm, err := registry.NewMetricsManagerForSubsystem(ctx, metricsSubsystem)
	if err != nil {
		return i18n.WrapError(ctx, err, msgs.MsgMetricsInitFail, metricsSubsystem)
	}
	mm.NewGaugeMetric(ctx, metricTargetBlockHeight, "The block height reported by the connected node via eth_blockNumber", false)
	mm.NewGaugeMetric(ctx, metricCanonicalBlockHeight, "The block height of the head of the chain tracked by the block listener", false)
	mm.NewCounterMetricWithLabels(ctx, metricPollFailures, "The number of block listener JSON/RPC polls that have failed, by method", []string{metricLabelPollFailures}, false)
	mm.NewCounterMetricWithLabels(ctx, metricLightModeDrift, "The number of times, in light chain tracking mode, a JSON/RPC response showed the answering node behind the highest observed chain head, by kind", []string{metricLabelLightModeDrift}, false)

	bl.metricsLock.Lock()
	defer bl.metricsLock.Unlock()
	bl.metrics = mm
	return nil
}

func (bl *blockListener) getMetrics() metric.MetricsManager {
	bl.metricsLock.RLock()
	defer bl.metricsLock.RUnlock()
	return bl.metrics
}

func (bl *blockListener) setBlockHeightMetric(metricName string, blockHeight uint64) {
	mm := bl.getMetrics()
	if mm == nil {
		return
	}
	mm.SetGaugeMetric(bl.ctx, metricName, float64(blockHeight), nil)
}

func (bl *blockListener) incPollFailureMetric(method string) {
	mm := bl.getMetrics()
	if mm == nil {
		return
	}
	mm.IncCounterMetricWithLabels(bl.ctx, metricPollFailures, map[string]string{metricLabelPollFailures: method}, nil)
}

func (bl *blockListener) incLightModeDriftMetric(kind string) {
	mm := bl.getMetrics()
	if mm == nil {
		return
	}
	mm.IncCounterMetricWithLabels(bl.ctx, metricLightModeDrift, map[string]string{metricLabelLightModeDrift: kind}, nil)
}

func (bl *blockListener) IncLightModeRangeAhead() {
	bl.incLightModeDriftMetric(metricLightModeDriftRangeAhead)
}

func (bl *blockListener) IncLightModeDriftViolation() {
	bl.incLightModeDriftMetric(metricLightModeDriftDriftViolation)
}

// refreshTargetBlockHeightMetric queries the node for the height it reports, purely so the target gauge
// stays current. Only needed in full chain tracking mode.
func (bl *blockListener) refreshTargetBlockHeightMetric() {
	if bl.getMetrics() == nil {
		return // never drive any query of the node when metrics are not enabled
	}
	if _, err := bl.queryBlockHeightFromRPC(); err != nil {
		// Diagnostic only - the failure is recorded on the query failure counter, and the listen loop
		// has its own error handling for the chain state
		log.L(bl.ctx).Warnf("Failed to refresh target block height: %s", err)
	}
}
