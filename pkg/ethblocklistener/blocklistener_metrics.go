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
	"time"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

const (
	metricsSubsystem = "blocklistener"

	// metricTargetBlockHeight is the block height the endpoint we are connected to reports via eth_blockNumber.
	metricTargetBlockHeight = "target_block_height"
	// metricCanonicalBlockHeight is the height of the head of the canonical chain this listener is managing,
	// built from the block filter / newHeads subscription. It should track the target height very closely.
	metricCanonicalBlockHeight = "canonical_block_height"
)

// InitMetrics registers the block height gauges against the supplied registry, and starts the poll loop
// that emits them.
func (bl *blockListener) InitMetrics(ctx context.Context, registry metric.MetricsRegistry) error {
	mm, err := registry.NewMetricsManagerForSubsystem(ctx, metricsSubsystem)
	if err != nil {
		return i18n.WrapError(ctx, err, msgs.MsgMetricsInitFail, metricsSubsystem)
	}
	mm.NewGaugeMetric(ctx, metricTargetBlockHeight, "The block height reported by the connected node via eth_blockNumber", false)
	mm.NewGaugeMetric(ctx, metricCanonicalBlockHeight, "The block height of the head of the canonical chain tracked by the block listener", false)

	bl.metricsLock.Lock()
	defer bl.metricsLock.Unlock()
	bl.metrics = mm
	if bl.metricsLoopDone == nil {
		bl.metricsLoopDone = make(chan struct{})
		go bl.metricsLoop()
	}
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

// metricsLoop samples the block heights on the block polling interval. Decoupled from the listener loop.
func (bl *blockListener) metricsLoop() {
	defer close(bl.metricsLoopDone)

	// Wait for the listen loop to establish the initial block height before making any query of our own.
	// As well as avoiding driving JSON/RPC traffic before the listener is running, this ensures the
	// WebSocket backend (when configured) has been switched in before we read bl.backend.
	select {
	case <-bl.initialBlockHeightObtained:
	case <-bl.ctx.Done():
		return
	}

	for {
		bl.emitChainStateMetrics()
		select {
		case <-bl.ctx.Done():
			return
		case <-time.After(bl.BlockPollingInterval):
		}
	}
}

func (bl *blockListener) emitChainStateMetrics() {
	if bl.getMetrics() == nil {
		return // never drive any query of the node when metrics are not enabled
	}

	// The canonical head is free to read - note in light chain tracking mode there is no canonical chain,
	// so the listen loop emits the head it dispatches to consumers instead
	if bl.ChainTrackingMode != ffcapi.ChainTrackingModeLight {
		if canonicalHeight, ok := bl.getCanonicalBlockHeight(); ok {
			bl.setBlockHeightMetric(metricCanonicalBlockHeight, canonicalHeight)
		}
	}

	// The target height requires a query of the node
	head, err := bl.refreshHighestBlockFromRPC()
	if err != nil {
		// Purely a metrics query - the listen loop has its own error handling for the chain state
		log.L(bl.ctx).Warnf("Failed to query target block height for metrics: %s", err)
		return
	}
	bl.setBlockHeightMetric(metricTargetBlockHeight, head)
}

// getCanonicalBlockHeight returns the head of the in-memory canonical chain, with ok false until we
// have indexed a block.
func (bl *blockListener) getCanonicalBlockHeight() (uint64, bool) {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	back := bl.canonicalChain.Back()
	if back == nil || back.Value == nil {
		return 0, false
	}
	return back.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64(), true
}
