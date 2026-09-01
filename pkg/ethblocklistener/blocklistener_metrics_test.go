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
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// readGaugeMetric returns the current value of one of our block height gauges, and whether it has
// been reported at all yet
func readGaugeMetric(t *testing.T, registry metric.MetricsRegistry, metricName string) (float64, bool) {
	mfs, err := registry.GetGatherer().Gather()
	require.NoError(t, err)
	fullName := "ff_" + metricsSubsystem + "_" + metricName
	for _, mf := range mfs {
		if mf.GetName() == fullName {
			require.Len(t, mf.GetMetric(), 1)
			return mf.GetMetric()[0].GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// readPollFailureMetric returns the current count of poll failures for the given JSON/RPC method
func readPollFailureMetric(t *testing.T, registry metric.MetricsRegistry, method string) float64 {
	mfs, err := registry.GetGatherer().Gather()
	require.NoError(t, err)
	fullName := "ff_" + metricsSubsystem + "_" + metricPollFailures
	for _, mf := range mfs {
		if mf.GetName() == fullName {
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == metricLabelPollFailures && l.GetValue() == method {
						return m.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return 0
}

func waitForGaugeMetric(t *testing.T, registry metric.MetricsRegistry, metricName string, expected float64) {
	assert.Eventually(t, func() bool {
		v, ok := readGaugeMetric(t, registry, metricName)
		return ok && v == expected
	}, 5*time.Second, time.Millisecond, "gauge %s did not reach %f", metricName, expected)
}

func initTestMetrics(t *testing.T, bl *blockListener) metric.MetricsRegistry {
	registry := metric.NewPrometheusMetricsRegistry("ut")
	require.NoError(t, bl.InitMetrics(context.Background(), registry))
	return registry
}

func TestBlockListenerMetricsInitFailDuplicateSubsystem(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	registry := metric.NewPrometheusMetricsRegistry("ut")
	_, err := registry.NewMetricsManagerForSubsystem(context.Background(), metricsSubsystem)
	require.NoError(t, err)

	err = bl.InitMetrics(context.Background(), registry)
	assert.Regexp(t, "FF23074", err)
	assert.Nil(t, bl.getMetrics())
}

func TestBlockListenerMetricsNoopBeforeInit(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	// All emit points no-op, and the target height refresh makes no query at all - the latter
	// asserted by done(), as no eth_blockNumber call is mocked
	bl.setBlockHeightMetric(metricTargetBlockHeight, 1000)
	bl.incPollFailureMetric("eth_blockNumber")
	bl.refreshTargetBlockHeightMetric()
}

func TestBlockListenerMetricsTrackedHeightFromListenerState(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	registry := initTestMetrics(t, bl)

	// Nothing is reported until we have a height
	_, ok := readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.False(t, ok)

	// The initial height established at startup from eth_blockNumber
	bl.setHighestBlock(1000)
	v, ok := readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.True(t, ok)
	assert.Equal(t, float64(1000), v)

	// Then each block we index that advances the head we are tracking
	bl.checkAndSetHighestBlock(&ethrpc.BlockInfoJSONRPC{
		Number: ethtypes.HexUint64(1001),
		Hash:   testBlockHashFor(1001),
	})
	v, _ = readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.Equal(t, float64(1001), v)

	// Blocks at or below the head we already have don't move it
	bl.checkAndSetHighestBlock(&ethrpc.BlockInfoJSONRPC{
		Number: ethtypes.HexUint64(999),
		Hash:   testBlockHashFor(999),
	})
	v, _ = readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.Equal(t, float64(1001), v)
}

func TestBlockListenerMetricsTargetBlockHeightQueryFail(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	registry := initTestMetrics(t, bl)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"})

	// No retry, and no height reported - just the failure counted, so a node that is failing to answer
	// is distinguishable from one reporting a height that isn't moving
	bl.refreshTargetBlockHeightMetric()
	_, ok := readGaugeMetric(t, registry, metricTargetBlockHeight)
	assert.False(t, ok)
	assert.Equal(t, float64(1), readPollFailureMetric(t, registry, "eth_blockNumber"))
}

func TestBlockListenerMetricsFullMode(t *testing.T) {
	blockHash1000 := testBlockHashFor(1000)
	blockHash1001 := testBlockHashFor(1001)

	ctx, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.BlockPollingInterval = 1 * time.Millisecond
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(1000)
		})
		mockSeedBlockNotFound(mRPC, 1000-uint64(conf.MonitoredHeadLength)+1)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, blockHash1001).Once()
		mockFilterChangesEmpty(mRPC)
		mockBlockByHash(mRPC, 1001, blockHash1001, blockHash1000)
	})
	defer done()

	registry := initTestMetrics(t, bl)

	updates := make(chan *ffcapi.BlockHashEvent, 16)
	bl.AddConsumer(ctx, &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     ctx,
		Updates: updates,
	})

	// The height the node reports, refreshed by the listen loop, and the head of the chain we've built
	waitForGaugeMetric(t, registry, metricTargetBlockHeight, 1000)
	waitForGaugeMetric(t, registry, metricCanonicalBlockHeight, 1001)
}

func TestBlockListenerMetricsFullModeFilterFail(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.BlockPollingInterval = 1 * time.Millisecond

		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(1001)
		})
		mockSeedBlockNotFound(mRPC, 1001-uint64(conf.MonitoredHeadLength)+1)
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(&rpcbackend.RPCError{Message: "pop"})
	})
	defer done()

	registry := initTestMetrics(t, bl)

	// Start the loop directly - the filter never establishes, so the listener never marks itself started
	bl.checkAndStartListenerLoop()

	// The target height is refreshed ahead of the filter calls, so we can still see the chain moving on
	// while the filter is broken - and the failures are counted
	waitForGaugeMetric(t, registry, metricTargetBlockHeight, 1001)
	assert.Eventually(t, func() bool {
		return readPollFailureMetric(t, registry, "eth_newBlockFilter") > 0
	}, 5*time.Second, time.Millisecond)
}

func TestBlockListenerMetricsLightMode(t *testing.T) {
	ctx, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.ChainTrackingMode = ffcapi.ChainTrackingModeLight
		conf.BlockPollingInterval = 1 * time.Millisecond

		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(2000)
		})
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChangesEmpty(mRPC)
	})
	defer done()

	registry := initTestMetrics(t, bl)

	updates := make(chan *ffcapi.BlockHashEvent, 16)
	bl.AddConsumer(ctx, &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     ctx,
		Updates: updates,
	})

	// In light mode there is no canonical chain, so the head we dispatch is the height we track
	waitForGaugeMetric(t, registry, metricTargetBlockHeight, 2000)
	waitForGaugeMetric(t, registry, metricCanonicalBlockHeight, 2000)
}
