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
	assert.Nil(t, bl.metricsLoopDone) // no poll loop started
}

func TestBlockListenerMetricsNoopBeforeInit(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	bl.canonicalChain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number: ethtypes.HexUint64(1000),
		Hash:   testBlockHashFor(1000),
	})

	// No metrics, and no eth_blockNumber call for the target height - the latter asserted by done()
	bl.setBlockHeightMetric(metricTargetBlockHeight, 1000)
	bl.emitChainStateMetrics()
}

func TestBlockListenerMetricsChainStateHeights(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	registry := initTestMetrics(t, bl)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(1005)
	})

	// The target height is reported before any block has been indexed
	bl.emitChainStateMetrics()
	_, ok := readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.False(t, ok)
	v, ok := readGaugeMetric(t, registry, metricTargetBlockHeight)
	assert.True(t, ok)
	assert.Equal(t, float64(1005), v)

	// Once blocks are indexed the canonical height is reported too
	bl.canonicalChain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number: ethtypes.HexUint64(1000),
		Hash:   testBlockHashFor(1000),
	})
	bl.canonicalChain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number: ethtypes.HexUint64(1001),
		Hash:   testBlockHashFor(1001),
	})
	bl.emitChainStateMetrics()
	v, ok = readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.True(t, ok)
	assert.Equal(t, float64(1001), v)

	// The canonical height follows the chain back down when a re-org trims the head
	_ = bl.canonicalChain.Remove(bl.canonicalChain.Back())
	bl.emitChainStateMetrics()
	v, _ = readGaugeMetric(t, registry, metricCanonicalBlockHeight)
	assert.Equal(t, float64(1000), v)
}

func TestBlockListenerMetricsTargetBlockHeightQueryFail(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	registry := initTestMetrics(t, bl)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"})

	// No retry, and nothing reported - the listen loop drives the chain state error handling
	bl.emitChainStateMetrics()
	_, ok := readGaugeMetric(t, registry, metricTargetBlockHeight)
	assert.False(t, ok)
}

func TestBlockListenerMetricsLoopWaitsForInitialBlockHeight(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	// The listen loop is never started, so the metrics loop must make no query at all (asserted by done())
	initTestMetrics(t, bl)
	time.Sleep(shortDelay)
	require.NotNil(t, bl.metricsLoopDone)
}

func TestBlockListenerMetricsInitTwiceStartsOneLoop(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	initTestMetrics(t, bl)
	loopDone := bl.metricsLoopDone
	initTestMetrics(t, bl) // separate registry, so registration succeeds again
	assert.Equal(t, loopDone, bl.metricsLoopDone)
}

func TestBlockListenerMetricsFullMode(t *testing.T) {
	blockHash1000 := testBlockHashFor(1000)
	blockHash1001 := testBlockHashFor(1001)

	ctx, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.BlockPollingInterval = 1 * time.Millisecond

		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(1001)
		})
		mockSeedBlockNotFound(mRPC, 1001-uint64(conf.MonitoredHeadLength)+1)
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

	// The height the node reports, and the height of the chain we've built from the filter
	waitForGaugeMetric(t, registry, metricTargetBlockHeight, 1001)
	waitForGaugeMetric(t, registry, metricCanonicalBlockHeight, 1001)
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

	// In light mode there is no canonical chain, so the head we dispatch is the canonical height
	waitForGaugeMetric(t, registry, metricTargetBlockHeight, 2000)
	waitForGaugeMetric(t, registry, metricCanonicalBlockHeight, 2000)
}
