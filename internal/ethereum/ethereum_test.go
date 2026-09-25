// Copyright © 2022 Kaleido, Inc.
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
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/evmconnect/mocks/ethblocklistenermocks"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethblocklistener"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

func utRPC(t *testing.T, mRPC rpcbackend.RPC) ethrpc.Client {
	rpc, err := ethrpc.NewClientWithBackends(t.Context(), ethrpc.RoutingModeHTTP, mRPC, nil)
	require.NoError(t, err)
	return rpc
}

func newTestConnector(t *testing.T, confSetup ...func(conf config.Section)) (context.Context, *ethConnector, *rpcbackendmocks.Backend, func()) {
	ctx, c, mRPC, done := newTestConnectorWithNoBlockerFilterDefaultMocks(t, confSetup...)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		filterID := args[1].(*string)
		*filterID = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Maybe()
	return ctx, c, mRPC, done
}

func newTestConnectorWithNoBlockerFilterDefaultMocks(t *testing.T, confSetup ...func(conf config.Section)) (context.Context, *ethConnector, *rpcbackendmocks.Backend, func()) {
	return newTestConnectorWithMockRPC(t, &rpcbackendmocks.Backend{}, confSetup...)
}

func newTestConnectorWithMockRPC(t *testing.T, mRPC *rpcbackendmocks.Backend, confSetup ...func(conf config.Section)) (context.Context, *ethConnector, *rpcbackendmocks.Backend, func()) {
	config.RootConfigReset()
	conf := config.RootSection("unittest")
	InitConfig(conf)
	//conf.Set(TraceTXForRevertReason, true)
	conf.Set(ffresty.HTTPConfigURL, "http://localhost:8545")
	conf.Set(BlockPollingInterval, "1h") // Disable for tests that are not using it
	logrus.SetLevel(logrus.DebugLevel)
	for _, fn := range confSetup {
		fn(conf)
	}
	ctx, done := context.WithCancel(context.Background())
	rpc, err := ethrpc.NewClientWithBackends(ctx, ethrpc.RoutingModeHTTP, mRPC, nil)
	require.NoError(t, err)
	cc, err := NewEthereumConnectorWithRPC(ctx, conf, rpc)
	assert.NoError(t, err)
	assert.NotNil(t, cc.RPC())

	c := cc.(*ethConnector)
	return ctx, c, mRPC, func() {
		done()
		mRPC.AssertExpectations(t)
		c.WaitClosed()
	}
}

// lightModeTestConf returns a config setup for light chain tracking mode
func lightModeTestConf(conf config.Section) {
	conf.Set(ChainTrackingMode, ffcapi.ChainTrackingModeLight)
	conf.Set(EventsFilterPollingMode, string(FilterPollingModeClient))
}

// mockLightModeRangeProbe sets the expectations for the startup probe that runs when a light mode
// connector is constructed: a head of 1000, and the node rejecting the range above it
func mockLightModeRangeProbe(mRPC *rpcbackendmocks.Backend) {
	mockLightModeProbeHead(mRPC, 1000)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", lightModeProbeRange).Return(&rpcbackend.RPCError{Message: "toBlock is greater than latest block"}).Once()
}

func mockLightModeProbeHead(mRPC *rpcbackendmocks.Backend, head uint64) {
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(head)
	}).Once()
}

var lightModeProbeRange = mock.MatchedBy(func(f *ethrpc.LogFilterJSONRPC) bool {
	return f.FromBlock.BigInt().Uint64() == 1000+lightModeRangeProbeOffset && f.ToBlock.BigInt().Uint64() == 1001+lightModeRangeProbeOffset
})

func newLightModeTestConnector(t *testing.T, confSetup ...func(conf config.Section)) (context.Context, *ethConnector, *rpcbackendmocks.Backend, func()) {
	mRPC := &rpcbackendmocks.Backend{}
	mockLightModeRangeProbe(mRPC)
	return newTestConnectorWithMockRPC(t, mRPC, append([]func(conf config.Section){lightModeTestConf}, confSetup...)...)
}

func TestConnectorInitLightModeValidation(t *testing.T) {

	newConnector := func(mRPC *rpcbackendmocks.Backend, confSetup func(conf config.Section)) error {
		config.RootConfigReset()
		conf := config.RootSection("unittest")
		InitConfig(conf)
		conf.Set(ffresty.HTTPConfigURL, "http://localhost:8545")
		lightModeTestConf(conf)
		confSetup(conf)
		_, err := NewEthereumConnectorWithRPC(context.Background(), conf, utRPC(t, mRPC))
		mRPC.AssertExpectations(t)
		return err
	}

	// The catchup page size must cover the unstable window (checkpointBlockGap+1) in a single page
	err := newConnector(&rpcbackendmocks.Backend{}, func(conf config.Section) {
		conf.Set(EventsCheckpointBlockGap, 50)
		conf.Set(EventsCatchupPageSize, 50)
	})
	assert.Regexp(t, "FF23081.*50.*51", err)

	// Defaults (page size 500, gap 50) are valid, and the range probe runs as part of construction
	mRPC := &rpcbackendmocks.Backend{}
	mockLightModeRangeProbe(mRPC)
	err = newConnector(mRPC, func(conf config.Section) {})
	assert.NoError(t, err)
}

func TestConnectorInitLightModeOverrides(t *testing.T) {

	// Server filter polling is overridden to client in light mode. Block timestamps only warn
	_, c, _, done := newLightModeTestConnector(t, func(conf config.Section) {
		conf.Set(EventsFilterPollingMode, string(FilterPollingModeServer))
		conf.Set(EventsBlockTimestamps, true)
	})
	defer done()
	assert.Equal(t, FilterPollingModeClient, c.eventFilterPollingMode)
	assert.True(t, c.eventBlockTimestamps)
}

func TestConnectorInitLightModeRangeProbeWarnings(t *testing.T) {

	// The probe is advisory - neither outcome below stops the connector starting (the connectors
	// here were constructed against a node that passed the probe, then re-probed directly)

	// The node silently returns an empty result for a range above its head - warned about
	_, c, mRPC, done := newLightModeTestConnector(t, func(conf config.Section) {
		conf.Set(EventsBlockTimestamps, false)
	})
	mockLightModeProbeHead(mRPC, 1000)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", lightModeProbeRange).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{}
	}).Once()
	c.verifyLightModeRangeErrors(context.Background())
	done()

	// The head cannot be queried - warned about, no retry
	_, c, mRPC, done = newLightModeTestConnector(t)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"}).Once()
	c.verifyLightModeRangeErrors(context.Background())
	done()
}

func TestConnectorDownscaleCatchupPageSize(t *testing.T) {

	_, c, _, done := newLightModeTestConnector(t, func(conf config.Section) {
		conf.Set(EventsCheckpointBlockGap, 50)
		conf.Set(EventsCatchupPageSize, 500)
	})
	defer done()

	// In light mode the page size halves down to, and never below, checkpointBlockGap+1
	for _, expected := range []int64{250, 125, 62, 51, 51} {
		c.downscaleCatchupPageSize(context.Background())
		assert.Equal(t, expected, c.getCatchupPageSize())
	}

	// In full mode it halves all the way down to 1, and stops there
	c.chainTrackingMode = ffcapi.ChainTrackingModeFull
	for _, expected := range []int64{25, 12, 6, 3, 1, 1} {
		c.downscaleCatchupPageSize(context.Background())
		assert.Equal(t, expected, c.getCatchupPageSize())
	}
}

func TestConnectorInit(t *testing.T) {

	config.RootConfigReset()
	conf := config.RootSection("unittest")
	InitConfig(conf)

	cc, err := NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23025", err)

	conf.Set(ffresty.HTTPConfigURL, "http://localhost:8545")
	conf.Set(ChainTrackingMode, "wrong")
	_, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23069.*wrong", err)

	conf.Set(RPCRoutingMode, "wrong")
	_, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23075.*wrong", err)

	conf.Set(RPCRoutingMode, ethrpc.RoutingModeAuto)
	conf.Set(ChainTrackingMode, "")
	conf.Set(EventsFilterPollingMode, "wrong")
	_, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23078.*wrong", err)

	conf.Set(EventsFilterPollingMode, string(FilterPollingModeClient))
	conf.Set(WebSocketsEnabled, true)
	conf.Set(EventsCatchupPageSize, 0)
	_, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23079", err)

	conf.Set(EventsCatchupThreshold, 1)
	conf.Set(EventsCatchupPageSize, 500)
	conf.Set(EventsCatchupDownscaleRegex, "Response size is larger.*error.")

	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	assert.Equal(t, int64(500), cc.(*ethConnector).catchupThreshold) // set to page size

	params := &abi.ParameterArray{
		{Name: "x", Type: "uint256"},
		{Name: "y", Type: "uint256"},
	}
	cv, err := params.ParseJSON([]byte(`{"x":12345,"y":23456}`))
	assert.NoError(t, err)

	conf.Set(ConfigDataFormat, "map")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	jv, err := cc.(*ethConnector).serializer.SerializeJSON(cv)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"x":"12345","y":"23456"}`, string(jv))

	conf.Set(ConfigDataFormat, "flat_array")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	jv, err = cc.(*ethConnector).serializer.SerializeJSON(cv)
	assert.NoError(t, err)
	assert.JSONEq(t, `["12345","23456"]`, string(jv))

	conf.Set(ConfigDataFormat, "self_describing")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	jv, err = cc.(*ethConnector).serializer.SerializeJSON(cv)
	assert.NoError(t, err)
	assert.JSONEq(t, `[{"name":"x","type":"uint256","value":"12345"},{"name":"y","type":"uint256","value":"23456"}]`, string(jv))

	tlsConf := conf.SubSection("tls")
	tlsConf.Set(fftls.HTTPConfTLSEnabled, true)
	tlsConf.Set(fftls.HTTPConfTLSCAFile, "!!!badness")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF00153", err)
	tlsConf.Set(fftls.HTTPConfTLSEnabled, false)

	conf.Set(ConfigDataFormat, "wrong")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23032.*wrong", err)

	conf.Set(ConfigDataFormat, "map")
	conf.Set(BlockCacheSize, "-1")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23040", err)

	conf.Set(BlockCacheSize, "1")
	conf.Set(TxCacheSize, "-1")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23040", err)

	conf.Set(TxCacheSize, "1")
	conf.Set(ReceiptCacheEnabled, true)
	conf.Set(ReceiptCacheSize, "-1")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23040", err)

	conf.Set(ReceiptCacheSize, "1")
	conf.Set(EventsCatchupDownscaleRegex, "[")
	cc, err = NewEthereumConnector(context.Background(), conf)
	assert.Regexp(t, "FF23051", err)
}

// TODO: remove once deprecated fields are removed
func TestNewEthereumConnectorConfigDeprecates(t *testing.T) {
	// Test deprecated fields
	config.RootConfigReset()
	conf := config.RootSection("unittest")
	InitConfig(conf)
	conf.Set(ffresty.HTTPConfigURL, "http://localhost:8545")

	// check deprecates
	conf.Set(DeprecatedRetryInitDelay, "100ms")
	conf.Set(DeprecatedRetryFactor, 2.0)
	conf.Set(DeprecatedRetryMaxDelay, "30s")
	cc, err := NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	assert.NotNil(t, cc)
	assert.Equal(t, 100*time.Millisecond, cc.(*ethConnector).retry.InitialDelay)
	assert.Equal(t, 2.0, cc.(*ethConnector).retry.Factor)
	assert.Equal(t, 30*time.Second, cc.(*ethConnector).retry.MaximumDelay)
}

func TestNewEthereumConnectorConfig(t *testing.T) {
	// Test deprecated fields
	config.RootConfigReset()
	conf := config.RootSection("unittest")
	InitConfig(conf)
	conf.Set(ffresty.HTTPConfigURL, "http://localhost:8545")

	// check new values set
	conf.Set(RetryInitDelay, "10s")
	conf.Set(RetryFactor, 4.0)
	conf.Set(RetryMaxDelay, "30s")
	cc, err := NewEthereumConnector(context.Background(), conf)
	assert.NoError(t, err)
	assert.NotNil(t, cc)
	assert.Equal(t, 10*time.Second, cc.(*ethConnector).retry.InitialDelay)
	assert.Equal(t, 4.0, cc.(*ethConnector).retry.Factor)
	assert.Equal(t, 30*time.Second, cc.(*ethConnector).retry.MaximumDelay)
}

func TestWithDeprecatedConfFallback(t *testing.T) {

	config.RootConfigReset()
	conf := config.RootSection("tdcf")
	conf.AddKnownKey("deprecatedKey")
	conf.AddKnownKey("newKey")

	conf.Set("deprecatedKey", 1111)
	require.Equal(t, 1111, withDeprecatedConfFallback(conf, conf.GetInt, "deprecatedKey", "newKey"))

	conf.Set("newKey", 2222)
	require.Equal(t, 2222, withDeprecatedConfFallback(conf, conf.GetInt, "deprecatedKey", "newKey"))

	config.RootConfigReset()
	conf = config.RootSection("tdcf")
	conf.AddKnownKey("deprecatedKey")
	conf.AddKnownKey("newKey")
	conf.Set("newKey", 2222)
	require.Equal(t, 2222, withDeprecatedConfFallback(conf, conf.GetInt, "deprecatedKey", "newKey"))

}

func TestRetryDefaultsFor429(t *testing.T) {
	config.RootConfigReset()
	conf := config.RootSection("unittest")
	InitConfig(conf)

	const serverAddress = "127.0.0.1:8545"
	conf.Set(ffresty.HTTPConfigURL, "http://"+serverAddress)
	conf.Set(BlockPollingInterval, "1h") // Disable for tests that are not using it
	logrus.SetLevel(logrus.DebugLevel)
	ctx, done := context.WithCancel(context.Background())
	cc, err := NewEthereumConnector(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, cc.RPC())
	assert.False(t, cc.RPC().HasWebSocket())
	defer done()

	// Start a simple HTTP server that always replies with 429 Too Many Requests
	listener, err := net.Listen("tcp", serverAddress)
	require.NoError(t, err)

	count := 0
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count++
			w.WriteHeader(http.StatusTooManyRequests)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"jsonrpc":"2.0","error":{"code":429,"message":"429 Too Many Requests"},"id":1}`))
		}),
	}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })

	rpcErr := cc.RPC().CallRPC(context.Background(), nil, "myMethod")
	assert.Regexp(t, "429 Too Many Requests", rpcErr.Message)
	// Verify retries occurred: initial call + retries (at least 2 retries should have occurred)
	assert.GreaterOrEqual(t, count, 2, "should retry on 429")
}

func TestReconcileConfirmationsForTransaction(t *testing.T) {
	ctx, c, _, done := newTestConnectorWithNoBlockerFilterDefaultMocks(t)
	defer done()

	mbl := ethblocklistenermocks.NewBlockListener(t)
	c.blockListener = mbl
	mbl.On("WaitClosed").Return()
	mbl.On("ReconcileConfirmationsForTransaction", ctx, "hash1", []*ethrpc.MinimalBlockInfo{
		{BlockNumber: 12345, BlockHash: []byte{}, ParentHash: []byte{}},
	}, uint64(10)).
		Return(
			&ethblocklistener.ConfirmationUpdateResult{
				Confirmations: []*ethrpc.MinimalBlockInfo{
					{
						BlockNumber: 12345,
					},
				},
			},
			&ethrpc.TxReceiptJSONRPC{},
			nil)

	_, err := c.ReconcileConfirmationsForTransaction(ctx, "hash1", []*ffcapi.MinimalBlockInfo{
		{BlockNumber: 12345},
	}, 10)
	require.NoError(t, err)

}
