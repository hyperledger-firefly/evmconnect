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

package ethrpc

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockWSBackend is a mock rpcbackend.Backend with the extra WebSocketRPCClient methods, so
// we can assert which of the two connections a call was routed to.
type mockWSBackend struct {
	rpcbackendmocks.Backend
	connectErr  error
	connections int
	// blockConnect makes Connect hang until its context is cancelled, modelling a node that
	// accepts the TCP connection and then never completes the handshake
	blockConnect  bool
	connectCtx    context.Context
	connectedCtxs chan context.Context
	sub           rpcbackend.Subscription
	subErr        *rpcbackend.RPCError
	unsubs        int
	closes        int
}

func (m *mockWSBackend) Connect(ctx context.Context) error {
	m.connections++
	m.connectCtx = ctx
	if m.connectedCtxs != nil {
		m.connectedCtxs <- ctx
	}
	if m.blockConnect {
		<-ctx.Done()
		return ctx.Err()
	}
	return m.connectErr
}

func (m *mockWSBackend) Subscribe(_ context.Context, _ ...interface{}) (rpcbackend.Subscription, *rpcbackend.RPCError) {
	return m.sub, m.subErr
}

func (m *mockWSBackend) Subscriptions() []rpcbackend.Subscription { return nil }

func (m *mockWSBackend) UnsubscribeAll(_ context.Context) *rpcbackend.RPCError {
	m.unsubs++
	return nil
}

func (m *mockWSBackend) Close() { m.closes++ }

// newTestClient builds a connected client, and returns the mock behind each connection.
func newTestClient(t *testing.T, mode RoutingMode) (Client, *rpcbackendmocks.Backend, *mockWSBackend) {
	mHTTP := &rpcbackendmocks.Backend{}
	mWS := &mockWSBackend{}
	c, err := NewClientWithBackends(context.Background(), mode, mHTTP, mWS)
	require.NoError(t, err)
	require.NoError(t, c.Connect())
	return c, mHTTP, mWS
}

func TestClientRoutingAuto(t *testing.T) {
	// The full routing table as it stands, asserted end to end. Anything not in
	// httpRoutedMethods must be node-sticky and land on the WebSocket.
	httpMethods := []string{
		"debug_traceTransaction",
		"eth_call",
		"eth_estimateGas",
		"eth_gasPrice",
		"eth_getBalance",
		"eth_getTransactionCount",
		"eth_sendRawTransaction",
		"eth_sendTransaction",
	}
	wsMethods := []string{
		"eth_blockNumber",
		"eth_getBlockByHash",
		"eth_getBlockByNumber",
		"eth_getBlockReceipts",
		"eth_getFilterChanges",
		"eth_getFilterLogs",
		"eth_getLogs",
		"eth_getTransactionByHash",
		"eth_getTransactionReceipt",
		"eth_newBlockFilter",
		"eth_newFilter",
		"eth_uninstallFilter",
		"net_version",
		"some_methodWeHaveNeverHeardOf", // unlisted methods are sticky by default
	}

	assertRouting(t, RoutingModeAuto, httpMethods, wsMethods)
}

func TestClientRoutingLegacy(t *testing.T) {
	// Only the block listener used the WebSocket before this was configurable, so its methods
	// are the WebSocket set and everything else - the whole of the rest of the connector,
	// event streams included - stays on HTTP.
	wsMethods := []string{
		"eth_blockNumber",
		"eth_getBlockByHash",
		"eth_getBlockByNumber",
		"eth_getBlockReceipts",
		// Called by both sides. The block listener's use is preserved here, so the
		// connector's own TransactionReceipt operation moves onto the WebSocket.
		"eth_getTransactionReceipt",
	}
	httpMethods := []string{
		"debug_traceTransaction",
		"eth_call",
		"eth_estimateGas",
		"eth_gasPrice",
		"eth_getBalance",
		"eth_getLogs",
		"eth_getTransactionByHash",
		"eth_getTransactionCount",
		"eth_sendRawTransaction",
		"eth_sendTransaction",
		"net_version",
		// The event streams' filter lifecycle, all of which was on HTTP before. The block
		// listener's block filter is dragged onto HTTP with it - eth_getFilterChanges cannot
		// be in both sets, and eth_newBlockFilter has to follow it so that the filter is
		// created and polled on one connection.
		"eth_getFilterChanges",
		"eth_getFilterLogs",
		"eth_newBlockFilter",
		"eth_newFilter",
		"eth_uninstallFilter",
		// Unlisted methods stay on HTTP in this mode, which is where anything the old code
		// did not explicitly hand to the block listener would have gone
		"some_methodWeHaveNeverHeardOf",
	}

	assertRouting(t, RoutingModeLegacy, httpMethods, wsMethods)
}

// assertRouting drives every method through a connected client, and fails if any of them lands
// on the connection it was not expected on.
func assertRouting(t *testing.T, mode RoutingMode, httpMethods, wsMethods []string) {
	c, mHTTP, mWS := newTestClient(t, mode)
	for _, method := range httpMethods {
		mHTTP.On("CallRPC", mock.Anything, mock.Anything, method).Return(nil).Once()
	}
	for _, method := range wsMethods {
		mWS.On("CallRPC", mock.Anything, mock.Anything, method).Return(nil).Once()
	}

	for _, method := range append(append([]string{}, httpMethods...), wsMethods...) {
		require.Nil(t, c.CallRPC(context.Background(), nil, method), method)
	}

	// Each mock asserts it saw exactly the calls expected of it, and no others
	mHTTP.AssertExpectations(t)
	mWS.AssertExpectations(t)
}

func TestClientRoutingAllHTTP(t *testing.T) {
	c, mHTTP, mWS := newTestClient(t, RoutingModeHTTP)
	// Including the node-sticky ones - in this mode the WebSocket carries subscriptions only
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs").Return(nil).Once()
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction").Return(nil).Once()

	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_getLogs"))
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_sendTransaction"))

	mHTTP.AssertExpectations(t)
	mWS.AssertExpectations(t)
}

func TestClientRoutingAllWS(t *testing.T) {
	c, mHTTP, mWS := newTestClient(t, RoutingModeWS)
	// Including the stateless ones, which give up the HTTP pool's concurrency in this mode
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs").Return(nil).Once()
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction").Return(nil).Once()

	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_getLogs"))
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_sendTransaction"))

	mHTTP.AssertExpectations(t)
	mWS.AssertExpectations(t)
}

func TestClientRoutesToHTTPUntilConnected(t *testing.T) {
	mHTTP := &rpcbackendmocks.Backend{}
	mWS := &mockWSBackend{}
	c, err := NewClientWithBackends(context.Background(), RoutingModeWS, mHTTP, mWS)
	require.NoError(t, err)

	// Even in all-WS mode, a call before Connect degrades to HTTP rather than failing
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Once()
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_blockNumber"))

	require.NoError(t, c.Connect())

	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Once()
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_blockNumber"))

	mHTTP.AssertExpectations(t)
	mWS.AssertExpectations(t)
}

func TestClientRoutesToHTTPWithNoWebSocket(t *testing.T) {
	mHTTP := &rpcbackendmocks.Backend{}
	c, err := NewClientWithBackends(context.Background(), RoutingModeWS, mHTTP, nil)
	require.NoError(t, err)
	assert.False(t, c.HasWebSocket())

	// Connect is a no-op, and everything routes to HTTP whatever the mode asked for
	require.NoError(t, c.Connect())
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs").Return(nil).Once()
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_getLogs"))

	// ... and Close has nothing to do
	c.Close()
	mHTTP.AssertExpectations(t)
}

func TestClientPropagatesResultAndError(t *testing.T) {
	c, mHTTP, mWS := newTestClient(t, RoutingModeAuto)
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_gasPrice").Return(nil).Run(func(args mock.Arguments) {
		*(args[1].(*string)) = "0x12345"
	}).Once()
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).
		Return(&rpcbackend.RPCError{Message: "pop"}).Once()

	var gasPrice string
	require.Nil(t, c.CallRPC(context.Background(), &gasPrice, "eth_gasPrice"))
	assert.Equal(t, "0x12345", gasPrice)

	rpcErr := c.CallRPC(context.Background(), nil, "eth_getLogs", "params")
	require.NotNil(t, rpcErr)
	assert.Equal(t, "pop", rpcErr.Message)

	mHTTP.AssertExpectations(t)
	mWS.AssertExpectations(t)
}

func TestClientConnectIdempotentAndPropagatesFailure(t *testing.T) {
	mWS := &mockWSBackend{connectErr: fmt.Errorf("pop")}
	c, err := NewClientWithBackends(context.Background(), RoutingModeAuto, &rpcbackendmocks.Backend{}, mWS)
	require.NoError(t, err)

	require.Regexp(t, "pop", c.Connect())
	assert.Equal(t, 1, mWS.connections)

	// A retry after a failed connect does reconnect...
	mWS.connectErr = nil
	require.NoError(t, c.Connect())
	assert.Equal(t, 2, mWS.connections)

	// ... but once connected, a repeat call is a no-op. The block listener calls Connect from
	// its retry loop, and must not tear down a working connection when only Subscribe failed.
	require.NoError(t, c.Connect())
	assert.Equal(t, 2, mWS.connections)
}

func TestClientSubscribe(t *testing.T) {
	mSub := &rpcbackendmocks.Subscription{}
	c, _, mWS := newTestClient(t, RoutingModeAuto)
	mWS.sub = mSub

	sub, rpcErr := c.Subscribe(context.Background(), "newHeads")
	require.Nil(t, rpcErr)
	assert.Same(t, mSub, sub)
}

func TestClientSubscribeNoWebSocket(t *testing.T) {
	c, err := NewClientWithBackends(context.Background(), RoutingModeHTTP, &rpcbackendmocks.Backend{}, nil)
	require.NoError(t, err)

	_, rpcErr := c.Subscribe(context.Background(), "newHeads")
	require.NotNil(t, rpcErr)
	assert.Regexp(t, "FF23076", rpcErr.Message)
}

func TestClientCloseUnsubscribes(t *testing.T) {
	c, mHTTP, mWS := newTestClient(t, RoutingModeAuto)
	c.Close()
	assert.Equal(t, 1, mWS.unsubs)
	assert.Equal(t, 1, mWS.closes)

	// Close is idempotent - a second call does not unsubscribe or close again
	c.Close()
	assert.Equal(t, 1, mWS.unsubs)
	assert.Equal(t, 1, mWS.closes)

	// Once closed we route at HTTP, rather than at a dead socket
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs").Return(nil).Once()
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_getLogs"))
	mHTTP.AssertExpectations(t)

	// Close is terminal - a Connect racing the shutdown must not establish a socket that
	// nothing is left to close
	err := c.Connect()
	assert.Regexp(t, "FF23077", err)
	assert.Equal(t, 1, mWS.connections)
}

func TestClientCloseUnblocksStuckConnect(t *testing.T) {
	mWS := &mockWSBackend{blockConnect: true, connectedCtxs: make(chan context.Context, 1)}
	c, err := NewClientWithBackends(context.Background(), RoutingModeAuto, &rpcbackendmocks.Backend{}, mWS)
	require.NoError(t, err)

	connectDone := make(chan error, 1)
	go func() {
		// Note the caller's context is never cancelled here - only Close can release this
		connectDone <- c.Connect()
	}()
	<-mWS.connectedCtxs // wait until we are actually inside the connect, holding the mutex

	c.Close()

	select {
	case err := <-connectDone:
		require.Error(t, err) // released with the context cancelled error, not a connection
	case <-time.After(5 * time.Second):
		require.Fail(t, "Connect was not released by Close")
	}
	assert.False(t, c.(*client).wsConnected.Load())
	assert.Equal(t, 1, mWS.closes)
}

func TestClientConnectSurvivesReturn(t *testing.T) {
	mWS := &mockWSBackend{}
	c, err := NewClientWithBackends(context.Background(), RoutingModeAuto, &rpcbackendmocks.Backend{}, mWS)
	require.NoError(t, err)
	require.NoError(t, c.Connect())

	require.NotNil(t, mWS.connectCtx)
	require.NoError(t, mWS.connectCtx.Err(), "the connection context was cancelled, which closes the socket")

	// ... and the routing is live over the WebSocket, not degraded back to HTTP
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs").Return(nil).Once()
	require.Nil(t, c.CallRPC(context.Background(), nil, "eth_getLogs"))
	mWS.AssertExpectations(t)

	// Close releases the context that was carrying the socket's lifetime
	c.Close()
	assert.Error(t, mWS.connectCtx.Err())
}

func TestClientConnectCancelledByConstructionContext(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	mWS := &mockWSBackend{}
	c, err := NewClientWithBackends(ctx, RoutingModeAuto, &rpcbackendmocks.Backend{}, mWS)
	require.NoError(t, err)
	require.NoError(t, c.Connect())

	require.NoError(t, mWS.connectCtx.Err())
	cancelCtx()
	assert.Error(t, mWS.connectCtx.Err())
}

func TestClientConnectAfterContextCancelled(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	mWS := &mockWSBackend{}
	c, err := NewClientWithBackends(ctx, RoutingModeAuto, &rpcbackendmocks.Backend{}, mWS)
	require.NoError(t, err)
	cancelCtx()

	assert.Regexp(t, "FF23077", c.Connect())
	assert.Equal(t, 0, mWS.connections)
}

func TestClientBadRoutingMode(t *testing.T) {
	_, err := NewClientWithBackends(context.Background(), RoutingMode("wrong"), &rpcbackendmocks.Backend{}, nil)
	assert.Regexp(t, "FF23075.*wrong", err)
}

func TestClientDefaultRoutingMode(t *testing.T) {
	c, err := NewClientWithBackends(context.Background(), "", &rpcbackendmocks.Backend{}, nil)
	require.NoError(t, err)
	assert.Equal(t, RoutingModeAuto, c.RoutingMode())
}

func TestNewClientFromConfig(t *testing.T) {
	c, err := NewClient(context.Background(), &Config{
		RoutingMode:           RoutingModeAuto,
		HTTP:                  &ffresty.Config{URL: "http://localhost:8545"},
		WS:                    &wsclient.WSConfig{HTTPURL: "http://localhost:8546"},
		MaxConcurrentRequests: 10,
	})
	require.NoError(t, err)
	assert.True(t, c.HasWebSocket())

	c, err = NewClient(context.Background(), &Config{
		HTTP: &ffresty.Config{URL: "http://localhost:8545"},
	})
	require.NoError(t, err)
	assert.False(t, c.HasWebSocket())
}

func TestClientConcurrentConnect(t *testing.T) {
	mHTTP := &rpcbackendmocks.Backend{}
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Maybe()
	mWS := &mockWSBackend{}
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Maybe()

	c, err := NewClientWithBackends(context.Background(), RoutingModeAuto, mHTTP, mWS)
	require.NoError(t, err)

	const callers = 20
	const callsEach = 50
	var wg sync.WaitGroup
	wg.Add(callers + 1)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsEach; j++ {
				require.Nil(t, c.CallRPC(context.Background(), nil, "eth_blockNumber"))
			}
		}()
	}
	go func() {
		defer wg.Done()
		for j := 0; j < callsEach; j++ {
			require.NoError(t, c.Connect())
		}
	}()

	wg.Wait()
	assert.Equal(t, 1, mWS.connections)
}

func TestClientConcurrentConnectAndClose(t *testing.T) {
	mHTTP := &rpcbackendmocks.Backend{}
	mHTTP.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Maybe()
	mWS := &mockWSBackend{}
	mWS.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Maybe()

	c, err := NewClientWithBackends(context.Background(), RoutingModeAuto, mHTTP, mWS)
	require.NoError(t, err)

	const callers = 10
	const callsEach = 50
	var wg sync.WaitGroup
	wg.Add(callers + 2)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsEach; j++ {
				// Whichever side of the close this lands, it must reach a live connection
				require.Nil(t, c.CallRPC(context.Background(), nil, "eth_blockNumber"))
			}
		}()
	}
	go func() {
		defer wg.Done()
		for j := 0; j < callsEach; j++ {
			// Either it connects, or it loses to Close and gets told the client is closed -
			// never a socket established after Close that nothing will close
			if err := c.Connect(); err != nil {
				assert.Regexp(t, "FF23077", err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for j := 0; j < callsEach; j++ {
			c.Close()
		}
	}()

	wg.Wait()
	assert.Equal(t, 1, mWS.closes)
	assert.Equal(t, 1, mWS.unsubs)
	assert.LessOrEqual(t, mWS.connections, 1)
}
