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
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
)

// RoutingMode determines which of the two connections a given JSON/RPC method is sent on.
type RoutingMode string

const (
	// RoutingModeHTTP sends every JSON/RPC call over the HTTP connection pool.
	RoutingModeHTTP RoutingMode = "http"
	// RoutingModeWS sends every JSON/RPC call over the WebSocket connection.
	RoutingModeWS RoutingMode = "ws"
	// RoutingModeAuto picks the connection per method: node-sticky calls go over the
	// WebSocket, and stateless calls over the HTTP connection pool.
	// See httpRoutedMethods for the split.
	RoutingModeAuto RoutingMode = "auto"
	// RoutingModeLegacy reproduces the routing that existed before this was configurable.
	// See wsRoutedMethodsLegacy for exactly what it does, and for the two places
	// it cannot match the old behavior precisely.
	RoutingModeLegacy RoutingMode = "legacy"
)

// httpRoutedMethods are the JSON/RPC methods that carry no node-scoped state, and so are
// safe (and preferable) to spread across the load-balanced HTTP connection pool when
// running in RoutingModeAuto.
//
// Everything else is node-sticky: block, log, filter and receipt queries must all land on
// the same node as each other so that filter IDs stay valid, and so that the view of the
// chain we build up is self-consistent.
//
// A method that is not listed here routes to the WebSocket in auto mode. That is the safe
// default to have: a new chain-query method that silently spread itself across the HTTP
// pool would break stickiness in a way that only shows up as rare inconsistencies, whereas
// a new submission method landing on the WebSocket is merely less parallel than it could be.
var httpRoutedMethods = map[string]bool{
	"debug_traceTransaction":  true, // due to the size of the payload mainly
	"eth_call":                true,
	"eth_estimateGas":         true,
	"eth_gasPrice":            true,
	"eth_getBalance":          true,
	"eth_getTransactionCount": true,
	"eth_sendRawTransaction":  true,
	"eth_sendTransaction":     true,
}

// wsRoutedMethodsLegacy is the RoutingModeLegacy table - everything not listed here goes over
// HTTP.
//
// Before routing became configurable the split was not by method at all, it was by component:
// the block listener swapped its whole backend over to the WebSocket once connected, and every
// other part of the connector - including the event streams - held an HTTP-only backend and
// never touched the WebSocket. This table is that split expressed by method name, which it can
// be for every method called by only one of the two sides.
//
// Two methods are called by both sides, so no method-name table can reproduce the old routing
// exactly. Where they land here, and why:
//
//   - eth_getTransactionReceipt: the block listener fetched receipts over the WebSocket, and
//     the connector's own TransactionReceipt operation fetched them over HTTP. Listed here, so
//     the block listener's path (much the higher volume of the two, in full tracking mode) is
//     preserved and the connector's moves onto the WebSocket.
//
//   - eth_getFilterChanges: the block listener polled its block filter over the WebSocket, and
//     the event streams polled their log filters over HTTP. NOT listed here, so the event
//     streams are preserved - they are the higher volume, and they are the traffic anyone
//     reaching for this mode is most likely trying to move back off the WebSocket.
var wsRoutedMethodsLegacy = map[string]bool{
	"eth_blockNumber":           true,
	"eth_getBlockByHash":        true,
	"eth_getBlockByNumber":      true,
	"eth_getBlockReceipts":      true,
	"eth_getTransactionReceipt": true,
}

// Client is the single JSON/RPC entry point for the connector. It owns both the HTTP
// connection pool and the WebSocket connection (when one is configured), and decides which
// of them each call goes on based on the method name and the configured RoutingMode.
//
// Call sites do not choose a connection - they just call CallRPC.
//
// If Connect() is not called, every call routes to HTTP regardless of mode.
type Client interface {
	rpcbackend.RPC

	// Connect establishes the WebSocket connection if one is configured, and is a no-op
	// otherwise. It is idempotent so can be called multiple times, but fails after Close.
	Connect() error

	// Subscribe establishes a subscription on the WebSocket connection. It is an error to
	// call this when no WebSocket is configured - check HasWebSocket first.
	Subscribe(ctx context.Context, params ...interface{}) (rpcbackend.Subscription, *rpcbackend.RPCError)

	// HasWebSocket returns true if a WebSocket connection is configured. Note this is
	// independent of the routing mode - a WebSocket can be configured purely to carry
	// subscriptions, with all JSON/RPC calls going over HTTP.
	HasWebSocket() bool

	// RoutingMode returns the mode this client was constructed with.
	RoutingMode() RoutingMode

	// Close unsubscribes and closes the WebSocket connection (currently does not
	// prevent use of HTTP RPC calls after close)
	Close()
}

// Config is the configuration for a Client built from connection configuration.
type Config struct {
	RoutingMode           RoutingMode
	HTTP                  *ffresty.Config
	WS                    *wsclient.WSConfig // nil when no WebSocket is configured
	MaxConcurrentRequests int64
}

type client struct {
	mode        RoutingMode
	httpBackend rpcbackend.RPC
	wsBackend   rpcbackend.WebSocketRPCClient
	wsCtx       context.Context    // used by the websocket
	cancelWSCtx context.CancelFunc // allows connect to be cancelled
	connectMux  sync.Mutex         // serializes Connect against Close, and guards closed
	closed      bool
	wsConnected atomic.Bool
}

// NewClient builds a Client from config
func NewClient(ctx context.Context, conf *Config) (Client, error) {
	var wsBackend rpcbackend.WebSocketRPCClient
	if conf.WS != nil {
		wsBackend = rpcbackend.NewWSRPCClient(conf.WS)
	}
	httpBackend := rpcbackend.NewRPCClientWithOption(ffresty.NewWithConfig(ctx, *conf.HTTP), rpcbackend.RPCClientOptions{
		MaxConcurrentRequest: conf.MaxConcurrentRequests,
	})
	return NewClientWithBackends(ctx, conf.RoutingMode, httpBackend, wsBackend)
}

// NewClientWithBackends builds a client with existing backends already constructed
func NewClientWithBackends(ctx context.Context, mode RoutingMode, httpBackend rpcbackend.RPC, wsBackend rpcbackend.WebSocketRPCClient) (Client, error) {
	if mode == "" {
		mode = RoutingModeAuto
	}
	switch mode {
	case RoutingModeHTTP, RoutingModeWS, RoutingModeAuto, RoutingModeLegacy:
	default:
		return nil, i18n.NewError(ctx, msgs.MsgInvalidRPCRoutingMode, mode)
	}
	log.L(ctx).Infof("JSON/RPC routing mode '%s' (websocket configured=%t)", mode, wsBackend != nil)
	c := &client{
		mode:        mode,
		httpBackend: httpBackend,
		wsBackend:   wsBackend,
	}
	c.wsCtx, c.cancelWSCtx = context.WithCancel(ctx)
	return c, nil
}

func (c *client) RoutingMode() RoutingMode {
	return c.mode
}

func (c *client) HasWebSocket() bool {
	return c.wsBackend != nil
}

// routeFor picks the connection for a method. Until the WebSocket is connected, or if there
// is no WebSocket at all, everything goes to HTTP.
func (c *client) routeFor(method string) rpcbackend.RPC {
	if c.wsBackend == nil || !c.wsConnected.Load() {
		return c.httpBackend
	}
	switch c.mode {
	case RoutingModeWS:
		return c.wsBackend
	case RoutingModeAuto:
		if !httpRoutedMethods[method] {
			return c.wsBackend
		}
	case RoutingModeLegacy:
		if wsRoutedMethodsLegacy[method] {
			return c.wsBackend
		}
	}
	// RoutingModeHTTP, plus the methods each of the per-method modes leaves on HTTP
	return c.httpBackend
}

func (c *client) CallRPC(ctx context.Context, result interface{}, method string, params ...interface{}) *rpcbackend.RPCError {
	return c.routeFor(method).CallRPC(ctx, result, method, params...)
}

func (c *client) Connect() error {
	if c.wsBackend == nil {
		log.L(c.wsCtx).Infof("JSON/RPC only has a HTTP connection (routing mode '%s' does not affect behavior)", c.mode)
		return nil
	}

	c.connectMux.Lock()
	defer c.connectMux.Unlock()
	if c.wsConnected.Load() {
		return nil
	}
	if c.closed || c.wsCtx.Err() != nil {
		return i18n.NewError(c.wsCtx, msgs.MsgRPCClientClosed)
	}
	if err := c.wsBackend.Connect(c.wsCtx); err != nil {
		return err
	}
	// The WS connection is established by the time we get here (regardless of backgroundConnect options).
	c.wsConnected.Store(true)
	log.L(c.wsCtx).Infof("JSON/RPC WebSocket connected - routing mode '%s' now fully in effect", c.mode)
	return nil
}

func (c *client) Subscribe(ctx context.Context, params ...interface{}) (rpcbackend.Subscription, *rpcbackend.RPCError) {
	if c.wsBackend == nil {
		return nil, rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, msgs.MsgWebSocketNotConfigured)
	}
	return c.wsBackend.Subscribe(ctx, params...)
}

func (c *client) Close() {
	// Release any Connect that is blocked waiting for a socket before taking mutex
	c.cancelWSCtx()

	c.connectMux.Lock()
	defer c.connectMux.Unlock()
	if c.closed {
		return
	}
	// Cleanup the ws
	if c.wsBackend != nil {
		// Perform the unsubscribe best-effort on the background thread
		bgCtx, cancelCtx := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCtx()
		_ = c.wsBackend.UnsubscribeAll(bgCtx)
		c.wsBackend.Close()
		c.wsConnected.Store(false)
		// Now the socket is closed, release the context that was carrying its lifetime
		c.cancelWSCtx()
	}
	c.closed = true
}
