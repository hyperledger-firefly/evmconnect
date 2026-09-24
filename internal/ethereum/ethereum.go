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
	"context"
	"fmt"
	"math/big"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/internal/retryutil"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethblocklistener"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type ethConnector struct {
	rpc               ethrpc.Client
	chainTrackingMode ffcapi.ChainTrackingMode

	serializer                 *abi.Serializer
	gasEstimationFactor        *big.Float
	catchupPageSize            int64
	catchupThreshold           int64
	catchupDownscaleRegex      *regexp.Regexp
	checkpointBlockGap         int64
	retry                      retryutil.RetryWrapper
	eventBlockTimestamps       bool
	blockListener              ethblocklistener.BlockListener
	eventFilterPollingInterval time.Duration
	eventFilterPollingMode     filterPollingMode
	traceTXForRevertReason     bool
	chainID                    string

	mux          sync.Mutex
	eventStreams map[fftypes.UUID]*eventStream
	txCache      *lru.Cache
}

type Connector interface {
	ffcapi.API

	// RPC returns the JSON/RPC client used for everything the connector does. It owns both
	// the HTTP connection pool and the WebSocket (when enabled), and routes each call to one
	// of them based on the method name and the configured routing mode.
	RPC() ethrpc.Client

	// Get the high level block listener functionality, which provides a view of the head of the chain
	BlockListener() ethblocklistener.BlockListener
}

// NewEthereumConnector builds the JSON/RPC client from configuration, and the connector over it.
func NewEthereumConnector(ctx context.Context, conf config.Section) (cc Connector, err error) {
	if conf.GetString(ffresty.HTTPConfigURL) == "" {
		return nil, i18n.NewError(ctx, msgs.MsgMissingBackendURL)
	}

	var wsConf *wsclient.WSConfig
	if conf.GetBool(WebSocketsEnabled) {
		if wsConf, err = wsclient.GenerateConfig(ctx, conf); err != nil {
			return nil, err
		}
	}
	httpConf, err := ffresty.GenerateConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	rpc, err := ethrpc.NewClient(ctx, &ethrpc.Config{
		RoutingMode:           ethrpc.RoutingMode(conf.GetString(RPCRoutingMode)),
		HTTP:                  httpConf,
		WS:                    wsConf,
		MaxConcurrentRequests: conf.GetInt64(MaxConcurrentRequests),
	})
	if err != nil {
		return nil, err
	}

	return NewEthereumConnectorWithRPC(ctx, conf, rpc)
}

// NewEthereumConnectorWithRPC builds the connector over a pre-constructed JSON/RPC client.
// The caller owns closing the client - see ethConnector.WaitClosed.
func NewEthereumConnectorWithRPC(ctx context.Context, conf config.Section, rpc ethrpc.Client) (cc Connector, err error) {

	chainTrackingMode := ffcapi.ChainTrackingMode(conf.GetString(ChainTrackingMode))
	if chainTrackingMode == "" {
		chainTrackingMode = ffcapi.ChainTrackingModeFull
	}
	if chainTrackingMode != ffcapi.ChainTrackingModeLight && chainTrackingMode != ffcapi.ChainTrackingModeFull {
		return nil, i18n.NewError(ctx, msgs.MsgInvalidChainTrackingMode, chainTrackingMode)
	}

	eventFilterPollingMode := filterPollingMode(conf.GetString(EventsFilterPollingMode))
	if eventFilterPollingMode == "" {
		eventFilterPollingMode = FilterPollingModeServer
	}
	if eventFilterPollingMode != FilterPollingModeServer && eventFilterPollingMode != FilterPollingModeClient {
		return nil, i18n.NewError(ctx, msgs.MsgInvalidFilterPollingMode, eventFilterPollingMode)
	}

	c := &ethConnector{
		rpc:                        rpc,
		eventStreams:               make(map[fftypes.UUID]*eventStream),
		catchupPageSize:            conf.GetInt64(EventsCatchupPageSize),
		catchupThreshold:           conf.GetInt64(EventsCatchupThreshold),
		checkpointBlockGap:         conf.GetInt64(EventsCheckpointBlockGap),
		eventBlockTimestamps:       conf.GetBool(EventsBlockTimestamps),
		eventFilterPollingInterval: conf.GetDuration(EventsFilterPollingInterval),
		eventFilterPollingMode:     eventFilterPollingMode,
		traceTXForRevertReason:     conf.GetBool(TraceTXForRevertReason),
		chainTrackingMode:          chainTrackingMode,
		retry:                      retryutil.RetryWrapper{Retry: &retry.Retry{}},
	}

	c.retry.InitialDelay = withDeprecatedConfFallback(conf, conf.GetDuration, DeprecatedRetryInitDelay, RetryInitDelay)
	c.retry.Factor = withDeprecatedConfFallback(conf, conf.GetFloat64, DeprecatedRetryFactor, RetryFactor)
	c.retry.MaximumDelay = withDeprecatedConfFallback(conf, conf.GetDuration, DeprecatedRetryMaxDelay, RetryMaxDelay)

	if c.catchupPageSize < 1 {
		return nil, i18n.NewError(ctx, msgs.MsgCatchupPageSizeInvalid, c.catchupPageSize)
	}

	if c.catchupThreshold < c.catchupPageSize {
		log.L(ctx).Warnf("Catchup threshold %d must be at least as large as the catchup page size %d (overridden to %d)", c.catchupThreshold, c.catchupPageSize, c.catchupPageSize)
		c.catchupThreshold = c.catchupPageSize
	}

	if c.chainTrackingMode == ffcapi.ChainTrackingModeLight {
		// The contract light mode relies on is documented on ffcapi.ChainTrackingModeLight
		if c.catchupPageSize < c.checkpointBlockGap+1 {
			return nil, i18n.NewError(ctx, msgs.MsgLightModeCatchupPageSizeInvalid, c.catchupPageSize, c.checkpointBlockGap+1)
		}
		if c.eventFilterPollingMode != FilterPollingModeClient {
			return nil, i18n.NewError(ctx, msgs.MsgLightModeFilterPollingModeInvalid, c.eventFilterPollingMode)
		}
		if c.eventBlockTimestamps {
			log.L(ctx).Warnf("Light chain tracking mode with %s=true fetches each block by hash to obtain its timestamp - a fail-safe extra JSON/RPC call per block with events, that can be avoided by setting it false", EventsBlockTimestamps)
		}
		c.verifyLightModeRangeErrors(ctx)
	}

	c.txCache, err = lru.New(conf.GetInt(TxCacheSize))
	if err != nil {
		return nil, i18n.WrapError(ctx, err, msgs.MsgCacheInitFail, "transaction")
	}

	c.gasEstimationFactor = big.NewFloat(conf.GetFloat64(ConfigGasEstimationFactor))

	c.catchupDownscaleRegex, err = regexp.Compile(conf.GetString(EventsCatchupDownscaleRegex))
	if err != nil {
		return nil, i18n.WrapError(ctx, err, msgs.MsgInvalidRegex, c.catchupDownscaleRegex)
	}

	c.serializer = abi.NewSerializer().SetByteSerializer(abi.HexByteSerializer0xPrefix)
	switch conf.Get(ConfigDataFormat) {
	case "map":
		c.serializer.SetFormattingMode(abi.FormatAsObjects)
	case "flat_array":
		c.serializer.SetFormattingMode(abi.FormatAsFlatArrays)
	case "self_describing":
		c.serializer.SetFormattingMode(abi.FormatAsSelfDescribingArrays)
	default:
		return nil, i18n.NewError(ctx, msgs.MsgBadDataFormat, conf.Get(ConfigDataFormat), "map,flat_array,self_describing")
	}
	c.serializer.SetDefaultNameGenerator(func(idx int) string {
		name := "output"
		if idx > 0 {
			name = fmt.Sprintf("%s%v", name, idx)
		}
		return name
	})

	if c.blockListener, err = ethblocklistener.NewBlockListener(ctx, c.retry.Retry, &ethblocklistener.BlockListenerConfig{
		BlockPollingInterval:          conf.GetDuration(BlockPollingInterval),
		MonitoredHeadLength:           int(c.checkpointBlockGap),
		HederaCompatibilityMode:       conf.GetBool(HederaCompatibilityMode),
		BlockCacheSize:                conf.GetInt(BlockCacheSize),
		ReceiptCacheEnabled:           conf.GetBool(ReceiptCacheEnabled),
		ReceiptCacheSize:              conf.GetInt(ReceiptCacheSize),
		MaxAsyncBlockFetchConcurrency: conf.GetInt(MaxAsyncBlockFetchConcurrency),
		UseGetBlockReceipts:           conf.GetBool(UseGetBlockReceipts),
		ChainTrackingMode:             c.chainTrackingMode,
		FilterPollingMode:             c.eventFilterPollingMode,
	}, c.rpc); err != nil {
		return nil, err
	}

	return c, nil
}

const (
	lightModeRangeProbeOffset   = 1_000_000 // how far above the head the probe eth_getLogs range is placed
	lightModeRangeProbeAttempts = 5         // attempts to query the head before the probe gives up
)

// verifyLightModeRangeErrors probes the node once at startup, before anything else is polled, to
// check it rejects an eth_getLogs query for a block range above its head rather than returning an
// empty result. Light chain tracking mode relies on every successful eth_getLogs response being
// complete for the requested range to guarantee no events are missed when requests are load balanced
// across nodes at different heights (see ffcapi.ChainTrackingModeLight). The probe is advisory: a
// node that fails it, or cannot be reached, is warned about and the connector starts regardless.
func (c *ethConnector) verifyLightModeRangeErrors(ctx context.Context) {
	var hexHead ethtypes.HexInteger
	err := c.retry.Do(ctx, "light mode range probe", func(attempt int) (bool, error) {
		rpcErr := c.rpc.CallRPC(ctx, &hexHead, "eth_blockNumber")
		if rpcErr != nil {
			return attempt < lightModeRangeProbeAttempts, rpcErr.Error()
		}
		return false, nil
	})
	if err != nil {
		log.L(ctx).Warnf("Unable to verify the node rejects eth_getLogs ranges above its head (see ffcapi.ChainTrackingModeLight) - failed to query the chain head: %s", err)
		return
	}
	fromBlock := hexHead.BigInt().Uint64() + lightModeRangeProbeOffset
	var logs []*ethrpc.LogJSONRPC
	rpcErr := c.rpc.CallRPC(ctx, &logs, "eth_getLogs", &ethrpc.LogFilterJSONRPC{
		FromBlock: ethtypes.NewHexIntegerU64(fromBlock),
		ToBlock:   ethtypes.NewHexIntegerU64(fromBlock + 1),
	})
	if rpcErr == nil {
		log.L(ctx).Warnf("The node returned a successful response for an eth_getLogs query above its head (head=%d probe fromBlock=%d). Light chain tracking mode requires nodes to reject eth_getLogs ranges outside the blocks they know (geth, Nethermind, Besu 26.1.0+, reth 1.10.0+ and Erigon 3.3.3+ do), otherwise events can be missed when load balanced nodes are at different heights", hexHead.BigInt().Uint64(), fromBlock)
		return
	}
	log.L(ctx).Infof("Verified the node rejects eth_getLogs ranges above its head (head=%d probe fromBlock=%d): %s", hexHead.BigInt().Uint64(), fromBlock, rpcErr.Message)
}

// getCatchupPageSize reads the catchup page size, which is shared by every polling loop and can be
// reduced at runtime by downscaleCatchupPageSize
func (c *ethConnector) getCatchupPageSize() int64 {
	return atomic.LoadInt64(&c.catchupPageSize)
}

// downscaleCatchupPageSize halves the catchup page size after an eth_getLogs error matching the
// configured downscale regex (a node limiting its response size). In light chain tracking mode the
// page size is floored at checkpointBlockGap+1, so a single page always covers the unstable window
// at the head of the chain (see ffcapi.ChainTrackingModeLight).
func (c *ethConnector) downscaleCatchupPageSize(ctx context.Context) {
	current := c.getCatchupPageSize()
	if current <= 1 {
		return
	}
	newSize := current / 2
	if c.chainTrackingMode == ffcapi.ChainTrackingModeLight {
		if floor := c.checkpointBlockGap + 1; newSize < floor {
			if current <= floor {
				log.L(ctx).Warnf("Catchup page size cannot be reduced below checkpointBlockGap+1 (%d) in light chain tracking mode - the node is rejecting a page of that size", floor)
				return
			}
			newSize = floor
		}
	}
	atomic.StoreInt64(&c.catchupPageSize, newSize)
	if newSize < 20 {
		log.L(ctx).Warnf("Catchup page size auto-reduced to extremely low value %d. The connector may never catch up with the head of the chain.", newSize)
	}
}

func (c *ethConnector) RPC() ethrpc.Client {
	return c.rpc
}

func (c *ethConnector) BlockListener() ethblocklistener.BlockListener {
	return c.blockListener
}

// WaitClosed can be called after cancelling all the contexts, to wait for everything to close down
func (c *ethConnector) WaitClosed() {
	if c.blockListener != nil {
		c.blockListener.WaitClosed()
	}
	for _, s := range c.eventStreams {
		<-s.streamLoopDone
	}
	if c.rpc != nil {
		c.rpc.Close()
	}
}

func withDeprecatedConfFallback[T any](conf config.Section, getter func(string) T, deprecatedKey, newKey string) T {
	if !conf.IsSet(deprecatedKey) || (conf.IsSet(deprecatedKey) && conf.IsSet(newKey)) {
		return getter(newKey)
	}
	return getter(deprecatedKey)
}

// ReconcileConfirmationsForTransaction is the public API for reconciling transaction confirmations.
// It delegates to the blockListener's internal reconciliation logic.
func (c *ethConnector) ReconcileConfirmationsForTransaction(ctx context.Context, txHash string, existingConfirmations []*ffcapi.MinimalBlockInfo, targetConfirmationCount uint64) (res *ffcapi.ConfirmationUpdateResult, err error) {
	// Now we can start the reconciliation process
	var ethrpcRes *ethblocklistener.ConfirmationUpdateResult
	var ethrpcReceipt *ethrpc.TxReceiptJSONRPC
	ethrpcEC, err := ffcapiToEthRPCConfirmations(existingConfirmations)
	if err == nil {
		ethrpcRes, ethrpcReceipt, err = c.blockListener.ReconcileConfirmationsForTransaction(ctx, txHash, ethrpcEC, targetConfirmationCount)
	}
	if err == nil && ethrpcRes != nil {
		res = &ffcapi.ConfirmationUpdateResult{
			Confirmations:            ethRPCtoFFCAPIConfirmations(ethrpcRes.Confirmations),
			Rebuilt:                  ethrpcRes.Rebuilt,
			NewFork:                  ethrpcRes.NewFork,
			Confirmed:                ethrpcRes.Confirmed,
			CurrentConfirmationCount: ethrpcRes.CurrentConfirmationCount,
			TargetConfirmationCount:  ethrpcRes.TargetConfirmationCount,
		}
		if ethrpcReceipt != nil {
			res.Receipt = c.enrichTransactionReceipt(ctx, ethrpcReceipt)
		}
	}
	return res, err
}

func ffcapiToEthRPCConfirmations(ffcapiEC []*ffcapi.MinimalBlockInfo) (ec []*ethrpc.MinimalBlockInfo, err error) {
	ec = make([]*ethrpc.MinimalBlockInfo, len(ffcapiEC))
	for i, c := range ffcapiEC {
		ec[i] = &ethrpc.MinimalBlockInfo{BlockNumber: c.BlockNumber}
		if err == nil {
			ec[i].BlockHash, err = ethtypes.NewHexBytes0xPrefix(c.BlockHash)
		}
		if err == nil {
			ec[i].ParentHash, err = ethtypes.NewHexBytes0xPrefix(c.ParentHash)
		}
	}
	return ec, err
}

func ethRPCtoFFCAPIConfirmations(ffcapiEC []*ethrpc.MinimalBlockInfo) []*ffcapi.MinimalBlockInfo {
	ec := make([]*ffcapi.MinimalBlockInfo, len(ffcapiEC))
	for i, c := range ffcapiEC {
		ec[i] = &ffcapi.MinimalBlockInfo{
			BlockNumber: c.BlockNumber,
			BlockHash:   c.BlockHash.String(),
			ParentHash:  c.ParentHash.String(),
		}
	}
	return ec
}
