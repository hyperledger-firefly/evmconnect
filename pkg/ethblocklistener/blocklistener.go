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
	"container/list"
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/internal/retryutil"
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// a linked list of accumulated confirmations for a transaction
// the list is sorted by block number
//   - the first block is the block that contains the transaction hash
//   - the last block is the most recent confirmation
//
// this list can be used as input to the future reconcile request to avoid re-fetching the blocks if they are no longer
// in the in-memory partial chain
// WARNING: mutation to this list is not expected, invalid modifications will cause inefficiencies in the reconciliation process
//
//	`rebuilt` will be true if an invalid confirmation list is detected by the reconciliation process
type ConfirmationUpdateResult struct {
	Confirmations            []*ethrpc.MinimalBlockInfo `json:"confirmations,omitempty"`  // the confirmation list
	Rebuilt                  bool                       `json:"rebuilt,omitempty"`        // when true, it means the existing confirmations contained invalid blocks, the new confirmations are rebuilt from scratch
	NewFork                  bool                       `json:"newFork,omitempty"`        // when true, it means a new fork was detected based on the existing confirmations
	Confirmed                bool                       `json:"confirmed,omitempty"`      // when true, it means the confirmation list is complete and the transaction is confirmed
	TargetConfirmationCount  uint64                     `json:"targetConfirmationCount"`  // the target number of confirmations for this reconcile request
	CurrentConfirmationCount uint64                     `json:"currentConfirmationCount"` // the current number of confirmations for this reconcile request
}

type BlockListenerConfig struct {
	MonitoredHeadLength           int                      `json:"monitoredHeadLength"`
	BlockPollingInterval          time.Duration            `json:"blockPollingInterval"`
	HederaCompatibilityMode       bool                     `json:"hederaCompatibilityMode"`
	BlockCacheSize                int                      `json:"blockCacheSize"`
	ReceiptCacheEnabled           bool                     `json:"receiptCacheEnabled"`
	ReceiptCacheSize              int                      `json:"receiptCacheSize"`
	IncludeLogsBloom              bool                     `json:"includeLogsBloom"`
	UseGetBlockReceipts           bool                     `json:"useGetBlockReceipts"`
	MaxAsyncBlockFetchConcurrency int                      `json:"maxAsyncBlockFetchConcurrency"`
	ChainTrackingMode             ffcapi.ChainTrackingMode `json:"chainTrackingMode,omitempty"`
}

type BlockListener interface {
	ReconcileConfirmationsForTransaction(ctx context.Context, txHash string, existingConfirmations []*ethrpc.MinimalBlockInfo, targetConfirmationCount uint64) (*ConfirmationUpdateResult, *ethrpc.TxReceiptJSONRPC, error)
	GetMonitoredHeadLength() int // provides a getter on the configuration for unstable head length - as this information is important to consumers (might be multiple from this block listener)
	AddConsumer(ctx context.Context, c *BlockUpdateConsumer)
	RemoveConsumer(ctx context.Context, id *fftypes.UUID)
	GetHighestBlock(ctx context.Context) (uint64, bool)
	GetHighestBlockInfo(ctx context.Context) (*ethrpc.BlockInfoJSONRPC, bool)
	GetBlockGasLimit() *ethtypes.HexInteger // nil if unknown
	GetBlockInfoByNumber(ctx context.Context, blockNumber uint64, allowCache bool, expectedParentHashStr string, expectedBlockHashStr string) (*ethrpc.BlockInfoJSONRPC, error)
	GetBlockInfoByHash(ctx context.Context, hash0xString string) (*ethrpc.BlockInfoJSONRPC, error)
	GetEVMBlockWithTxHashesByHash(ctx context.Context, hash0xString string) (b *ethrpc.EVMBlockWithTxHashesJSONRPC, err error)
	GetEVMBlockWithTransactionsByHash(ctx context.Context, hash0xString string) (b *ethrpc.EVMBlockWithTransactionsJSONRPC, err error)
	GetEVMBlockWithTxHashesByNumber(ctx context.Context, numberLookup string) (b *ethrpc.EVMBlockWithTxHashesJSONRPC, err error)
	GetEVMBlockWithTransactionsByNumber(ctx context.Context, numberLookup string) (b *ethrpc.EVMBlockWithTransactionsJSONRPC, err error)
	FetchBlockReceiptsAsync(blockNumber uint64, blockHash ethtypes.HexBytes0xPrefix, cb func([]*ethrpc.TxReceiptJSONRPC, error))
	SnapshotMonitoredHeadChain() []*ethrpc.BlockInfoJSONRPC // snapshot the whole view, with complete blocks, using the read-lock.
	WaitClosed()
	GetBackend() rpcbackend.RPC
	UTSetBackend(rpcbackend.RPC)
}

func toMinimalBlockInfoList(blocks []*ethrpc.BlockInfoJSONRPC) []*ethrpc.MinimalBlockInfo {
	res := make([]*ethrpc.MinimalBlockInfo, len(blocks))
	for i, b := range blocks {
		res[i] = b.ToMinimalBlockInfo()
	}
	return res
}

type BlockUpdateConsumer struct {
	ID      *fftypes.UUID // could be an event stream ID for example - must be unique
	Ctx     context.Context
	Updates chan<- *ffcapi.BlockHashEvent // FFTM change events
}

// blockListener has two functions:
// 1) To establish and keep track of what the head block height of the blockchain is, so event streams know how far from the head they are
// 2) To feed new block information to any registered consumers
type blockListener struct {
	ctx            context.Context
	retry          *retryutil.RetryWrapper
	backend        rpcbackend.RPC
	wsBackend      rpcbackend.WebSocketRPCClient // if configured the getting the blockheight will not complete until WS connects, overrides backend once connected
	listenLoopDone chan struct{}

	isStarted bool
	startDone chan struct{}

	initialBlockHeightObtained    chan struct{}
	newHeadsTap                   chan struct{}
	newHeadsSub                   rpcbackend.Subscription
	consumerMux                   sync.Mutex // covers consumers and listenLoopDone
	consumers                     map[fftypes.UUID]*BlockUpdateConsumer
	blockCache                    *lru.Cache
	txReceiptCache                *lru.Cache
	blockFetchConcurrencyThrottle chan *blockReceiptRequest
	BlockListenerConfig

	//  canonical chain
	monitoredHeadLength uint64
	canonicalChainLock  sync.RWMutex // covers highestBlock and canonicalChain
	canonicalChain      *list.List
	highestBlockSet     bool
	highestBlock        uint64
	headBlockInfo       *ethrpc.BlockInfoJSONRPC // full info for the current head block, when seen

	// tx receipts indexed during canonical chain build, keyed by transaction hash
	txReceiptCacheLock            sync.RWMutex
	txReceiptCacheGeneration      uint64
	invalidatedReceiptBlockHashes map[string]struct{}
	pendingReceiptFetches         []*pendingReceiptFetch // queued under the canonicalChain write lock, dispatched after it is released

	// headBlockNumber mode: last head value sent on the block listener channel (only written from listenLoop)
	currentChainHead uint64
}

func NewBlockListener(ctx context.Context, retry *retry.Retry, conf *BlockListenerConfig, httpConf *ffresty.Config, wsConf *wsclient.WSConfig) (bl BlockListener, err error) {
	httpClient := ffresty.NewWithConfig(ctx, *httpConf)
	var wsBackend rpcbackend.WebSocketRPCClient
	if wsConf != nil {
		wsBackend = rpcbackend.NewWSRPCClient(wsConf)
	}
	return NewBlockListenerSupplyBackend(ctx, retry, conf, rpcbackend.NewRPCClient(httpClient), wsBackend)
}

func NewBlockListenerSupplyBackend(ctx context.Context, retry *retry.Retry, conf *BlockListenerConfig, httpBackend rpcbackend.RPC, wsBackend rpcbackend.WebSocketRPCClient) (_ BlockListener, err error) {
	if conf.MaxAsyncBlockFetchConcurrency <= 0 {
		conf.MaxAsyncBlockFetchConcurrency = 1
	}
	if conf.ChainTrackingMode == "" {
		conf.ChainTrackingMode = ffcapi.ChainTrackingModeFull
	}
	bl := &blockListener{
		ctx:                           log.WithLogFields(ctx, "role", "blocklistener"),
		retry:                         &retryutil.RetryWrapper{Retry: retry},
		backend:                       httpBackend, // use the HTTP backend - might get overwritten by a connected websocket later
		wsBackend:                     wsBackend,
		isStarted:                     false,
		startDone:                     make(chan struct{}),
		initialBlockHeightObtained:    make(chan struct{}),
		newHeadsTap:                   make(chan struct{}),
		highestBlockSet:               false,
		highestBlock:                  0,
		currentChainHead:              0,
		consumers:                     make(map[fftypes.UUID]*BlockUpdateConsumer),
		canonicalChain:                list.New(),
		blockFetchConcurrencyThrottle: make(chan *blockReceiptRequest, conf.MaxAsyncBlockFetchConcurrency),
		BlockListenerConfig:           *conf,
	}
	if conf.MonitoredHeadLength <= 0 {
		return nil, i18n.WrapError(ctx, err, msgs.MsgMonitoredHeadLengthInvalid, conf.MonitoredHeadLength)
	}
	bl.monitoredHeadLength = uint64(conf.MonitoredHeadLength)
	bl.blockCache, err = lru.New(conf.BlockCacheSize)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, msgs.MsgCacheInitFail, "block")
	}
	if conf.ReceiptCacheEnabled {
		bl.txReceiptCache, err = lru.New(conf.ReceiptCacheSize)
		if err != nil {
			return nil, i18n.WrapError(ctx, err, msgs.MsgCacheInitFail, "receipt")
		}
		bl.invalidatedReceiptBlockHashes = make(map[string]struct{})
	}
	return bl, nil
}

func (bl *blockListener) GetBackend() rpcbackend.RPC {
	return bl.backend
}

func (bl *blockListener) UTSetBackend(backend rpcbackend.RPC) {
	bl.backend = backend
}

func (bl *blockListener) GetMonitoredHeadLength() int {
	return bl.BlockListenerConfig.MonitoredHeadLength
}

// seedMonitoredHead fetches the single anchor block at highestBlock-MonitoredHeadLength+1.
// The returned block is used by the listen loop to seed the canonical chain on the first
// iteration via reconcileCanonicalChain, so that the chain is populated before the first
// filter poll and confirmations can be delivered as soon as they arrive.
func (bl *blockListener) seedMonitoredHead() *ethrpc.BlockInfoJSONRPC {
	bl.canonicalChainLock.RLock()
	highestBlockSet := bl.highestBlockSet
	startBlock := uint64(0)
	if bl.highestBlock >= bl.monitoredHeadLength {
		startBlock = bl.highestBlock - bl.monitoredHeadLength + 1
	}
	bl.canonicalChainLock.RUnlock()

	if !highestBlockSet {
		return nil
	}

	var bi *ethrpc.BlockInfoJSONRPC
	if err := bl.retry.Do(bl.ctx, "seed monitored head", func(_ int) (retry bool, err error) {
		bi, err = bl.GetBlockInfoByNumber(bl.ctx, startBlock, false, "", "")
		return err != nil, err
	}); err != nil || bi == nil {
		log.L(bl.ctx).Warnf("Failed to seed monitored head at block %d: %v", startBlock, err)
		return nil
	}
	log.L(bl.ctx).Infof("Seeded monitored head at block %d", startBlock)
	return bi
}

// setting block filter status updates that new block filter has been created
func (bl *blockListener) markStarted() {
	if !bl.isStarted {
		bl.isStarted = true
		close(bl.startDone)
	}
}

func (bl *blockListener) waitUntilStarted(ctx context.Context) {
	select {
	case <-bl.startDone:
	case <-bl.ctx.Done():
	case <-ctx.Done():
	}
}

func (bl *blockListener) newHeadsSubListener() {
	for range bl.newHeadsSub.Notifications() {
		select {
		case bl.newHeadsTap <- struct{}{}:
			// Do nothing apart from tap the listener to wake up early
			// when there's a notification to the change of the head.
		default:
		}
	}
}

// getBlockHeightWithRetry keeps retrying attempting to get the initial block height until successful
func (bl *blockListener) establishBlockHeightWithRetry() error {
	wsConnected := false
	return bl.retry.Do(bl.ctx, "get initial block height", func(_ int) (retry bool, err error) {
		// If we have a WebSocket backend, then we connect it and switch over to using it
		// (we accept an un-locked update here to backend, as the most important routine that's
		// querying block state is the one we're called on)
		if bl.wsBackend != nil {
			if !wsConnected {
				if err := bl.wsBackend.Connect(bl.ctx); err != nil {
					log.L(bl.ctx).Warnf("WebSocket connection failed, blocking startup of block listener: %s", err)
					return true, err
				}
				bl.backend = bl.wsBackend
				// if we retry subscribe, we don't want to retry connect
				wsConnected = true
			}
			if bl.newHeadsSub == nil {
				// Once subscribed the backend will keep us subscribed over reconnect
				sub, rpcErr := bl.wsBackend.Subscribe(bl.ctx, "newHeads")
				if rpcErr != nil {
					return true, rpcErr.Error()
				}
				bl.newHeadsSub = sub
				go bl.newHeadsSubListener()
			}
			// Ok all JSON/RPC from this point on uses our WS Backend, thus ensuring we're
			// sticky to the same node that the WS is connected to when we're doing queries
			// and building our cache.
			bl.backend = bl.wsBackend
		}

		// Now get the block height
		var hexBlockHeight ethtypes.HexInteger
		rpcErr := bl.backend.CallRPC(bl.ctx, &hexBlockHeight, "eth_blockNumber")
		if rpcErr != nil {
			log.L(bl.ctx).Warnf("Block height could not be obtained: %s", rpcErr.Message)
			return true, rpcErr.Error()
		}

		bl.setHighestBlock(hexBlockHeight.BigInt().Uint64())
		return false, nil
	})
}

// refreshHighestBlockFromRPC updates highestBlock from eth_blockNumber. Caller must not hold canonicalChainLock.
func (bl *blockListener) refreshHighestBlockFromRPC() (uint64, error) {
	var hexBlockHeight ethtypes.HexInteger
	rpcErr := bl.backend.CallRPC(bl.ctx, &hexBlockHeight, "eth_blockNumber")
	if rpcErr != nil {
		return 0, rpcErr.Error()
	}
	head := hexBlockHeight.BigInt().Uint64()
	return head, nil
}

func (bl *blockListener) waitNextIteration() bool {
	select {
	case <-bl.ctx.Done():
		return false
	case <-time.After(bl.BlockPollingInterval):
	case <-bl.newHeadsTap:
	}
	return true
}

func (bl *blockListener) listenLoop() {
	defer close(bl.listenLoopDone)

	err := bl.establishBlockHeightWithRetry()
	close(bl.initialBlockHeightObtained)
	if err != nil {
		log.L(bl.ctx).Warnf("Block listener exiting before establishing initial block height: %s", err)
	}

	// Seed the canonical chain before starting the filter loop (not applicable in light mode).
	// The seed block is reconciled on the first loop iteration instead of polling the filter,
	// so the in-memory chain is pre-populated and confirmations can be delivered immediately.
	var seedBi *ethrpc.BlockInfoJSONRPC
	if bl.ChainTrackingMode != ffcapi.ChainTrackingModeLight {
		seedBi = bl.seedMonitoredHead()
	}

	var filter string
	failCount := 0
	gapPotential := true
	firstIteration := true
	for {
		if failCount > 0 {
			if bl.retry.DoFailureDelay(bl.ctx, failCount) {
				log.L(bl.ctx).Debugf("Block listener loop exiting")
				return
			}
		} else {
			// Sleep for the polling interval, or until we're shoulder tapped by the newHeads listener
			if !firstIteration {
				if !bl.waitNextIteration() {
					log.L(bl.ctx).Debugf("Block listener loop stopping")
					return
				}
			} else {
				firstIteration = false
			}
		}

		if filter == "" {
			err := bl.backend.CallRPC(bl.ctx, &filter, "eth_newBlockFilter")
			if err != nil {
				log.L(bl.ctx).Errorf("Failed to establish new block filter: %s", err.Message)
				failCount++
				continue
			}
			bl.markStarted()
		}

		// On the first iteration use the seed block (leaves blockHashes nil).
		// On subsequent iterations poll the filter for new block hashes.
		var blockHashes []ethtypes.HexBytes0xPrefix
		var notifyPos *list.Element
		if seedBi != nil {
			notifyPos = bl.reconcileCanonicalChain(seedBi)
			seedBi = nil
		} else {
			rpcErr := bl.backend.CallRPC(bl.ctx, &blockHashes, "eth_getFilterChanges", filter)
			if rpcErr != nil {
				if etherrors.MapError(etherrors.FilterRPCMethods, rpcErr.Error()) == ffcapi.ErrorReasonNotFound {
					log.L(bl.ctx).Warnf("Block filter '%v' no longer valid. Recreating filter: %s", filter, rpcErr.Message)
					filter = ""
					gapPotential = true
				}
				log.L(bl.ctx).Errorf("Failed to query block filter changes: %s", rpcErr.Message)
				failCount++
				continue
			}
			log.L(bl.ctx).Debugf("Block filter received new block hashes: %+v", blockHashes)
		}

		if bl.ChainTrackingMode == ffcapi.ChainTrackingModeLight {
			head, err := bl.refreshHighestBlockFromRPC()
			if err != nil {
				log.L(bl.ctx).Errorf("Failed to refresh chain head: %s", err)
				failCount++
				continue
			}
			if head == bl.currentChainHead {
				failCount = 0
				continue
			}
			bl.currentChainHead = head
			update := &ffcapi.BlockHashEvent{GapPotential: false, Created: fftypes.Now(), HeadBlockNumber: bl.currentChainHead}
			bl.consumerMux.Lock()
			consumers := make([]*BlockUpdateConsumer, 0, len(bl.consumers))
			for _, c := range bl.consumers {
				consumers = append(consumers, c)
			}
			bl.consumerMux.Unlock()
			bl.dispatchToConsumers(consumers, update)
			failCount = 0
			continue
		}

		update := &ffcapi.BlockHashEvent{GapPotential: gapPotential, Created: fftypes.Now()}
		for _, h := range blockHashes {
			if len(h) != 32 {
				if !bl.HederaCompatibilityMode {
					log.L(bl.ctx).Errorf("Attempted to index block header with non-standard length: %d", len(h))
					failCount++
					continue
				}

				if len(h) < 32 {
					log.L(bl.ctx).Errorf("Cannot index block header hash of length: %d", len(h))
					failCount++
					continue
				}

				h = h[0:32]
			}

			// Do a lookup of the block (which will then go into our cache).
			bi, err := bl.GetBlockInfoByHash(bl.ctx, h.String())
			switch {
			case err != nil:
				log.L(bl.ctx).Debugf("Failed to query block '%s': %s", h, err)
			case bi == nil:
				log.L(bl.ctx).Debugf("Block '%s' no longer available after notification (assuming due to re-org)", h)
			default:
				candidate := bl.reconcileCanonicalChain(bi)
				// Check this is the lowest position to notify from
				if candidate != nil && (notifyPos == nil || candidate.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64() <= notifyPos.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64()) {
					notifyPos = candidate
				}
			}
		}
		if notifyPos != nil {
			// We notify for all hashes from the point of change in the chain onwards
			for notifyPos != nil {
				update.BlockHashes = append(update.BlockHashes, notifyPos.Value.(*ethrpc.BlockInfoJSONRPC).Hash.String())
				notifyPos = notifyPos.Next()
			}

			// Take a copy of the consumers in the lock
			bl.consumerMux.Lock()
			consumers := make([]*BlockUpdateConsumer, 0, len(bl.consumers))
			for _, c := range bl.consumers {
				consumers = append(consumers, c)
			}
			bl.consumerMux.Unlock()

			// Spin through delivering the block update
			bl.dispatchToConsumers(consumers, update)
		}

		// Reset retry count when we have a full successful loop
		failCount = 0
		gapPotential = false

	}
}

// reconcileCanonicalChain takes an update on a block, and reconciles it against the in-memory view of the
// head of the canonical chain we have. If these blocks do not just fit onto the end of the chain, then we
// work backwards building a new view and notify about all blocks that are changed in that process.
func (bl *blockListener) reconcileCanonicalChain(bi *ethrpc.BlockInfoJSONRPC) *list.Element {
	bl.canonicalChainLock.Lock()
	pos := bl.reconcileCanonicalChainLocked(bi)
	pending := bl.pendingReceiptFetches
	bl.pendingReceiptFetches = nil
	bl.canonicalChainLock.Unlock()

	// Dispatch queued receipt fetches only after releasing the lock - dispatch blocks when
	// the fetch concurrency throttle is saturated, and must not stall readers of the chain
	bl.dispatchPendingReceiptFetches(pending)
	return pos
}

// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) reconcileCanonicalChainLocked(bi *ethrpc.BlockInfoJSONRPC) *list.Element {
	bl.checkAndSetHighestBlock(bi)

	// Find the position of this block in the block sequence
	pos := bl.canonicalChain.Back()
	for {
		if pos == nil || pos.Value == nil {
			// We've eliminated all the existing chain (if there was any)
			return bl.handleNewBlock(bi, nil)
		}
		posBlock := pos.Value.(*ethrpc.BlockInfoJSONRPC)
		switch {
		case posBlock.Equal(bi):
			// This is a duplicate - no need to notify of anything
			return nil
		case posBlock.Number.Uint64() == bi.Number.Uint64():
			// We are replacing a block in the chain
			return bl.handleNewBlock(bi, pos.Prev())
		case posBlock.Number.Uint64() < bi.Number.Uint64():
			// We have a position where this block goes
			return bl.handleNewBlock(bi, pos)
		default:
			// We've not wound back to the point this block fits yet
			pos = pos.Prev()
		}
	}
}

// handleNewBlock rebuilds the canonical chain around a new block, checking if we need to rebuild our
// view of the canonical chain behind it, or trimming anything after it that is invalidated by a new fork.
//
// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) handleNewBlock(mbi *ethrpc.BlockInfoJSONRPC, addAfter *list.Element) *list.Element {
	// If we have an existing canonical chain before this point, then we need to check we've not
	// invalidated that with this block. If we have, then we have to re-verify our whole canonical
	// chain from the first block. Then notify from the earliest point where it has diverged.
	if addAfter != nil {
		prevBlock := addAfter.Value.(*ethrpc.BlockInfoJSONRPC)
		if prevBlock.Number.Uint64() != (mbi.Number.Uint64()-1) || !prevBlock.Hash.Equals(mbi.ParentHash) {
			log.L(bl.ctx).Infof("Notified of block %d / %s that does not fit after block %d / %s (expected parent: %s)", mbi.Number.Uint64(), mbi.Hash, prevBlock.Number.Uint64(), prevBlock.Hash, mbi.ParentHash)
			return bl.rebuildCanonicalChain()
		}
	}

	// Ok, we can add this block
	var newElem *list.Element
	if addAfter == nil {
		bl.resetReceiptCache()
		_ = bl.canonicalChain.Init()
		newElem = bl.canonicalChain.PushBack(mbi)
	} else {
		newElem = bl.canonicalChain.InsertAfter(mbi, addAfter)
		// Trim everything from this point onwards. Note that the following cases are covered on other paths:
		// - This was just a duplicate notification of a block that fits into our chain - discarded in reconcileCanonicalChain()
		// - There was a gap before us in the chain, and the tail is still valid - we would have called rebuildCanonicalChain() above
		var trimmedBlocks []*ethrpc.BlockInfoJSONRPC
		nextElem := newElem.Next()
		for nextElem != nil {
			toRemove := nextElem
			nextElem = nextElem.Next()
			if toRemove.Value != nil {
				trimmedBlocks = append(trimmedBlocks, toRemove.Value.(*ethrpc.BlockInfoJSONRPC))
			}
			_ = bl.canonicalChain.Remove(toRemove)
		}
		if len(trimmedBlocks) > 0 {
			bl.invalidateReceiptsForForkTrim(trimmedBlocks)
		}
	}

	// Trim the amount of history we keep based on the configured amount of instability at the front of the chain
	for bl.canonicalChain.Len() > bl.MonitoredHeadLength {
		_ = bl.canonicalChain.Remove(bl.canonicalChain.Front())
	}
	bl.queueReceiptFetch(mbi)

	log.L(bl.ctx).Debugf("Added block %d / %s parent=%s to in-memory canonical chain (new length=%d)", mbi.Number.Uint64(), mbi.Hash, mbi.ParentHash, bl.canonicalChain.Len())

	return newElem
}

// rebuildCanonicalChain is called (only on non-empty case) when our current chain does not seem to line up with
// a recent block advertisement. So we need to work backwards to the last point of consistency with the current
// chain and re-query the chain state from there.
//
// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) rebuildCanonicalChain() *list.Element {
	// If none of our blocks were valid, start from the first block number we've notified about previously
	lastValidBlock := bl.trimToLastValidBlock()
	var nextBlockNumber uint64
	var expectedParentHash ethtypes.HexBytes0xPrefix
	if lastValidBlock != nil {
		nextBlockNumber = lastValidBlock.Number.Uint64() + 1
		log.L(bl.ctx).Infof("Canonical chain partially rebuilding from block %d", nextBlockNumber)
		expectedParentHash = lastValidBlock.Hash
	} else {
		// no valid block found, so we need to re-initialize the chain
		bl.resetReceiptCache()
		firstBlock := bl.canonicalChain.Front()
		if firstBlock == nil || firstBlock.Value == nil {
			return nil
		}
		nextBlockNumber = firstBlock.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64()
		log.L(bl.ctx).Warnf("Canonical chain re-initialized at block %d", nextBlockNumber)
		// Clear out the whole chain
		bl.canonicalChain = bl.canonicalChain.Init()
	}
	var notifyPos *list.Element
	for {
		var bi *ethrpc.BlockInfoJSONRPC
		var reason ffcapi.ErrorReason
		err := bl.retry.Do(bl.ctx, "rebuild listener canonical chain", func(_ int) (retry bool, err error) {
			bi, err = bl.GetBlockInfoByNumber(bl.ctx, nextBlockNumber, false, "", "")
			return true, err
		})
		if err != nil {
			if reason != ffcapi.ErrorReasonNotFound {
				return nil // Context must have been cancelled
			}
		}
		if bi == nil {
			log.L(bl.ctx).Infof("Canonical chain rebuilt the chain to the head block %d", nextBlockNumber-1)
			break
		}

		// It's possible the chain will change while we're doing this, and we fall back to the next block notification
		// to sort that out.
		if expectedParentHash != nil && !bi.ParentHash.Equals(expectedParentHash) {
			log.L(bl.ctx).Infof("Canonical chain rebuilding stopped at block: %d due to mismatch hash for parent block (%d): %s (expected: %s)", nextBlockNumber, nextBlockNumber-1, bi.ParentHash, expectedParentHash)
			break
		}
		expectedParentHash = bi.Hash
		nextBlockNumber++

		// Note we do not trim to a length here, as we need to notify for every block we haven't notified for.
		// Trimming to a length will happen when we get blocks that slot into our existing view
		newElem := bl.canonicalChain.PushBack(bi)
		if notifyPos == nil {
			notifyPos = newElem
		}

		bl.checkAndSetHighestBlock(bi)
		bl.queueReceiptFetch(bi)

	}
	return notifyPos
}

// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) trimToLastValidBlock() (lastValidBlock *ethrpc.BlockInfoJSONRPC) {
	// First remove from the end until we get a block that matches the current un-cached query view from the chain
	lastElem := bl.canonicalChain.Back()
	var startingNumber *uint64
	var trimmedBlocks []*ethrpc.BlockInfoJSONRPC
	for lastElem != nil && lastElem.Value != nil {

		// Query the block that is no at this blockNumber
		currentViewBlock := lastElem.Value.(*ethrpc.BlockInfoJSONRPC)
		if startingNumber == nil {
			currentNumber := currentViewBlock.Number.Uint64()
			startingNumber = &currentNumber
			log.L(bl.ctx).Debugf("Canonical chain checking from last block: %d", startingNumber)
		}
		var freshBlockInfo *ethrpc.BlockInfoJSONRPC
		err := bl.retry.Do(bl.ctx, "rebuild listener canonical chain", func(_ int) (retry bool, err error) {
			log.L(bl.ctx).Debugf("Canonical chain validating block: %d", currentViewBlock.Number.Uint64())
			freshBlockInfo, err = bl.GetBlockInfoByNumber(bl.ctx, currentViewBlock.Number.Uint64(), false, "", "")
			return true, err
		})
		if err != nil {
			return nil // Context must have been cancelled
		}

		if freshBlockInfo != nil && freshBlockInfo.Hash.Equals(currentViewBlock.Hash) {
			log.L(bl.ctx).Debugf("Canonical chain found last valid block %d", currentViewBlock.Number.Uint64())
			lastValidBlock = currentViewBlock
			// Trim everything after this point, as it's invalidated
			nextElem := lastElem.Next()
			for nextElem != nil {
				next := nextElem.Next()
				_ = bl.canonicalChain.Remove(nextElem)
				nextElem = next
			}
			break
		}
		trimmedBlocks = append(trimmedBlocks, currentViewBlock)
		lastElem = lastElem.Prev()
	}
	// Invalidate cached receipts for the trimmed blocks in one pass. If no valid block was
	// found the caller resets the whole cache anyway, but invalidating here is still safe.
	bl.invalidateReceiptsForForkTrim(trimmedBlocks)

	if startingNumber != nil && lastValidBlock != nil && *startingNumber != lastValidBlock.Number.Uint64() {
		log.L(bl.ctx).Debugf("Canonical chain trimmed from block %d to block %d (total number of in memory blocks: %d)", startingNumber, lastValidBlock.Number.Uint64(), bl.MonitoredHeadLength)
	}
	return lastValidBlock
}

func (bl *blockListener) dispatchToConsumers(consumers []*BlockUpdateConsumer, update *ffcapi.BlockHashEvent) {
	for _, c := range consumers {
		log.L(bl.ctx).Tracef("Notifying consumer %s of blocks %v (gap=%t)", c.ID, update.BlockHashes, update.GapPotential)
		select {
		case c.Updates <- update:
		case <-bl.ctx.Done(): // loop, we're stopping and will exit on next loop
		case <-c.Ctx.Done():
			log.L(bl.ctx).Debugf("Block update consumer %s closed", c.ID)
			bl.consumerMux.Lock()
			delete(bl.consumers, *c.ID)
			bl.consumerMux.Unlock()
		}
	}
}

func (bl *blockListener) checkAndStartListenerLoop() {
	bl.consumerMux.Lock()
	defer bl.consumerMux.Unlock()
	if bl.listenLoopDone == nil {
		bl.listenLoopDone = make(chan struct{})
		go bl.listenLoop()
	}
}

func (bl *blockListener) AddConsumer(ctx context.Context, c *BlockUpdateConsumer) {
	bl.checkAndStartListenerLoop()
	bl.waitUntilStarted(ctx) // need to make sure the listener is started before adding any consumers
	bl.consumerMux.Lock()
	defer bl.consumerMux.Unlock()
	bl.consumers[*c.ID] = c
}

func (bl *blockListener) RemoveConsumer(_ context.Context, id *fftypes.UUID) {
	bl.consumerMux.Lock()
	defer bl.consumerMux.Unlock()
	delete(bl.consumers, *id)
}

func (bl *blockListener) waitForBlockHeightInit(ctx context.Context) bool {
	bl.canonicalChainLock.RLock()
	highestBlockSet := bl.highestBlockSet
	bl.canonicalChainLock.RUnlock()
	if highestBlockSet {
		return true
	}
	select {
	case <-bl.initialBlockHeightObtained:
		return true
	case <-ctx.Done():
		return false
	}
}

func (bl *blockListener) GetHighestBlock(ctx context.Context) (uint64, bool) {
	bl.checkAndStartListenerLoop()
	// block height will be established as the first step of listener startup process
	// so we don't need to wait for the entire startup process to finish to return the result
	if !bl.waitForBlockHeightInit(ctx) {
		return 0, false
	}
	bl.canonicalChainLock.RLock()
	highestBlock := bl.highestBlock
	bl.canonicalChainLock.RUnlock()
	log.L(ctx).Debugf("ChainHead=%d", highestBlock)
	return highestBlock, true
}

func (bl *blockListener) GetHighestBlockInfo(ctx context.Context) (*ethrpc.BlockInfoJSONRPC, bool) {
	bl.checkAndStartListenerLoop()
	if !bl.waitForBlockHeightInit(ctx) {
		return nil, false
	}
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	if bl.headBlockInfo == nil {
		return nil, false
	}
	return bl.headBlockInfo, true
}

// Gives a non-nil value only if the block listener is tracking the head and has access to the full block
func (bl *blockListener) GetBlockGasLimit() *ethtypes.HexInteger {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	if bl.headBlockInfo == nil || bl.headBlockInfo.GasLimit == nil {
		return nil
	}
	if bl.headBlockInfo.GasLimit.BigInt().Sign() <= 0 {
		return nil
	}
	return bl.headBlockInfo.GasLimit
}

func (bl *blockListener) GetHeadBlockNumber(_ context.Context) uint64 {
	return bl.currentChainHead
}

func (bl *blockListener) setHighestBlock(block uint64) {
	bl.canonicalChainLock.Lock()
	defer bl.canonicalChainLock.Unlock()
	bl.highestBlock = block
	bl.highestBlockSet = true
}

// checkAndSetHighestBlock records the chain head height and caches full block info for the head.
// highestBlock is often set first by eth_blockNumber during startup, before any full block arrives.
// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) checkAndSetHighestBlock(bi *ethrpc.BlockInfoJSONRPC) {
	block := bi.Number.Uint64()
	if block > bl.highestBlock {
		bl.highestBlock = block
		bl.highestBlockSet = true
		bl.headBlockInfo = bi
	} else if block == bl.highestBlock {
		// Height already known from eth_blockNumber. Store the first full block at that height.
		bl.headBlockInfo = bi
	}
	// Lower blocks are ignored. reconcileCanonicalChain also processes historical blocks during fork rebuilds.
}

// snapshot the whole view using the read-lock.
func (bl *blockListener) SnapshotMonitoredHeadChain() []*ethrpc.BlockInfoJSONRPC {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()

	res := make([]*ethrpc.BlockInfoJSONRPC, 0, bl.canonicalChain.Len())
	for pos := bl.canonicalChain.Front(); pos != nil; pos = pos.Next() {
		res = append(res, pos.Value.(*ethrpc.BlockInfoJSONRPC))
	}
	return res
}

func (bl *blockListener) WaitClosed() {
	bl.consumerMux.Lock()
	listenLoopDone := bl.listenLoopDone
	bl.consumerMux.Unlock()
	if bl.wsBackend != nil {
		_ = bl.wsBackend.UnsubscribeAll(bl.ctx)
		bl.wsBackend.Close()
	}
	if listenLoopDone != nil {
		select {
		case <-listenLoopDone:
		case <-bl.ctx.Done():
		}
	}
}
