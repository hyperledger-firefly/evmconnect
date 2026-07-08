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
	"runtime/debug"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
)

type blockReceiptRequest struct {
	bl          *blockListener
	blockNumber ethtypes.HexUint64
	blockHash   ethtypes.HexBytes0xPrefix
	cb          func([]*ethrpc.TxReceiptJSONRPC, error)
}

// Initiates a background request to get all the receipts in a block.
// Blocks if throttled.
// Delivers an error if the block is not found.
func (bl *blockListener) FetchBlockReceiptsAsync(blockNumber uint64, blockHash ethtypes.HexBytes0xPrefix, cb func([]*ethrpc.TxReceiptJSONRPC, error)) {
	brr := &blockReceiptRequest{
		bl:          bl,
		blockNumber: ethtypes.HexUint64(blockNumber),
		blockHash:   blockHash,
		cb:          cb,
	}
	// We have a throttle here that's global to the whole blockListener, to protect us from flooding the RPC gateway / node
	brr.bl.blockFetchConcurrencyThrottle <- brr
	go brr.run()
}

func (brr *blockReceiptRequest) run() {
	var err error
	var receipts []*ethrpc.TxReceiptJSONRPC
	earlyExit := true
	defer func() {
		<-brr.bl.blockFetchConcurrencyThrottle // return our slot
		if earlyExit {
			panicDetail := recover()
			log.L(brr.bl.ctx).Errorf("Observed panic: %v\n%s", panicDetail, debug.Stack())
			err = i18n.NewError(brr.bl.ctx, msgs.MsgObservedPanic, panicDetail)
		}
		brr.cb(receipts, err)
	}()
	rpc := brr.bl.backend

	if brr.bl.UseGetBlockReceipts {
		// just need to make a single call to get all the receipts
		rpcErr := rpc.CallRPC(brr.bl.ctx, &receipts, "eth_getBlockReceipts", brr.blockNumber)
		switch {
		case rpcErr != nil:
			err = rpcErr.Error()
		case receipts == nil:
			err = i18n.NewError(brr.bl.ctx, msgs.MsgBlockNotAvailable)
		default:
			// check the hash in all the receipts
			for _, r := range receipts {
				if brr.blockHash != nil && !r.BlockHash.Equals(brr.blockHash) {
					err = i18n.NewError(brr.bl.ctx, msgs.MsgReturnedBlockHashMismatch, brr.blockNumber.Uint64(), r.BlockHash, brr.blockHash)
					break
				}
			}
		}
	} else {
		// we don't currently optimize this branch, as all modern clients support eth_getBlockReceipts
		// and it seems well established that using that RPC is more efficient than attempting
		// parallelization or batching of eth_getTransactionReceipt calls.

		// Get the block by hash first
		var blockInfo *ethrpc.BlockInfoJSONRPC
		blockInfo, err = brr.bl.GetBlockInfoByHash(brr.bl.ctx, brr.blockHash.String())
		if err == nil && blockInfo == nil {
			err = i18n.NewError(brr.bl.ctx, msgs.MsgBlockNotAvailable)
		}
		if err == nil {
			// Then get each receipt
			receipts = make([]*ethrpc.TxReceiptJSONRPC, len(blockInfo.Transactions))
			for i := 0; i < len(receipts) && err == nil; i++ {
				receipts[i], err = brr.bl.GetTransactionReceipt(brr.bl.ctx, blockInfo.Transactions[i].String())
			}
		}

	}

	// No early return in this function - return must happen by reaching here
	earlyExit = false
}

// resetReceiptCache clears all cached transaction receipts when the canonical chain
// is rebuilt or re-initialized. Receipts are keyed only by transaction hash, so after
// a reorg the same hash can refer to a block that is no longer canonical.
//
// The generation counter invalidates any in-flight async fetches that complete after
// the reset, so they cannot repopulate the cache with orphaned data.
//
// Fork trims use invalidateReceiptsForForkTrim instead, which drops only receipts for
// the orphaned tail blocks and leaves the canonical prefix cache intact.
func (bl *blockListener) resetReceiptCache() {
	if bl.txReceiptCache == nil {
		return
	}
	bl.txReceiptCacheLock.Lock()
	defer bl.txReceiptCacheLock.Unlock()
	bl.resetReceiptCacheLocked()
}

// Caller MUST hold txReceiptCacheLock
func (bl *blockListener) resetReceiptCacheLocked() {
	bl.txReceiptCache.Purge()
	bl.txReceiptCacheGeneration++
	bl.invalidatedReceiptBlockHashes = make(map[string]struct{})
}

// invalidateReceiptsForForkTrim drops cached receipts for blocks removed from the
// canonical chain tail during a fork. Blocks indexed by the listener carry their
// transaction hashes, so entries are removed directly by tx hash. Trimmed block
// hashes are also recorded so in-flight async fetches cannot repopulate them.
func (bl *blockListener) invalidateReceiptsForForkTrim(trimmedBlocks []*ethrpc.BlockInfoJSONRPC) {
	if bl.txReceiptCache == nil || len(trimmedBlocks) == 0 {
		return
	}

	bl.txReceiptCacheLock.Lock()
	defer bl.txReceiptCacheLock.Unlock()

	// Check if the invalidated block hash set is too large, if so reset the cache
	// monitored head length, which is the deepest possible single fork trim. Entries are
	// only removed when a trimmed block becomes canonical again, so on a long-running
	// listener the set would otherwise grow with every fork trim. Hitting the bound
	// falls back to a full cache reset, which is always safe.
	// On a full rebuild of the canonical chain, the cache is reset anyway, so not
	// affected by this check.
	if len(bl.invalidatedReceiptBlockHashes)+len(trimmedBlocks) > bl.MonitoredHeadLength {
		bl.resetReceiptCacheLocked()
		return
	}
	for _, bi := range trimmedBlocks {
		if bi == nil || bi.Hash == nil {
			continue
		}
		bl.invalidatedReceiptBlockHashes[bi.Hash.String()] = struct{}{}
		for _, txHash := range bi.Transactions {
			bl.txReceiptCache.Remove(txHash.String())
		}
	}
}

func (bl *blockListener) revalidateReceiptBlockHash(blockHash ethtypes.HexBytes0xPrefix) {
	if bl.txReceiptCache == nil || blockHash == nil {
		return
	}
	bl.txReceiptCacheLock.Lock()
	delete(bl.invalidatedReceiptBlockHashes, blockHash.String())
	bl.txReceiptCacheLock.Unlock()
}

func (bl *blockListener) getReceiptCacheGeneration() uint64 {
	bl.txReceiptCacheLock.RLock()
	defer bl.txReceiptCacheLock.RUnlock()
	return bl.txReceiptCacheGeneration
}

// storeReceiptsInCache stores the receipts in the cache.
// The generation must match the current generation, otherwise the receipts are ignored.
// The blockHash is used to check if the receipts are for a block that is no longer canonical.
// If the blockHash is provided, the receipts are only stored if the block is not in the invalidatedReceiptBlockHashes map.
func (bl *blockListener) storeReceiptsInCache(receipts []*ethrpc.TxReceiptJSONRPC, generation uint64, blockHash ethtypes.HexBytes0xPrefix) {
	if bl.txReceiptCache == nil {
		return
	}
	bl.txReceiptCacheLock.Lock()
	defer bl.txReceiptCacheLock.Unlock()
	if generation != bl.txReceiptCacheGeneration {
		return
	}
	if blockHash != nil {
		if _, invalidated := bl.invalidatedReceiptBlockHashes[blockHash.String()]; invalidated {
			return
		}
	}
	for _, r := range receipts {
		if r != nil && r.TransactionHash != nil {
			if r.BlockHash != nil {
				if _, invalidated := bl.invalidatedReceiptBlockHashes[r.BlockHash.String()]; invalidated {
					continue
				}
			}
			bl.txReceiptCache.Add(r.TransactionHash.String(), r)
		}
	}
}

func (bl *blockListener) getCachedTransactionReceipt(txHash string) (*ethrpc.TxReceiptJSONRPC, bool) {
	if bl.txReceiptCache == nil {
		return nil, false
	}
	bl.txReceiptCacheLock.RLock()
	defer bl.txReceiptCacheLock.RUnlock()
	cached, ok := bl.txReceiptCache.Get(txHash)
	if !ok {
		return nil, false
	}
	return cached.(*ethrpc.TxReceiptJSONRPC), true
}

type pendingReceiptFetch struct {
	blockInfo  *ethrpc.BlockInfoJSONRPC
	generation uint64
}

// queueReceiptFetch records a block whose receipts should be fetched into the cache once
// the canonical chain lock is released. The block is canonical at this point, so its hash
// is revalidated and the cache generation snapshotted here, under the lock. If a fork trim
// removes the block again before the fetch completes, the hash is re-invalidated and
// storeReceiptsInCache discards the results.
//
// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) queueReceiptFetch(bi *ethrpc.BlockInfoJSONRPC) {
	if bl.txReceiptCache == nil {
		return
	}
	bl.revalidateReceiptBlockHash(bi.Hash)
	bl.pendingReceiptFetches = append(bl.pendingReceiptFetches, &pendingReceiptFetch{
		blockInfo:  bi,
		generation: bl.getReceiptCacheGeneration(),
	})
}

// Caller MUST hold the canonicalChain WRITE LOCK
func (bl *blockListener) queueReceiptFetchesForCanonicalChain() {
	if bl.txReceiptCache == nil {
		return
	}
	for pos := bl.canonicalChain.Front(); pos != nil; pos = pos.Next() {
		if pos.Value != nil {
			bl.queueReceiptFetch(pos.Value.(*ethrpc.BlockInfoJSONRPC))
		}
	}
}

// dispatchPendingReceiptFetches starts the async receipt fetches for blocks queued by
// queueReceiptFetch. FetchBlockReceiptsAsync blocks when the fetch concurrency throttle
// is saturated, so this must be called WITHOUT the canonical chain lock held, to avoid
// stalling readers of the chain.
func (bl *blockListener) dispatchPendingReceiptFetches(pending []*pendingReceiptFetch) {
	for _, f := range pending {
		bi := f.blockInfo
		generation := f.generation
		bl.FetchBlockReceiptsAsync(bi.Number.Uint64(), bi.Hash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
			if err != nil {
				log.L(bl.ctx).Debugf("Failed to fetch receipts for block %d / %s: %v", bi.Number.Uint64(), bi.Hash, err)
				return
			}
			bl.storeReceiptsInCache(receipts, generation, bi.Hash)
		})
	}
}
