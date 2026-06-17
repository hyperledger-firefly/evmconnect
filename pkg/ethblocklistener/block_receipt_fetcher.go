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

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-evmconnect/internal/msgs"
	"github.com/hyperledger/firefly-evmconnect/pkg/ethrpc"
	"github.com/hyperledger/firefly-signer/pkg/ethtypes"
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

func (bl *blockListener) resetReceiptCache() {
	bl.txReceiptCacheLock.Lock()
	defer bl.txReceiptCacheLock.Unlock()
	bl.txReceiptCache.Purge()
	bl.txReceiptCacheGeneration++
}

func (bl *blockListener) getReceiptCacheGeneration() uint64 {
	bl.txReceiptCacheLock.RLock()
	defer bl.txReceiptCacheLock.RUnlock()
	return bl.txReceiptCacheGeneration
}

func (bl *blockListener) storeReceiptsInCache(receipts []*ethrpc.TxReceiptJSONRPC, generation uint64) {
	bl.txReceiptCacheLock.Lock()
	defer bl.txReceiptCacheLock.Unlock()
	if generation != bl.txReceiptCacheGeneration {
		return
	}
	for _, r := range receipts {
		if r != nil && r.TransactionHash != nil {
			bl.txReceiptCache.Add(r.TransactionHash.String(), r)
		}
	}
}

func (bl *blockListener) getCachedTransactionReceipt(txHash string) (*ethrpc.TxReceiptJSONRPC, bool) {
	bl.txReceiptCacheLock.RLock()
	defer bl.txReceiptCacheLock.RUnlock()
	cached, ok := bl.txReceiptCache.Get(txHash)
	if !ok {
		return nil, false
	}
	return cached.(*ethrpc.TxReceiptJSONRPC), true
}

func (bl *blockListener) fetchAndCacheBlockReceipts(bi *ethrpc.BlockInfoJSONRPC) {
	generation := bl.getReceiptCacheGeneration()
	bl.FetchBlockReceiptsAsync(bi.Number.Uint64(), bi.Hash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		if err != nil {
			log.L(bl.ctx).Debugf("Failed to fetch receipts for block %d / %s: %v", bi.Number.Uint64(), bi.Hash, err)
			return
		}
		bl.storeReceiptsInCache(receipts, generation)
	})
}

func (bl *blockListener) refetchReceiptsForCanonicalChain() {
	for pos := bl.canonicalChain.Front(); pos != nil; pos = pos.Next() {
		if pos.Value != nil {
			bl.fetchAndCacheBlockReceipts(pos.Value.(*ethrpc.BlockInfoJSONRPC))
		}
	}
}
