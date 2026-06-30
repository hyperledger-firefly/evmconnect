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

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
)

func (bl *blockListener) addToBlockCache(blockInfo *ethrpc.BlockInfoJSONRPC) {
	bl.blockCache.Add(blockInfo.Hash.String(), blockInfo)
	bl.blockCache.Add(blockInfo.Number.String(), blockInfo)
}

func (bl *blockListener) getReceiptAndBlock(ctx context.Context, txHash string) (*ethrpc.BlockInfoJSONRPC, *ethrpc.TxReceiptJSONRPC, error) {

	// Query the chain to find the transaction block
	receipt, receiptErr := bl.GetTransactionReceipt(ctx, txHash)
	if receiptErr != nil {
		return nil, nil, i18n.WrapError(ctx, receiptErr, msgs.MsgFailedToQueryReceipt, txHash)
	}
	if receipt == nil {
		return nil, nil, nil
	}
	txBlockHash := receipt.BlockHash
	txBlockNumber := receipt.BlockNumber.Uint64()
	// get the parent hash of the transaction block
	bi, err := bl.GetBlockInfoByNumber(ctx, txBlockNumber, true, "", txBlockHash.String())
	if err != nil {
		return nil, nil, i18n.WrapError(ctx, err, msgs.MsgFailedToQueryBlockInfo, txHash)
	}
	if bi == nil {
		// if the block info is not found, then there could be a fork, twe don't throw error in this case and treating it as block not found
		return nil, nil, nil
	}

	return bi, receipt, nil
}

func (bl *blockListener) GetTransactionReceipt(ctx context.Context, txHash string) (ethReceipt *ethrpc.TxReceiptJSONRPC, err error) {
	if receipt, ok := bl.getCachedTransactionReceipt(txHash); ok {
		return receipt, nil
	}
	rpcErr := bl.backend.CallRPC(ctx, &ethReceipt, "eth_getTransactionReceipt", txHash)
	if rpcErr != nil || ethReceipt == nil {
		var err error
		if rpcErr != nil {
			err = rpcErr.Error()
		}
		return nil, err
	}
	return ethReceipt, nil
}

func (bl *blockListener) GetBlockInfoByNumber(ctx context.Context, blockNumber uint64, allowCache bool, expectedParentHashStr string, expectedBlockHashStr string) (*ethrpc.BlockInfoJSONRPC, error) {
	hexBlockNumber := ethtypes.HexUint64(blockNumber)
	var blockInfo *ethrpc.BlockInfoJSONRPC
	if allowCache {
		cached, ok := bl.blockCache.Get(hexBlockNumber.String())
		if ok {
			blockInfo = cached.(*ethrpc.BlockInfoJSONRPC)
			if (expectedParentHashStr != "" && blockInfo.ParentHash.String() != expectedParentHashStr) || (expectedBlockHashStr != "" && blockInfo.Hash.String() != expectedBlockHashStr) {
				log.L(ctx).Debugf("Block cache miss for block %d due to mismatched parent hash expected=%s found=%s", blockNumber, expectedParentHashStr, blockInfo.ParentHash)
				blockInfo = nil
			}
		}
	}

	if blockInfo == nil {
		b, err := bl.GetEVMBlockWithTxHashesByNumber(ctx, hexBlockNumber.String())
		if err != nil {
			return nil, err
		}
		blockInfo = b.ToBlockInfo(bl.IncludeLogsBloom)
	}

	return blockInfo, nil
}

func (bl *blockListener) GetBlockInfoByHash(ctx context.Context, hash0xString string) (*ethrpc.BlockInfoJSONRPC, error) {
	var blockInfo *ethrpc.BlockInfoJSONRPC // the minimal set we cache
	cached, ok := bl.blockCache.Get(hash0xString)
	if ok {
		blockInfo = cached.(*ethrpc.BlockInfoJSONRPC)
	}

	if blockInfo == nil {
		b, err := bl.GetEVMBlockWithTxHashesByHash(ctx, hash0xString)
		if err != nil {
			return nil, err
		}
		blockInfo = b.ToBlockInfo(bl.IncludeLogsBloom)
	}

	return blockInfo, nil
}

// Does not use cache, but will add to cache
func (bl *blockListener) GetEVMBlockWithTxHashesByHash(ctx context.Context, hash0xString string) (b *ethrpc.EVMBlockWithTxHashesJSONRPC, err error) {
	rpcErr := bl.backend.CallRPC(ctx, &b, "eth_getBlockByHash", hash0xString, false /* only the txn hashes */)
	if rpcErr != nil {
		return nil, rpcErr.Error()
	}
	if b != nil {
		bl.addToBlockCache(b.ToBlockInfo(bl.IncludeLogsBloom))
	}
	return b, nil
}

// Does not use cache, but will add to cache
func (bl *blockListener) GetEVMBlockWithTransactionsByHash(ctx context.Context, hash0xString string) (b *ethrpc.EVMBlockWithTransactionsJSONRPC, err error) {
	rpcErr := bl.backend.CallRPC(ctx, &b, "eth_getBlockByHash", hash0xString, true /* full blocks */)
	if rpcErr != nil {
		return nil, rpcErr.Error()
	}
	if b != nil {
		bl.addToBlockCache(b.ToBlockInfo(bl.IncludeLogsBloom))
	}
	return b, nil
}

// Does not use cache, but will add to cache
func (bl *blockListener) GetEVMBlockWithTxHashesByNumber(ctx context.Context, numberLookup string) (b *ethrpc.EVMBlockWithTxHashesJSONRPC, err error) {
	rpcErr := bl.backend.CallRPC(ctx, &b, "eth_getBlockByNumber", numberLookup, false /* only the txn hashes */)
	if rpcErr != nil {
		return nil, rpcErr.Error()
	}
	if b != nil {
		bl.addToBlockCache(b.ToBlockInfo(bl.IncludeLogsBloom))
	}
	return b, nil
}

// Does not use cache, but will add to cache
func (bl *blockListener) GetEVMBlockWithTransactionsByNumber(ctx context.Context, numberLookup string) (b *ethrpc.EVMBlockWithTransactionsJSONRPC, err error) {
	rpcErr := bl.backend.CallRPC(ctx, &b, "eth_getBlockByNumber", numberLookup, true /* full blocks */)
	if rpcErr != nil {
		return nil, rpcErr.Error()
	}
	if b != nil {
		bl.addToBlockCache(b.ToBlockInfo(bl.IncludeLogsBloom))
	}
	return b, nil
}
