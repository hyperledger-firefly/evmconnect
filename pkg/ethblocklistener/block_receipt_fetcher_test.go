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

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFetchBlockReceiptsAsyncOptimizedOk(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	receipt := &ethrpc.TxReceiptJSONRPC{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String()),
		BlockHash:       blockHash,
	}

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, blockNumber, args[3])
		res := args[1].(*[]*ethrpc.TxReceiptJSONRPC)
		*res = []*ethrpc.TxReceiptJSONRPC{receipt}
	})

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.NoError(t, err)
		assert.Equal(t, []*ethrpc.TxReceiptJSONRPC{receipt}, receipts)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncOptimizedNotFound(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).Return(nil)

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.Regexp(t, "FF23011", err)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncOptimizedBlockMismatch(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, blockNumber, args[3])
		res := args[1].(*[]*ethrpc.TxReceiptJSONRPC)
		*res = []*ethrpc.TxReceiptJSONRPC{
			{
				TransactionHash: ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String()),
				BlockHash:       ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String()),
			},
		}
	})

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.Regexp(t, "FF23068.*"+blockHash.String(), err)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncOptimizedBlockHandleError(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).
		Return(rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, i18n.Msg404NotFound))

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.Regexp(t, "FF00167", err)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncOptimizedBlockHandlePanic(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).Panic("pop")

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.Regexp(t, "FF23067.*pop", err)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncNonOptimizedOk(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)
	txHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())

	block := &ethrpc.EVMBlockWithTxHashesJSONRPC{
		BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number: blockNumber,
			Hash:   blockHash,
		},
		Transactions: []ethtypes.HexBytes0xPrefix{txHash},
	}

	receipt := &ethrpc.TxReceiptJSONRPC{
		TransactionHash: txHash,
		BlockHash:       blockHash,
	}

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", mock.Anything, false).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, blockHash.String(), args[3])
		res := args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC)
		*res = block
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, txHash.String(), args[3])
		res := args[1].(**ethrpc.TxReceiptJSONRPC)
		*res = receipt
	})

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.NoError(t, err)
		assert.Equal(t, []*ethrpc.TxReceiptJSONRPC{receipt}, receipts)
	})
	<-fetched
}

func TestFetchBlockReceiptsAsyncNonOptimizedNotFound(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	blockHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	blockNumber := ethtypes.HexUint64(12346)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", mock.Anything, false).Return(nil)

	fetched := make(chan struct{})
	bl.FetchBlockReceiptsAsync(blockNumber.Uint64(), blockHash, func(receipts []*ethrpc.TxReceiptJSONRPC, err error) {
		defer close(fetched)
		assert.Regexp(t, "FF23011", err)
	})
	<-fetched
}

func TestGetTransactionReceiptUsesCache(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	txHash := "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6"
	cachedReceipt := &ethrpc.TxReceiptJSONRPC{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
		BlockNumber:     ethtypes.HexUint64(1977),
		BlockHash:       generateTestHash(1977),
	}

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{cachedReceipt}, bl.getReceiptCacheGeneration())

	receipt, err := bl.GetTransactionReceipt(context.Background(), txHash)
	assert.NoError(t, err)
	assert.Equal(t, cachedReceipt, receipt)
	mRPC.AssertNotCalled(t, "CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", mock.Anything)
}

func TestResetReceiptCacheClearsCachedReceipts(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	txHash := "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6"
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash)},
	}, bl.getReceiptCacheGeneration())

	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.True(t, ok)

	bl.resetReceiptCache()

	_, ok = bl.getCachedTransactionReceipt(txHash)
	assert.False(t, ok)
}

func TestStoreReceiptsInCacheIgnoresStaleGeneration(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	gen := bl.getReceiptCacheGeneration()
	bl.resetReceiptCache()

	txHash := "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6"
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash)},
	}, gen)

	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.False(t, ok)
}

func TestReconcileConfirmationsForTransactionUsesCachedReceipt(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()
	bl.canonicalChain = createTestChain(1976, 1978)

	txHash := "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6"
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{
			TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
			BlockNumber:     ethtypes.HexUint64(1977),
			BlockHash:       generateTestHash(1977),
		},
	}, bl.getReceiptCacheGeneration())

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     1977,
			Hash:       generateTestHash(1977),
			ParentHash: generateTestHash(1976),
		}}
	})

	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), txHash, []*ethrpc.MinimalBlockInfo{}, 5)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, receipt)
	assert.Equal(t, ethtypes.HexUint64(1977), receipt.BlockNumber)
	mRPC.AssertNotCalled(t, "CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", mock.Anything)
}

func TestReceiptCacheEvictsWhenFull(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.ReceiptCacheEnabled = true
		conf.ReceiptCacheSize = 2
	})
	defer done()

	gen := bl.getReceiptCacheGeneration()
	txHash1 := "0x1111111111111111111111111111111111111111111111111111111111111111"
	txHash2 := "0x2222222222222222222222222222222222222222222222222222222222222222"
	txHash3 := "0x3333333333333333333333333333333333333333333333333333333333333333"

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash1)},
	}, gen)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash2)},
	}, gen)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash3)},
	}, gen)

	_, ok := bl.getCachedTransactionReceipt(txHash1)
	assert.False(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash2)
	assert.True(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash3)
	assert.True(t, ok)
}
