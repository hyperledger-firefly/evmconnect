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
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{cachedReceipt}, bl.getReceiptCacheGeneration(), cachedReceipt.BlockHash)

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
	}, bl.getReceiptCacheGeneration(), nil)

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
	}, gen, nil)

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
	}, bl.getReceiptCacheGeneration(), generateTestHash(1977))

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
	}, gen, nil)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash2)},
	}, gen, nil)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{
		{TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash3)},
	}, gen, nil)

	_, ok := bl.getCachedTransactionReceipt(txHash1)
	assert.False(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash2)
	assert.True(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash3)
	assert.True(t, ok)
}

func TestInvalidateReceiptsForForkTrimRemovesOnlyTrimmedBlocks(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	gen := bl.getReceiptCacheGeneration()
	txHash100 := "0x1000000000000000000000000000000000000000000000000000000000000001"
	txHash101 := "0x1010000000000000000000000000000000000000000000000000000000000001"
	txHash102 := "0x1020000000000000000000000000000000000000000000000000000000000001"
	blockHash100 := generateTestHash(100)
	blockHash101 := generateTestHash(101)
	blockHash102 := generateTestHash(102)

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash100),
		BlockHash:       blockHash100,
	}}, gen, blockHash100)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash101),
		BlockHash:       blockHash101,
	}}, gen, blockHash101)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash102),
		BlockHash:       blockHash102,
	}}, gen, blockHash102)

	bl.invalidateReceiptsForForkTrim([]*ethrpc.BlockInfoJSONRPC{
		{Hash: blockHash101, Number: 101, Transactions: []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash101)}},
		{Hash: blockHash102, Number: 102, Transactions: []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash102)}},
	})

	_, ok := bl.getCachedTransactionReceipt(txHash100)
	assert.True(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash101)
	assert.False(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash102)
	assert.False(t, ok)
	assert.Equal(t, gen, bl.getReceiptCacheGeneration())
}

func TestInvalidateReceiptsForForkTrimFallsBackToResetAtBound(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	gen := bl.getReceiptCacheGeneration()
	txHash := "0x1000000000000000000000000000000000000000000000000000000000000001"
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
	}}, gen, nil)

	trimmed := make([]*ethrpc.BlockInfoJSONRPC, bl.MonitoredHeadLength+1)
	for i := range trimmed {
		trimmed[i] = &ethrpc.BlockInfoJSONRPC{Hash: generateTestHash(uint64(i)), Number: ethtypes.HexUint64(i)}
	}
	bl.invalidateReceiptsForForkTrim(trimmed)

	// The whole cache is reset rather than tracking an unbounded invalidation set
	assert.Equal(t, gen+1, bl.getReceiptCacheGeneration())
	assert.Empty(t, bl.invalidatedReceiptBlockHashes)
	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.False(t, ok)
}

func TestStoreReceiptsInCacheIgnoresInvalidatedForkTrimBlock(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	blockHash := generateTestHash(102)
	txHash := "0x1020000000000000000000000000000000000000000000000000000000000001"
	bl.invalidateReceiptsForForkTrim([]*ethrpc.BlockInfoJSONRPC{
		{Hash: blockHash, Number: 102},
	})

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
		BlockHash:       blockHash,
	}}, bl.getReceiptCacheGeneration(), blockHash)

	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.False(t, ok)
}

func TestRevalidateReceiptBlockHashAllowsCachingAgain(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	defer done()

	blockHash := generateTestHash(101)
	txHash := "0x1010000000000000000000000000000000000000000000000000000000000001"
	bl.invalidateReceiptsForForkTrim([]*ethrpc.BlockInfoJSONRPC{
		{
			Hash:         blockHash,
			Number:       101,
			Transactions: []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash)},
		},
	})

	bl.revalidateReceiptBlockHash(blockHash)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
		BlockHash:       blockHash,
	}}, bl.getReceiptCacheGeneration(), blockHash)

	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.True(t, ok)
}

func TestQueuedReceiptFetchRevalidatesPreviouslyInvalidatedBlock(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	blockHash := generateTestHash(101)
	txHash := "0x1010000000000000000000000000000000000000000000000000000000000001"
	bl.invalidateReceiptsForForkTrim([]*ethrpc.BlockInfoJSONRPC{
		{
			Hash:         blockHash,
			Number:       101,
			Transactions: []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash)},
		},
	})

	receipt := &ethrpc.TxReceiptJSONRPC{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash),
		BlockHash:       blockHash,
	}
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockReceipts", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.TxReceiptJSONRPC) = []*ethrpc.TxReceiptJSONRPC{receipt}
	})

	bl.canonicalChainLock.Lock()
	bl.queueReceiptFetch(&ethrpc.BlockInfoJSONRPC{
		Number: 101,
		Hash:   blockHash,
	})
	pending := bl.pendingReceiptFetches
	bl.pendingReceiptFetches = nil
	bl.canonicalChainLock.Unlock()
	bl.dispatchPendingReceiptFetches(pending)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := bl.getCachedTransactionReceipt(txHash); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, ok := bl.getCachedTransactionReceipt(txHash)
	assert.True(t, ok)
}

func TestHandleNewBlockForkTrimPreservesPrefixReceiptCache(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.UseGetBlockReceipts = true
	})
	defer done()

	bl.canonicalChain = createTestChain(100, 102)
	gen := bl.getReceiptCacheGeneration()
	txHash100 := "0x1000000000000000000000000000000000000000000000000000000000000001"
	txHash101 := "0x1010000000000000000000000000000000000000000000000000000000000001"
	txHash102 := "0x1020000000000000000000000000000000000000000000000000000000000001"
	blockHash100 := generateTestHash(100)
	blockHash101 := generateTestHash(101)
	blockHash102 := generateTestHash(102)

	for pos := bl.canonicalChain.Front(); pos != nil; pos = pos.Next() {
		bi := pos.Value.(*ethrpc.BlockInfoJSONRPC)
		switch bi.Number.Uint64() {
		case 100:
			bi.Transactions = []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash100)}
		case 101:
			bi.Transactions = []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash101)}
		case 102:
			bi.Transactions = []ethtypes.HexBytes0xPrefix{ethtypes.MustNewHexBytes0xPrefix(txHash102)}
		}
	}

	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash100),
		BlockHash:       blockHash100,
	}}, gen, blockHash100)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash101),
		BlockHash:       blockHash101,
	}}, gen, blockHash101)
	bl.storeReceiptsInCache([]*ethrpc.TxReceiptJSONRPC{{
		TransactionHash: ethtypes.MustNewHexBytes0xPrefix(txHash102),
		BlockHash:       blockHash102,
	}}, gen, blockHash102)

	forkBlock := &ethrpc.BlockInfoJSONRPC{
		Number:     101,
		Hash:       generateTestHash(101, 1),
		ParentHash: blockHash100,
	}

	bl.canonicalChainLock.Lock()
	bl.handleNewBlock(forkBlock, bl.canonicalChain.Front())
	// The receipts of the new fork block are queued for fetching, not dispatched under the lock
	require.Len(t, bl.pendingReceiptFetches, 1)
	assert.Equal(t, forkBlock, bl.pendingReceiptFetches[0].blockInfo)
	bl.pendingReceiptFetches = nil
	bl.canonicalChainLock.Unlock()

	_, ok := bl.getCachedTransactionReceipt(txHash100)
	assert.True(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash101)
	assert.False(t, ok)
	_, ok = bl.getCachedTransactionReceipt(txHash102)
	assert.False(t, ok)
	assert.Equal(t, gen, bl.getReceiptCacheGeneration())
}
