// Copyright © 2025 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	lru "github.com/hashicorp/golang-lru"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleJSONRPCReceipt = `{
	"blockHash": "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
	"blockNumber": "0x7b9",
	"contractAddress": "0x87ae94ab290932c4e6269648bb47c86978af4436",
	"cumulativeGasUsed": "0x8414",
	"effectiveGasPrice": "0x0",
	"from": "0x2b1c769ef5ad304a4889f2a07a6617cd935849ae",
	"gasUsed": "0x8414",
	"logs": [
	{
		"address": "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3",
		"topics": [
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x0000000000000000000000005dae1910885cde875de559333d12722357e69c42"
		],
		"data": "0x000000000000000000000000000000000000000000000000016345785d8a0000",
		"blockNumber": "0x5",
		"transactionHash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
		"transactionIndex": "0x0",
		"blockHash": "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
		"logIndex": "0x0",
		"removed": false
	}
	],
	"logsBloom": "0x00000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000100000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000",
	"status": "0x1",
	"to": "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3",
	"transactionHash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
	"transactionIndex": "0x1e",
	"type": "0x0"
}`

// Transaction hash in sampleJSONRPCReceipt (block 0x7b9 = 1977).
const headModeSampleTxHash = "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2"

func headBlockNumberTestConf(conf *BlockListenerConfig, _ *rpcbackendmocks.Backend, _ context.CancelFunc) {
	conf.ChainTrackingMode = ffcapi.ChainTrackingModeLight
}

func mockHeadModeReceipt(t *testing.T, mRPC *rpcbackendmocks.Backend, txHash string) {
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", txHash).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})
}

// Tests of the reconcileConfirmationsForTransaction function

func TestReconcileConfirmationsForTransaction_TransactionNotFound(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	// Mock for TransactionReceipt call - return nil to simulate transaction not found
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", generateTestHash(100).String()).Return(nil).Run(func(args mock.Arguments) {
		err := json.Unmarshal([]byte("null"), args[1])
		assert.NoError(t, err)
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, _, err := bl.ReconcileConfirmationsForTransaction(context.Background(), generateTestHash(100).String(), nil, 5)

	// Assertions - expect an error when transaction doesn't exist
	assert.Error(t, err)
	assert.Regexp(t, "FF23061", err)
	assert.Nil(t, result)

	mRPC.AssertExpectations(t)
}

func TestReconcileConfirmationsForTransaction_HeadBlockNumber_ChainHeadBehindReceipt(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, headBlockNumberTestConf)
	defer done()

	mockHeadModeReceipt(t, mRPC, headModeSampleTxHash)
	bl.currentChainHead = 1976 // receipt block is 1977 (0x7b9)

	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), headModeSampleTxHash, nil, 5)
	assert.Error(t, err)
	assert.Regexp(t, "FF23070", err)
	assert.Nil(t, result)
	assert.Nil(t, receipt)
}

func TestReconcileConfirmationsForTransaction_HeadBlockNumber_PartialConfirmations(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, headBlockNumberTestConf)
	defer done()

	mockHeadModeReceipt(t, mRPC, headModeSampleTxHash)
	bl.currentChainHead = 1980 // 1980 - 1977 = 3 confirmations vs target 5

	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), headModeSampleTxHash, nil, 5)
	assert.NoError(t, err)
	if assert.NotNil(t, receipt) {
		assert.Equal(t, uint64(1977), receipt.BlockNumber.Uint64())
	}
	if assert.NotNil(t, result) {
		assert.False(t, result.Confirmed)
		assert.Equal(t, uint64(3), result.CurrentConfirmationCount)
		assert.Equal(t, uint64(5), result.TargetConfirmationCount)
	}
}

func TestReconcileConfirmationsForTransaction_HeadBlockNumber_FullyConfirmed(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, headBlockNumberTestConf)
	defer done()

	mockHeadModeReceipt(t, mRPC, headModeSampleTxHash)
	bl.currentChainHead = 1982 // 1982 - 1977 = 5 confirmations

	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), headModeSampleTxHash, nil, 5)
	assert.NoError(t, err)
	if assert.NotNil(t, receipt) && assert.NotNil(t, result) {
		assert.True(t, result.Confirmed)
		assert.Equal(t, uint64(5), result.CurrentConfirmationCount)
		assert.Equal(t, uint64(5), result.TargetConfirmationCount)
	}
}

func TestReconcileConfirmationsForTransaction_HeadBlockNumber_ActualCountCappedAtTarget(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, headBlockNumberTestConf)
	defer done()

	mockHeadModeReceipt(t, mRPC, headModeSampleTxHash)
	bl.currentChainHead = 2500 // many blocks past receipt; cap at target

	result, _, err := bl.ReconcileConfirmationsForTransaction(context.Background(), headModeSampleTxHash, nil, 3)
	assert.NoError(t, err)
	if assert.NotNil(t, result) {
		assert.True(t, result.Confirmed)
		assert.Equal(t, uint64(3), result.CurrentConfirmationCount)
		assert.Equal(t, uint64(3), result.TargetConfirmationCount)
	}
}

func TestReconcileConfirmationsForTransaction_HeadBlockNumber_ReceiptRPCError(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, headBlockNumberTestConf)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", headModeSampleTxHash).
		Return(&rpcbackend.RPCError{Message: "pop"}).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte("null"), args[1])
			assert.NoError(t, err)
		})

	bl.currentChainHead = 2000

	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), headModeSampleTxHash, nil, 5)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Nil(t, receipt)
}

func TestReconcileConfirmationsForTransaction_ReceiptRPCCallError(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	// Mock for TransactionReceipt call - return error to simulate RPC call error
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", generateTestHash(100).String()).Return(&rpcbackend.RPCError{Message: "pop"}).Run(func(args mock.Arguments) {
		err := json.Unmarshal([]byte("null"), args[1])
		assert.NoError(t, err)
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, _, err := bl.ReconcileConfirmationsForTransaction(context.Background(), generateTestHash(100).String(), []*ethrpc.MinimalBlockInfo{}, 5)

	// Assertions - expect an error when RPC call fails
	assert.Error(t, err)
	assert.Nil(t, result)
}

const wrongBlockNumber uint64 = 88888888

func TestReconcileConfirmationsForTransaction_BlockNotFound(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	// Mock for TransactionReceipt call
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(nil).Run(func(args mock.Arguments) {
		err := json.Unmarshal([]byte("null"), args[1])
		assert.NoError(t, err)
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, _, err := bl.ReconcileConfirmationsForTransaction(context.Background(), "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
		toMinimalBlockInfoList([]*ethrpc.BlockInfoJSONRPC{
			{Number: 1977, Hash: generateTestHash(1977), ParentHash: generateTestHash(1976)},
		}), 5)

	// Assertions - expect an error when transaction doesn't exist
	assert.Error(t, err)
	assert.Regexp(t, "FF23061", err)
	assert.Nil(t, result)

	mRPC.AssertExpectations(t)
}

func TestReconcileConfirmationsForTransaction_BlockRPCCallError(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(&rpcbackend.RPCError{Message: "pop"})

	// Execute the reconcileConfirmationsForTransaction function
	result, _, err := bl.ReconcileConfirmationsForTransaction(context.Background(), "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6", []*ethrpc.MinimalBlockInfo{}, 5)

	// Assertions - expect an error when RPC call fails
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestReconcileConfirmationsForTransaction_TxBlockNotInCanonicalChain(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()
	bl.canonicalChain = createTestChain(1976, 1978) // Single block at 50, tx is at 100

	// Mock for TransactionReceipt call
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	fakeParentHash := fftypes.NewRandB32().String()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     1977,
			Hash:       generateTestHash(1977),
			ParentHash: ethtypes.MustNewHexBytes0xPrefix(fakeParentHash),
		}}
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6", []*ethrpc.MinimalBlockInfo{}, 5)

	// Assertions - expect the transaction block to be returned
	// we trust the block retrieve by getBlockInfoContainsTxHash function more than the canonical chain
	// and we allow the canonical chain to be updated at its own pace
	// therefore, if the tx block is different from the block of same number in the canonical chain, we should return the tx block for now
	// and wait for the canonical chain to be updated
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.NewFork)
	assert.False(t, result.Confirmed)
	assert.Len(t, result.Confirmations, 2)
	assert.Equal(t, uint64(5), result.TargetConfirmationCount)
	assert.NotNil(t, receipt)
	mRPC.AssertExpectations(t)
}

func TestReconcileConfirmationsForTransaction_NewConfirmation(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()
	bl.canonicalChain = createTestChain(1976, 1978) // Single block at 50, tx is at 100

	// Mock for TransactionReceipt call
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     1977,
			Hash:       generateTestHash(1977),
			ParentHash: generateTestHash(1976),
		}}
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6", []*ethrpc.MinimalBlockInfo{}, 5)

	// Assertions - expect the existing confirmation queue to be returned because the tx block doesn't match the same block number in the canonical chain
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.NewFork)
	assert.False(t, result.Confirmed)
	assert.Equal(t, toMinimalBlockInfoList([]*ethrpc.BlockInfoJSONRPC{
		{Number: 1977, Hash: generateTestHash(1977), ParentHash: generateTestHash(1976)},
		{Number: 1978, Hash: generateTestHash(1978), ParentHash: generateTestHash(1977)},
	}), result.Confirmations)
	assert.Equal(t, uint64(5), result.TargetConfirmationCount)
	assert.NotNil(t, receipt)

	mRPC.AssertExpectations(t)
}

func TestReconcileConfirmationsForTransaction_DifferentTxBlock(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()
	bl.canonicalChain = createTestChain(1976, 1978) // Single block at 50, tx is at 100

	// Mock for TransactionReceipt call
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x7b9", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     1977,
			Hash:       generateTestHash(1977),
			ParentHash: generateTestHash(1976),
		}}
	})

	// Execute the reconcileConfirmationsForTransaction function
	result, receipt, err := bl.ReconcileConfirmationsForTransaction(context.Background(), "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
		toMinimalBlockInfoList([]*ethrpc.BlockInfoJSONRPC{
			{Number: 1979, Hash: generateTestHash(1979), ParentHash: generateTestHash(1978)},
			{Number: 1980, Hash: generateTestHash(1980), ParentHash: generateTestHash(1979)},
		}), 5)

	// Assertions - expect the existing confirmation queue to be returned because the tx block doesn't match the same block number in the canonical chain
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.NewFork)
	assert.False(t, result.Confirmed)
	assert.Equal(t, toMinimalBlockInfoList([]*ethrpc.BlockInfoJSONRPC{
		{Number: 1977, Hash: generateTestHash(1977), ParentHash: generateTestHash(1976)},
		{Number: 1978, Hash: generateTestHash(1978), ParentHash: generateTestHash(1977)},
	}), result.Confirmations)
	assert.Equal(t, uint64(5), result.TargetConfirmationCount)
	assert.NotNil(t, receipt)
	mRPC.AssertExpectations(t)
}

func TestBuildConfirmationList_GapInExistingConfirmationsShouldBeFilledIn(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{101})
	defer done()
	ctx := context.Background()

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		// gap in the existing confirmations list
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)

	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))

}

func TestBuildConfirmationList_EarlyListDrainsToEmptyDueToMismatch(t *testing.T) {
	ctx := context.Background()

	const altBlock = 0xffff

	chain := list.New()
	chain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(100),
		Hash:       generateTestHash(100),
		ParentHash: generateTestHash(99),
	})
	mRPC, bl := newFakedChainBlockListener(t, chain)
	mockBlockRange(mRPC, 100, 105, altBlock)

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	_, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.Regexp(t, "FF23060", err)
}

func TestBuildConfirmationList_EarlyFillInFail(t *testing.T) {
	ctx := context.Background()

	chain := list.New()
	chain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(100),
		Hash:       generateTestHash(100),
		ParentHash: generateTestHash(99),
	})
	mRPC, bl := newFakedChainBlockListener(t, chain)
	mockBlockRange(mRPC, 102, 105)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x65", false).
		Return(&rpcbackend.RPCError{Message: "pop"})

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		// gap in the existing confirmations list
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	_, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.Regexp(t, "pop", err)
}

func TestBuildConfirmationList_EarlyFillInInvalid(t *testing.T) {
	ctx := context.Background()

	chain := list.New()
	chain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(100),
		Hash:       generateTestHash(100),
		ParentHash: generateTestHash(99),
	})
	mRPC, bl := newFakedChainBlockListener(t, chain)
	mockBlockRange(mRPC, 102, 105)

	var altBlock uint64 = 0xffff
	mockBlockRange(mRPC, 101, 101, altBlock)

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		// gap in the existing confirmations list
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	_, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.Regexp(t, "FF23060", err)
}

func TestBuildConfirmationList_MismatchConfirmationBlockShouldBeReplaced(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{101})
	defer done()
	ctx := context.Background()

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(999), Number: 101, ParentHash: generateTestHash(100)}, // wrong hash and is a fork
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		{Hash: generateTestHash(103), Number: 103, ParentHash: generateTestHash(102)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)

	// Assert
	assert.True(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ExistingTxBockInfoIsWrongShouldBeIgnored(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{101})
	defer done()
	ctx := context.Background()

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(999), Number: 100, ParentHash: generateTestHash(99)}, // incorrect block number
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestReconcileConfirmationsForTransaction_ExistingConfirmationsWithLowerBlockNumberShouldBeIgnored(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{101})
	defer done()
	ctx := context.Background()

	// Create corrupted confirmation (gap in the existing confirmations list)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 99, ParentHash: generateTestHash(100)}, // somehow there is a lower block number
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

// Tests of the buildConfirmationList function

func TestBuildConfirmationList_EmptyChain(t *testing.T) {
	// Setup - create a chain with one block that's older than the transaction
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 50, []uint64{})
	defer done()
	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)

	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	// Assert - should return early due to chain being too short
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, []*ethrpc.BlockInfoJSONRPC{}, txBlockInfo, targetConfirmationCount)
	assert.Error(t, err)
	assert.Regexp(t, "FF23062", err.Error())
	assert.Nil(t, confirmationUpdateResult)
}

func TestBuildConfirmationQueueUsingInMemoryPartialChain_EmptyCanonicalChain(t *testing.T) {
	// Setup - create a blockListener with an empty canonical chain
	mRPC := &rpcbackendmocks.Backend{}
	bl := &blockListener{
		canonicalChain: list.New(), // Empty canonical chain
		backend:        mRPC,
	}
	bl.blockCache, _ = lru.New(100)

	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)

	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute - should return error when canonical chain is empty
	_, err := bl.buildConfirmationQueueUsingInMemoryPartialChain(ctx, txBlockInfo, targetConfirmationCount)

	// Assert - expect error with code FF23062 for empty canonical chain
	assert.Error(t, err)
	assert.Regexp(t, "FF23062", err.Error())
	mRPC.AssertExpectations(t)
}

func TestHandleZeroTargetConfirmationCount_EmptyCanonicalChain(t *testing.T) {
	// Setup - create a blockListener with an empty canonical chain
	mRPC := &rpcbackendmocks.Backend{}
	bl := &blockListener{
		canonicalChain: list.New(), // Empty canonical chain
		backend:        mRPC,
	}
	bl.blockCache, _ = lru.New(100)

	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)

	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}

	// Execute - should return error when canonical chain is empty
	result, err := bl.handleZeroTargetConfirmationCount(ctx, txBlockInfo)

	// Assert - expect error with code FF23062 for empty canonical chain
	assert.Error(t, err)
	assert.Regexp(t, "FF23062", err.Error())
	assert.Nil(t, result)
	mRPC.AssertExpectations(t)
}

func TestBuildConfirmationList_ChainTooShort(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 99, []uint64{})
	defer done()
	ctx := context.Background()

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)

	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, []*ethrpc.BlockInfoJSONRPC{}, txBlockInfo, targetConfirmationCount)
	assert.Error(t, err)
	assert.Nil(t, confirmationUpdateResult)
}

func TestBuildConfirmationList_NilConfirmationMap(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, nil, txBlockInfo, targetConfirmationCount)
	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, confirmationUpdateResult)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))

}

func TestBuildConfirmationList_NilConfirmationMap_ZeroConfirmationCount(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(0)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, nil, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.NotNil(t, confirmationUpdateResult.Confirmations)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	// The code builds a full confirmation queue from the canonical chain
	assert.Len(t, confirmationUpdateResult.Confirmations, 1)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
}

func TestBuildConfirmationList_NilConfirmationMap_ZeroConfirmationCountError(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 99, []uint64{})
	defer done()
	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(0)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, nil, txBlockInfo, targetConfirmationCount)
	assert.Error(t, err)
	assert.Nil(t, confirmationUpdateResult)
	assert.Regexp(t, "FF23062", err.Error())
}

func TestBuildConfirmationList_NilConfirmationMapUnconfirmed(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 100, 104, []uint64{})
	defer done()
	ctx := context.Background()
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, nil, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.NotNil(t, confirmationUpdateResult.Confirmations)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.False(t, confirmationUpdateResult.Confirmed)
	// The code builds a confirmation queue from the canonical chain up to the available blocks
	assert.Len(t, confirmationUpdateResult.Confirmations, 5) // 100, 101, 102, 103, 104
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))

}

func TestBuildConfirmationList_EmptyConfirmationQueue(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(txBlockNumber)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(txBlockNumber - 1),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, []*ethrpc.BlockInfoJSONRPC{}, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	// The code builds a full confirmation queue from the canonical chain
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ExistingConfirmationsTooDistant(t *testing.T) {
	// Setup

	bl, done := newBlockListenerWithTestChain(t, 100, 5, 145, 150, []uint64{102, 103, 104, 105})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert all confirmations are in the confirmation queue
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_CorruptedExistingConfirmationDoNotAffectConfirmations(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()

	ctx := context.Background()
	// Create corrupted confirmation (wrong parent hash)
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(wrongBlockNumber)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ConnectionNodeMismatch(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{101})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(wrongBlockNumber), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		{Hash: generateTestHash(103), Number: 103, ParentHash: generateTestHash(102)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_FailedToFetchBlockInfo(t *testing.T) {
	// Setup
	mRPC := &rpcbackendmocks.Backend{}
	bl := &blockListener{
		canonicalChain: createTestChain(150, 150),
		backend:        mRPC,
	}
	bl.blockCache, _ = lru.New(100)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x69", false).Return(&rpcbackend.RPCError{Message: "pop"})

	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(wrongBlockNumber)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.Error(t, err)
	assert.Regexp(t, "pop", err.Error())
	assert.Nil(t, confirmationUpdateResult)
}

func TestBuildConfirmationList_NilBlockInfo(t *testing.T) {
	// Setup
	mRPC := &rpcbackendmocks.Backend{}
	bl := &blockListener{
		canonicalChain: createTestChain(150, 150),
		backend:        mRPC,
	}
	bl.blockCache, _ = lru.New(100)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", "0x"+strconv.FormatUint(105, 16), false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = nil
	})

	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(wrongBlockNumber)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.Error(t, err)
	assert.Regexp(t, "FF23011", err.Error())
	assert.Nil(t, confirmationUpdateResult)
}

func TestBuildConfirmationList_NewForkAfterFirstConfirmation(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 100, 150, []uint64{})
	defer done()

	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(991 /* fork1 */), Number: 102, ParentHash: generateTestHash(101)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
}

func TestBuildConfirmationList_NewForkAfterFirstConfirmation_ZeroConfirmationCount(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 100, 150, []uint64{})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(991 /* fork1 */), Number: 102, ParentHash: generateTestHash(101)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(0)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 1)
}

func TestBuildConfirmationList_NewForkAndNoConnectionToCanonicalChain(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 103, 150, []uint64{101, 102})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(991 /* fork1 */), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(992 /* fork2 */), Number: 102, ParentHash: generateTestHash(991)},
		{Hash: generateTestHash(993 /* fork3 */), Number: 103, ParentHash: generateTestHash(992)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
}

func TestBuildConfirmationList_ConfirmWithNoFetches(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 102, 150, []uint64{})
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(2)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 3)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
}

func TestBuildConfirmationList_AlreadyConfirmable(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 103, 150, []uint64{})
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		{Hash: generateTestHash(103), Number: 103, ParentHash: generateTestHash(102)},
		{Hash: generateTestHash(104), Number: 104, ParentHash: generateTestHash(103)},
		{Hash: generateTestHash(105), Number: 105, ParentHash: generateTestHash(104)},
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(2)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 3)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
}

func TestBuildConfirmationList_AlreadyConfirmable_ZeroConfirmationCount(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 103, 150, []uint64{})
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		{Hash: generateTestHash(103), Number: 103, ParentHash: generateTestHash(102)},

		// all blocks after the first block of the canonical chain are discarded in the final confirmation queue
		{Hash: generateTestHash(104), Number: 104, ParentHash: generateTestHash(103)}, // discarded
		{Hash: generateTestHash(105), Number: 105, ParentHash: generateTestHash(104)}, // discarded
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(0)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 1)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
}

func TestBuildConfirmationList_AlreadyConfirmableConnectable(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 103, 150, []uint64{})
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		// didn't have block 103, which is the first block of the canonical chain
		// but we should still be able to validate the existing confirmations are valid using parent hash
	}
	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(1)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	// The confirmation queue should return the confirmation queue up to the first block of the canonical chain

	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 2)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
}

func TestBuildConfirmationList_HasSufficientConfirmationsButNoOverlapWithCanonicalChain(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 104, 150, []uint64{101})
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(1)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	// Because the existing confirmations do not have overlap with the canonical chain,
	// the confirmation queue should return the tx block and the first block of the canonical chain
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 2)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))

}

func TestBuildConfirmationList_ConfirmableWithLateList(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, nil)
	defer done()
	ctx := context.Background()
	// Create confirmations that already meet the target
	// and it connects to the canonical chain to validate they are still valid
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
		{Hash: generateTestHash(103), Number: 103, ParentHash: generateTestHash(102)},
		{Hash: generateTestHash(104), Number: 104, ParentHash: generateTestHash(103)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	// Because the existing confirmations do not have overlap with the canonical chain,
	// the confirmation queue should return the tx block and the first block of the canonical chain
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ValidExistingConfirmations(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
		{Hash: generateTestHash(101), Number: 101, ParentHash: generateTestHash(100)},
		{Hash: generateTestHash(102), Number: 102, ParentHash: generateTestHash(101)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ValidExistingTxBlock(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()
	existingQueue := []*ethrpc.BlockInfoJSONRPC{
		{Hash: generateTestHash(100), Number: 100, ParentHash: generateTestHash(99)},
	}

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(5)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, existingQueue, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.False(t, confirmationUpdateResult.NewFork)
	assert.True(t, confirmationUpdateResult.Confirmed)
	assert.Len(t, confirmationUpdateResult.Confirmations, 6)
	assert.Equal(t, txBlockNumber, uint64(confirmationUpdateResult.Confirmations[0].BlockNumber))
	assert.Equal(t, txBlockNumber+1, uint64(confirmationUpdateResult.Confirmations[1].BlockNumber))
	assert.Equal(t, txBlockNumber+2, uint64(confirmationUpdateResult.Confirmations[2].BlockNumber))
	assert.Equal(t, txBlockNumber+3, uint64(confirmationUpdateResult.Confirmations[3].BlockNumber))
	assert.Equal(t, txBlockNumber+4, uint64(confirmationUpdateResult.Confirmations[4].BlockNumber))
	assert.Equal(t, txBlockNumber+5, uint64(confirmationUpdateResult.Confirmations[5].BlockNumber))
}

func TestBuildConfirmationList_ReachTargetConfirmation(t *testing.T) {
	// Setup
	bl, done := newBlockListenerWithTestChain(t, 100, 5, 50, 150, []uint64{})
	defer done()
	ctx := context.Background()

	txBlockNumber := uint64(100)
	txBlockHash := generateTestHash(100)
	txBlockInfo := &ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(txBlockNumber),
		Hash:       txBlockHash,
		ParentHash: generateTestHash(99),
	}
	targetConfirmationCount := uint64(3)

	// Execute
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, []*ethrpc.BlockInfoJSONRPC{}, txBlockInfo, targetConfirmationCount)
	assert.NoError(t, err)
	// Assert
	assert.True(t, confirmationUpdateResult.Confirmed)
	// The code builds a full confirmation queue from the canonical chain
	assert.GreaterOrEqual(t, len(confirmationUpdateResult.Confirmations), 4) // tx block + 3 confirmations
}

// Helper functions

// generateTestHash creates a predictable hash for testing with consistent prefix and last 4 digits as index
func generateTestHash(index uint64, mods ...uint64) ethtypes.HexBytes0xPrefix {
	val := index
	for _, mod := range mods {
		val += mod * 10000000000 // tweaks the number, while still recognizable
	}
	return ethtypes.MustNewHexBytes0xPrefix(fmt.Sprintf("0x%060x", val))
}

func createTestChain(startBlock, endBlock uint64) *list.List {
	chain := list.New()
	for i := startBlock; i <= endBlock; i++ {
		blockHash := generateTestHash(i)

		var parentHash ethtypes.HexBytes0xPrefix
		if i > startBlock || i > 0 {
			parentHash = generateTestHash(i - 1)
		} else {
			// For the first block, if it's 0, use a dummy parent hash
			parentHash = generateTestHash(9999) // Use a high number to avoid conflicts
		}

		blockInfo := &ethrpc.BlockInfoJSONRPC{
			Number:     ethtypes.HexUint64(i),
			Hash:       blockHash,
			ParentHash: parentHash,
		}
		chain.PushBack(blockInfo)
	}
	return chain
}

func newFakedChainBlockListener(t *testing.T, chain *list.List) (*rpcbackendmocks.Backend, *blockListener) {
	mRPC := rpcbackendmocks.NewBackend(t)
	blockCache, _ := lru.New(100)
	bl := &blockListener{
		canonicalChain: chain,
		backend:        mRPC,
		blockCache:     blockCache,
	}
	return mRPC, bl
}

func newBlockListenerWithTestChain(t *testing.T, txBlock, confirmationCount, startCanonicalBlock, endCanonicalBlock uint64, blocksToMock []uint64) (*blockListener, func()) {
	mRPC, bl := newFakedChainBlockListener(t, createTestChain(startCanonicalBlock, endCanonicalBlock))

	if len(blocksToMock) > 0 {
		for _, blockNumber := range blocksToMock {
			hexBlockNumber := ethtypes.HexUint64(blockNumber)
			mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", hexBlockNumber.String(), false).Return(nil).Run(func(args mock.Arguments) {
				*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
					Number:     ethtypes.HexUint64(blockNumber),
					Hash:       generateTestHash(blockNumber),
					ParentHash: generateTestHash(blockNumber - 1),
				}}
			})
		}
	}
	return bl, func() {
		mRPC.AssertExpectations(t)
	}
}

func mockBlockRange(mRPC *rpcbackendmocks.Backend, start, end uint64, mods ...uint64) {
	for blockNumber := start; blockNumber <= end; blockNumber++ {
		hexBlockNumber := ethtypes.HexUint64(blockNumber)
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", hexBlockNumber.String(), false).Return(nil).Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
				Number:     ethtypes.HexUint64(blockNumber),
				Hash:       generateTestHash(blockNumber, mods...),
				ParentHash: generateTestHash(blockNumber-1, mods...),
			}}
		}).Maybe()
	}
}
