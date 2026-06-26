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
