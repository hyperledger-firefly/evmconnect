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
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBlockCache(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	block1000Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001AHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001BHash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", mock.Anything, false).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, block1001AHash.String(), args[3].(string))
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     ethtypes.HexUint64(1001),
			Hash:       block1001AHash,
			ParentHash: block1000Hash,
		}}
	}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Run(func(args mock.Arguments) {
		assert.Equal(t, ethtypes.NewHexInteger64(1001).String(), args[3].(string))
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number:     ethtypes.HexUint64(1001),
			Hash:       block1001BHash,
			ParentHash: block1000Hash,
		}}
	}).Once()

	block, err := bl.GetBlockInfoByHash(ctx, block1001AHash.String())
	require.NoError(t, err)
	require.Equal(t, block1001AHash, block.Hash)

	// From cache (tested by Once() above)
	block, err = bl.GetBlockInfoByHash(ctx, block1001AHash.String())
	require.NoError(t, err)
	require.Equal(t, block1001AHash, block.Hash)

	// NOT from cache due to mismatch
	block, err = bl.GetBlockInfoByNumber(ctx, 1001, true, block1000Hash.String(), block1001BHash.String())
	require.NoError(t, err)
	require.Equal(t, block1001BHash, block.Hash)

}

func TestGetEVMBlockWithTxHashesByHash(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	block1000Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", block1001Hash.String(), false).
		Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{
				BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
					Number:     ethtypes.HexUint64(1001),
					Hash:       block1001Hash,
					ParentHash: block1000Hash,
				},
			}
		}).
		Once()
	block, err := bl.GetEVMBlockWithTxHashesByHash(ctx, block1001Hash.String())
	require.NoError(t, err)
	require.Equal(t, block1001Hash, block.Hash)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", block1000Hash.String(), false).
		Return(rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, i18n.Msg404NotFound)).
		Once()
	_, err = bl.GetEVMBlockWithTxHashesByHash(ctx, block1000Hash.String())
	require.Regexp(t, "FF00167", err)
}

func TestGetEVMBlockWithTransactionsByHash(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	block1000Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", block1001Hash.String(), true).
		Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTransactionsJSONRPC) = &ethrpc.EVMBlockWithTransactionsJSONRPC{
				BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
					Number:     ethtypes.HexUint64(1001),
					Hash:       block1001Hash,
					ParentHash: block1000Hash,
				},
			}
		}).
		Once()
	block, err := bl.GetEVMBlockWithTransactionsByHash(ctx, block1001Hash.String())
	require.NoError(t, err)
	require.Equal(t, block1001Hash, block.Hash)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", block1000Hash.String(), true).
		Return(rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, i18n.Msg404NotFound)).
		Once()
	_, err = bl.GetEVMBlockWithTransactionsByHash(ctx, block1000Hash.String())
	require.Regexp(t, "FF00167", err)
}

func TestGetEVMBlockWithTxHashesByNumber(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	block1000Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", ethtypes.NewHexInteger64(1001).String(), false).
		Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{
				BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
					Number:     ethtypes.HexUint64(1001),
					Hash:       block1001Hash,
					ParentHash: block1000Hash,
				},
			}
		}).
		Once()
	block, err := bl.GetEVMBlockWithTxHashesByNumber(ctx, ethtypes.NewHexInteger64(1001).String())
	require.NoError(t, err)
	require.Equal(t, block1001Hash, block.Hash)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", ethtypes.NewHexInteger64(1000).String(), false).
		Return(rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, i18n.Msg404NotFound)).
		Once()
	_, err = bl.GetEVMBlockWithTxHashesByNumber(ctx, ethtypes.NewHexInteger64(1000).String())
	require.Regexp(t, "FF00167", err)
}

func TestGetEVMBlockWithTransactionsByNumber(t *testing.T) {
	ctx, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	block1000Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	block1001Hash := ethtypes.MustNewHexBytes0xPrefix(fftypes.NewRandB32().String())
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", ethtypes.NewHexInteger64(1001).String(), true).
		Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTransactionsJSONRPC) = &ethrpc.EVMBlockWithTransactionsJSONRPC{
				BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
					Number:     ethtypes.HexUint64(1001),
					Hash:       block1001Hash,
					ParentHash: block1000Hash,
				},
			}
		}).
		Once()
	block, err := bl.GetEVMBlockWithTransactionsByNumber(ctx, ethtypes.NewHexInteger64(1001).String())
	require.NoError(t, err)
	require.Equal(t, block1001Hash, block.Hash)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", ethtypes.NewHexInteger64(1000).String(), true).
		Return(rpcbackend.NewRPCError(ctx, rpcbackend.RPCCodeInternalError, i18n.Msg404NotFound)).
		Once()
	_, err = bl.GetEVMBlockWithTransactionsByNumber(ctx, ethtypes.NewHexInteger64(1000).String())
	require.Regexp(t, "FF00167", err)
}
