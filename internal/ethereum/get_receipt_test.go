// Copyright © 2022 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleGetReceipt = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "get_receipt"
	},
	"transactionHash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2"
}`

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

const sampleJSONRPCReceiptFailed = `{
	"blockHash": "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
	"blockNumber": "0x7b9",
	"contractAddress": "0x87ae94ab290932c4e6269648bb47c86978af4436",
	"cumulativeGasUsed": "0x8414",
	"effectiveGasPrice": "0x0",
	"from": "0x2b1c769ef5ad304a4889f2a07a6617cd935849ae",
	"gasUsed": "0x8414",
	"logs": [],
	"logsBloom": "0x00000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000100000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000",
	"status": "0",
	"to": "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3",
	"transactionHash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
	"transactionIndex": "0x1e",
	"type": "0x0"
}`

const sampleJSONRPCReceiptFailedWithRevertReason = `{
	"blockHash": "0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
	"blockNumber": "0x7b9",
	"contractAddress": "0x87ae94ab290932c4e6269648bb47c86978af4436",
	"cumulativeGasUsed": "0x8414",
	"effectiveGasPrice": "0x0",
	"from": "0x2b1c769ef5ad304a4889f2a07a6617cd935849ae",
	"gasUsed": "0x8414",
	"logs": [],
	"logsBloom": "0x00000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000100000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000",
	"status": "0",
	"to": "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3",
	"transactionHash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
	"transactionIndex": "0x1e",
	"revertReason": "0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000001d5468652073746f7265642076616c756520697320746f6f20736d616c6c000000",
	"type": "0x0"
}`

const sampleTransactionTraceGeth = `{
	"gas": 23512,
	"failed": true,
	"returnValue": "08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000114e6f7420656e6f75676820746f6b656e73000000000000000000000000000000",
	"structLogs": []
}
`

const sampleTransactionTraceNoReturnValue = `{
	"gas": 23512,
	"failed": true,
	"structLogs": []
}
`

const sampleTransactionTraceGethInvalidHex = `{
	"gas": 23512,
	"failed": true,
	"returnValue": "invalid hex",
	"structLogs": []
}
`

const sampleTransactionTraceGethInvalidCallData = `{
	"gas": 23512,
	"failed": true,
	"returnValue": "0badca11da7a",
	"structLogs": []
}
`

const sampleTransactionTraceBesu = `{
	"gas": 23512,
	"failed": true,
	"returnValue": "",
	"structLogs": [
		{
			"pc": 0,
			"op": "PUSH1",
			"gas": 78404,
			"gasCost": 3,
			"depth": 1,
			"stack": [],
			"memory": [],
			"storage": {},
			"reason": null
		},
		{
			"pc": 4,
			"op": "MSTORE",
			"gas": 78398,
			"gasCost": 12,
			"depth": 1,
			"stack": [
				"0000000000000000000000000000000000000000000000000000000000000080",
				"0000000000000000000000000000000000000000000000000000000000000040"
			],
			"memory": [
				"0000000000000000000000000000000000000000000000000000000000000000",
				"0000000000000000000000000000000000000000000000000000000000000000",
				"0000000000000000000000000000000000000000000000000000000000000080"
			],
			"storage": {},
			"reason": null
		},
		{
			"pc": 829,
			"op": "REVERT",
			"gas": 76488,
			"gasCost": 0,
			"depth": 1,
			"stack": [
				"00000000000000000000000000000000000000000000000000000000a9059cbb",
				"0000000000000000000000000000000000000000000000000000000000000129",
				"000000000000000000000000b5855faa164db05e70fd3476e3540fc8c4053a01",
				"00000000000000000000000000000000000000000000000000000000000f4241",
				"0000000000000000000000000000000000000000000000000000000000000064",
				"0000000000000000000000000000000000000000000000000000000000000080"
			],
			"memory": [
				"00000000000000000000000046ec33d9fd840ae95f36c3449cf46041ff6fb886",
				"0000000000000000000000000000000000000000000000000000000000000004",
				"0000000000000000000000000000000000000000000000000000000000000080",
				"0000000000000000000000000000000000000000000000000000000000000000",
				"08c379a000000000000000000000000000000000000000000000000000000000",
				"0000002000000000000000000000000000000000000000000000000000000000",
				"000000114e6f7420656e6f75676820746f6b656e730000000000000000000000",
				"0000000000000000000000000000000000000000000000000000000000000000"
			],
			"storage": {},
			"reason": "8c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000114e6f7420656e6f75676820746f6b656e73000000000000000000000000000000"
		}
	]
}`

const sampleTransactionInputJSONRPC = `{
	"blockHash": "0xa6d9ef8afd65f187e43d1ebd378681bf79663920da0563cd8c06b49dd1db8758",
	"blockNumber": "0x6790",
	"chainId": "0x3dbb0ab",
	"from": "0xa61465d0d19d842d73625cb7a2b6f318c74d304b",
	"gas": "0xd900",
	"gasPrice": "0x0",
	"hash": "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
	"input": "0x40c10f190000000000000000000000005dae1910885cde875de559333d12722357e69c42000000000000000000000000000000000000000000000000016345785d8a0000",
	"nonce": "0xc",
	"to": "0xd0685a91ae2d4b0ec4701f7a9787c6633790a65e",
	"transactionIndex": "0x0",
	"type": "0x0",
	"value": "0x0",
	"v": "0x7b76179",
	"r": "0x963a8620b31ac796dd605f37c7f386d039f40c3a31854931fae5e3c95b1faf7a",
	"s": "0x58bb39fa958611c123adbef40eaaba44d36cddf77532064a437f0840242e5d30"
}`

func TestGetReceiptOkSuccess(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})

	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.True(t, res.Success)
	assert.Equal(t, int64(1977), res.BlockNumber.Int64())
	assert.Equal(t, int64(30), res.TransactionIndex.Int64())

}

func TestGetReceiptNotFound(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte("null"), args[1])
			assert.NoError(t, err)
		})

	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.Regexp(t, "FF23012", err)
	assert.Equal(t, ffcapi.ErrorReasonNotFound, reason)
	assert.Nil(t, res)

}

func TestGetReceiptError(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt", mock.Anything).
		Return(&rpcbackend.RPCError{Message: "pop"})

	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.Regexp(t, "pop", err)
	assert.Empty(t, "", reason)
	assert.Nil(t, res)

}

func TestGetReceiptErrorReasonGeth(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceGeth), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)
	assert.Contains(t, res.ExtraInfo.String(), "Not enough tokens")

}

func TestGetReceiptErrorReasonBesu(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceBesu), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)
	assert.Contains(t, res.ExtraInfo.String(), "Not enough tokens")

}

func TestGetReceiptErrorReasonNotFound(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceNoReturnValue), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)

}

func TestGetReceiptErrorReasonErrorFromHexDecode(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceGethInvalidHex), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)
}

func TestGetReceiptErrorReasonErrorFromTrace(t *testing.T) {
	// if we get an error tracing the transaction, we ignore it.  Not all nodes support the debug_traceTransaction RPC call

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "unsupported"}).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceGeth), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)
}

func TestGetReceiptErrorReasonErrorFromDecodeCallData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	c.traceTXForRevertReason = true
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionTraceGethInvalidCallData), args[1])
			assert.NoError(t, err)
		})
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.False(t, res.Success)
}

func TestGetReceiptErrorReasonErrorFromReceiptRevert(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailedWithRevertReason), args[1])
			assert.NoError(t, err)
		})
	mRPC.AssertNotCalled(t, "CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction", mock.Anything)
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.Contains(t, res.ExtraInfo.String(), "The stored value is too small") // Check the decoded revert reason string is present in extra-info
	assert.False(t, res.Success)
}

func TestGetReceiptNoDebugTraceIfDisabled(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		mock.MatchedBy(func(txHash string) bool {
			assert.Equal(t, "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2", txHash)
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceiptFailed), args[1])
			assert.NoError(t, err)
		})
	mRPC.AssertNotCalled(t, "CallRPC", mock.Anything, mock.Anything, "debug_traceTransaction")
	var req ffcapi.TransactionReceiptRequest
	err := json.Unmarshal([]byte(sampleGetReceipt), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.False(t, res.Success)
	mRPC.AssertExpectations(t)
}

func TestProtocolIDForReceipt(t *testing.T) {
	assert.Equal(t, "000000012345/000042", ProtocolIDForReceipt(fftypes.NewFFBigInt(12345), fftypes.NewFFBigInt(42)))
	assert.Equal(t, "", ProtocolIDForReceipt(nil, nil))
}

func TestGetReceiptEventDecodeOK(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "net_version", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		c.chainID = "12345"
	}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionReceipt",
		"0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2").
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleJSONRPCReceipt), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash",
		"0x6197ef1a58a2a592bb447efb651f0db7945de21aa8048801b250bd7b7431f9b6",
		false).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleBlockJSONRPC), args[1])
			assert.NoError(t, err)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionByHash", mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := json.Unmarshal([]byte(sampleTransactionInputJSONRPC), args[1])
			assert.NoError(t, err)
		})

	req := ffcapi.TransactionReceiptRequest{
		TransactionHash: "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
		IncludeLogs:     true,
		Methods: []fftypes.JSONAny{`{
			"inputs": [
				{
					"internalType": "address",
					"name": "to",
					"type": "address"
				},
				{
					"internalType": "uint256",
					"name": "amount",
					"type": "uint256"
				}
			],
			"name": "mint",
			"outputs": [],
			"stateMutability": "nonpayable",
			"type": "function"
		}`},
		ExtractSigner: true,
		EventFilters: []fftypes.JSONAny{*fftypes.JSONAnyPtr(`{
			"event": {
				"anonymous": false,
				"inputs": [
					{
						"indexed": true,
						"name": "from",
						"type": "address"
					},
					{
						"indexed": true,
						"name": "to",
						"type": "address"
					},
					{
						"indexed": false,
						"name": "value",
						"type": "uint256"
					}
				],
				"name": "Transfer",
				"type": "event"
			}
		}`)},
	}
	res, reason, err := c.TransactionReceipt(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.True(t, res.Success)
	assert.Equal(t, int64(1977), res.BlockNumber.Int64())
	assert.Equal(t, int64(30), res.TransactionIndex.Int64())

	assert.Len(t, res.Logs, 1)
	assert.Len(t, res.Events, 1)
	b, err := json.Marshal(res.Events[0].Data)
	assert.NoError(t, err)
	fmt.Println(string(b))
	assert.JSONEq(t, `{
		"from": "0x0000000000000000000000000000000000000000",
		"to": "0x5dae1910885cde875de559333d12722357e69c42",
		"value": "100000000000000000"
	}`, string(b))
	b = res.Events[0].Info.(*eventInfo).InputArgs.Bytes()
	assert.JSONEq(t, `{
		"to": "0x5dae1910885cde875de559333d12722357e69c42",
		"amount": "100000000000000000"
	}`, string(b))
	assert.Equal(t, "0xa61465d0d19d842d73625cb7a2b6f318c74d304b", res.Events[0].Info.(*eventInfo).InputSigner.String())

}

func TestGetReceiptEventInvalidFilters(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	req := ffcapi.TransactionReceiptRequest{
		TransactionHash: "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
		IncludeLogs:     true,
		EventFilters:    []fftypes.JSONAny{*fftypes.JSONAnyPtr(`!! wrong`)},
	}
	_, reason, err := c.TransactionReceipt(ctx, &req)
	assert.Regexp(t, "FF23036", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestGetReceiptEventInvalidMethods(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	req := ffcapi.TransactionReceiptRequest{
		TransactionHash: "0x7d48ae971faf089878b57e3c28e3035540d34f38af395958d2c73c36c57c83a2",
		IncludeLogs:     true,
		Methods:         []fftypes.JSONAny{*fftypes.JSONAnyPtr(`!! wrong`)},
	}
	_, reason, err := c.TransactionReceipt(ctx, &req)
	assert.Regexp(t, "FF23013", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}
