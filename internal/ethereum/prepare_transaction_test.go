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
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const samplePrepareTXWithGas = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"value": "12345678901234567890123456789",
	"method": {
		"inputs": [],
		"name":"do",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": []
}`

const samplePrepareTXEstimateGas = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"nonce": "222",
	"method": {
		"inputs": [],
		"name":"do",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"method": {
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"name":"set",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": [ 4276993775 ]
}`

const samplePrepareTXBadMethod = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"method": false,
	"params": []
}`

const samplePrepareTXBadTo = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "badness",
	"gas": 1000000,
	"method": {"name":"set"},
	"params": []
}`

const samplePrepareTXBadParam = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"method": {
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"name":"set",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": [ "wrong type" ]
}`

const samplePrepareTXBadErrors = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"method": {
		"inputs": [],
		"name":"do",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"errors": [false]
}`

const samplePrepareTXHugeNumberParam = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"nonce": "222",
	"method": {
		"inputs": [],
		"name":"do",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"method": {
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"name":"set",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": [ 10000000000000000000000001 ]
}`

const samplePrepareTXScientificNumberParam = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "prepare_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"nonce": "222",
	"method": {
		"inputs": [],
		"name":"do",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"method": {
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"name":"set",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": [ 1.0000000000000000000000002e+25 ]
}`

func TestPrepareTransactionOkNoEstimate(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXWithGas), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)

	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, int64(1000000), res.Gas.Int64())

}

func TestPrepareTransactionOkHugeNumberParam(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b1000000000000000000000000000000000000000000084595161401484a000001", tx.Data.String())
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXHugeNumberParam), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)

	assert.NoError(t, err)
	assert.Empty(t, reason)

	// Basic check that our input param 10000000000000000000000001 is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "84595161401484a000001"))
}

func TestPrepareTransactionOkScientificNumberParam(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b1000000000000000000000000000000000000000000084595161401484a000002", tx.Data.String())
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXScientificNumberParam), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)

	assert.NoError(t, err)
	assert.Empty(t, reason)

	// Basic check that our input param 1.0000000000000000000000002e+25 is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "84595161401484a000002"))
}

func TestPrepareTransactionWithEstimate(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXEstimateGas), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, int64(18517) /* 1.5 uplift */, res.Gas.Int64())

}

func TestPrepareTransactionWithEstimateRevert(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").Run(
		func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000114d75707065747279206465746563746564000000000000000000000000000000")
		},
	).Return(nil)

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXEstimateGas), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "FF23021", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}

func TestPrepareTransactionWithEstimateFail(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").Return(&rpcbackend.RPCError{Message: "pop"})

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXEstimateGas), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "pop", err)
	assert.Empty(t, reason)
	assert.Nil(t, res)

}

func TestPrepareTransactionWithEstimateFailBadData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop", Data: "bad data"}).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		}), "latest").
		Return(&rpcbackend.RPCError{Message: "pop", Data: "bad data"})

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXEstimateGas), &req)
	assert.NoError(t, err)
	_, _, err = c.TransactionPrepare(ctx, &req)
	assert.Error(t, err)
	// We fall back to an eth_call, but return the original eth_estimateGas error as we can't process the eth_call response either
	assert.Regexp(t, "pop", err)

	mRPC.AssertExpectations(t)
}

func TestPrepareTransactionWithBadMethod(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXBadMethod), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "FF23013", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestPrepareTransactionWithBadParam(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXBadParam), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "FF22030", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestPrepareTransactionWithBadTo(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXBadTo), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "FF23020", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestPrepareTransactionWithBadErrors(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareTXBadErrors), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionPrepare(ctx, &req)
	assert.Regexp(t, "FF23050", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestMapFFCAPIToEthBadParams(t *testing.T) {

	_, c, _, done := newTestConnector(t)
	defer done()

	_, _, err := c.prepareCallData(context.Background(), &ffcapi.TransactionInput{
		Method: fftypes.JSONAnyPtr("{}"),
		Params: []*fftypes.JSONAny{fftypes.JSONAnyPtr("!wrong")},
	})
	assert.Regexp(t, "FF23014", err)

}
