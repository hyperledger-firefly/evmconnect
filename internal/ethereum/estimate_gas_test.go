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
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/evmconnect/mocks/ethblocklistenermocks"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var testDefaultError = &abi.Entry{
	Type:   abi.Error,
	Name:   "Error",
	Inputs: abi.ParameterArray{{Type: "string"}},
}

const sampleGasEstimate = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "get_address_balance"
	},
	"to": "0x4a8c8f1717570f9774652075e249ded38124d708",
	"from": "0x73bd8f17787a0f9774652075e2ba5ed381246bef",
	"value": "100000000",
	"nonce": "0x01"
}`

func TestGasEstimateOK(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, int64(18517) /* 1.5 uplift */, res.GasEstimate.Int64())

}

func TestGasEstimateAboveBlockLimit(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mbl := ethblocklistenermocks.NewBlockListener(t)
	mbl.On("GetBlockGasLimit").Return(ethtypes.NewHexInteger64(1000))
	mbl.On("WaitClosed").Return()
	c.blockListener = mbl

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	_, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23071", err)
	assert.Empty(t, reason)

}

func TestGasEstimateFail(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").Run(
		func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000114d75707065747279206465746563746564000000000000000000000000000000")
		},
	).Return(nil)

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23021", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}

func TestGasEstimateBadFromAddress(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(`{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "get_address_balance"
	},
	"to": "0x4a8c8f1717570f9774652075e249ded38124d708",
	"from": "bad address",
	"value": "100000000",
	"nonce": "0x01"
}`), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23019", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestGasEstimateBadToAddress(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(`{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "get_address_balance"
	},
	"to": "bad address",
	"from": "0x4a8c8f1717570f9774652075e249ded38124d708",
	"value": "100000000",
	"nonce": "0x01"
}`), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23020", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestGasEstimateFailRevertReasonInData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	errData, err := testDefaultError.EncodeCallDataValues([]string{"this reason"})
	assert.NoError(t, err)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "reverted for reason...", Data: *fftypes.JSONAnyPtr(
			`"0x` + hex.EncodeToString(errData) + `"`,
		)})

	var req ffcapi.TransactionInput
	err = json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23021.*this reason", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}

func TestGasEstimateFailThenNilCallNoMethod(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		}), "latest").
		Run(func(args mock.Arguments) {
			hb := args[1].(*ethtypes.HexBytes0xPrefix)
			*hb = []byte("000102030405") // ignored as there's no method to parse the outputs against
		}).
		Return(nil)

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "pop", err)
	assert.Empty(t, reason)
	assert.Nil(t, res)

}

func TestGasEstimateFailThenRevertDataFromCall(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	errData, err := testDefaultError.EncodeCallDataValues([]string{"this reason"})
	assert.NoError(t, err)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		}), "latest").
		Return(&rpcbackend.RPCError{Message: "reverted for reason...", Data: *fftypes.JSONAnyPtr(
			`"0x` + hex.EncodeToString(errData) + `"`,
		)})

	var req ffcapi.TransactionInput
	err = json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23021.*this reason", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}

func TestGasEstimateFailThenRevertErrorNoExtraInfo(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		}), "latest").
		Return(&rpcbackend.RPCError{Message: "execution reverted, without return data"})

	var req ffcapi.TransactionInput
	err := json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23021.*execution reverted, without return data", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}

func TestGasEstimateFailCustomErrorCannotParse(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	errData, err := testDefaultError.EncodeCallDataValues([]string{"this reason"})
	assert.NoError(t, err)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "reverted for reason...", Data: *fftypes.JSONAnyPtr(
			`"0x` + hex.EncodeToString(errData) + `"`,
		)})

	var req ffcapi.TransactionInput
	err = json.Unmarshal([]byte(sampleGasEstimate), &req)
	assert.NoError(t, err)
	res, reason, err := c.GasEstimate(ctx, &req)
	assert.Regexp(t, "FF23021.*this reason", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

}
