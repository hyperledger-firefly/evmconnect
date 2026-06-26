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

	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleSendTX = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"value": "12345678901234567890123456789",
	"transactionData": "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef"
}`

const sampleSendRawTX = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"transactionData": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",
	"preSigned": true
}`

const sampleSendTXBadFrom = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	}
}`

const sampleSendTXBadTo = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"from": "0x3088C3B2361e5b12c5270fA0692d2Fa6b29bdB63",
	"to": "bad to"
}`

const sampleSendTXBadData = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"transactionData": "not hex"
}`

const sampleSendTXBadGasPrice = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"from": "0x3088C3B2361e5b12c5270fA0692d2Fa6b29bdB63",
	"gasPrice": "not a number"
}`

const sampleSendTXGasPriceEIP1559 = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"from": "0x3088C3B2361e5b12c5270fA0692d2Fa6b29bdB63",
	"gasPrice": {
		"maxPriorityFeePerGas": 12345,
		"maxFeePerGas": "0xffff"
	}
}`

const sampleSendTXGasPriceLegacy = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "send_transaction"
	},
	"from": "0x3088C3B2361e5b12c5270fA0692d2Fa6b29bdB63",
	"gasPrice": {
		"gasPrice": "0xffff"
	}
}`

func TestSendTransactionBadHash(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x123456")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23048", err)
	assert.Empty(t, reason)

	mRPC.AssertExpectations(t)

}

func TestSendTransactionOK(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x3e2398ff4a875a8b9f87a6eeaaa41a139a68adeb509731300d4b90d1bdc1c4fc")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTX), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, "0x3e2398ff4a875a8b9f87a6eeaaa41a139a68adeb509731300d4b90d1bdc1c4fc", res.TransactionHash)

	mRPC.AssertExpectations(t)

}

func TestSendPreSignedTransactionOK(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x332db2d926128920c2dc1b2067de4e86d073975fd018e22ed2470449e755b508")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, "0x332db2d926128920c2dc1b2067de4e86d073975fd018e22ed2470449e755b508", res.TransactionHash)

	mRPC.AssertExpectations(t)
}

func TestSendPreSignedTransactionBadHash(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x1234")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23048", err)
	assert.Empty(t, reason)

	mRPC.AssertExpectations(t)
}

func TestSendPreSignedTransactionNonceTooLow(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "nonce too low"})

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "nonce_too_low", reason)
	assert.Regexp(t, "nonce too low", err.Error())

	mRPC.AssertExpectations(t)
}

func TestSendPreSignedTransactionKnownTransaction(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "known transaction"})

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "known_transaction", reason)
	assert.Regexp(t, "known transaction", err.Error())

	mRPC.AssertExpectations(t)
}

func TestSendPreSignedTransactionUnderpriced(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "transaction underpriced"})

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "transaction_underpriced", reason)
	assert.Regexp(t, "transaction underpriced", err.Error())

	mRPC.AssertExpectations(t)
}

func TestSendPreSignedTransactionInsufficientFunds(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendRawTransaction",
		mock.MatchedBy(func(data string) bool {
			assert.Equal(t, "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675", data)
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "insufficient funds"})

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendRawTX), &req)
	assert.NoError(t, err)
	_, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "insufficient_funds", reason)
	assert.Regexp(t, "insufficient funds", err.Error())

	mRPC.AssertExpectations(t)
}

func TestSendTransactionFail(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		})).
		Return(&rpcbackend.RPCError{Message: "pop"})

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTX), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "pop", err)
	assert.Empty(t, reason)
	assert.Nil(t, res)

	mRPC.AssertExpectations(t)

}

func TestSendErrorMapping(t *testing.T) {
	assert.Equal(t, ffcapi.ErrorReasonNonceTooLow, etherrors.MapError(etherrors.SendRPCMethods, fmt.Errorf("nonce too low")))
	assert.Equal(t, ffcapi.ErrorReasonInsufficientFunds, etherrors.MapError(etherrors.SendRPCMethods, fmt.Errorf("insufficient funds")))
	assert.Equal(t, ffcapi.ErrorReasonTransactionUnderpriced, etherrors.MapError(etherrors.SendRPCMethods, fmt.Errorf("transaction underpriced")))
	assert.Equal(t, ffcapi.ErrorKnownTransaction, etherrors.MapError(etherrors.SendRPCMethods, fmt.Errorf("known transaction")))
	assert.Equal(t, ffcapi.ErrorKnownTransaction, etherrors.MapError(etherrors.SendRPCMethods, fmt.Errorf("already known")))
}

func TestCallErrorMapping(t *testing.T) {
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, etherrors.MapError(etherrors.CallRPCMethods, fmt.Errorf("execution reverted")))
}

func TestSendTransactionBadFrom(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXBadFrom), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23019", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestSendTransactionBadTo(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXBadTo), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23020", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestSendTransactionBadData(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXBadData), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23018", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestSendTransactionBadGasPrice(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXBadGasPrice), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.Regexp(t, "FF23015", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)
	assert.Nil(t, res)

}

func TestSendTransactionGasPriceEIP1559(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, int64(65535), tx.MaxFeePerGas.BigInt().Int64())
			assert.Equal(t, int64(12345), tx.MaxPriorityFeePerGas.BigInt().Int64())
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x3e2398ff4a875a8b9f87a6eeaaa41a139a68adeb509731300d4b90d1bdc1c4fc")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXGasPriceEIP1559), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.NotNil(t, res)

}

func TestSendTransactionGasPriceLegacyNested(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_sendTransaction",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, int64(65535), tx.GasPrice.BigInt().Int64())
			return true
		})).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x3e2398ff4a875a8b9f87a6eeaaa41a139a68adeb509731300d4b90d1bdc1c4fc")
		}).
		Return(nil)

	var req ffcapi.TransactionSendRequest
	err := json.Unmarshal([]byte(sampleSendTXGasPriceLegacy), &req)
	assert.NoError(t, err)
	res, reason, err := c.TransactionSend(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.NotNil(t, res)

}
