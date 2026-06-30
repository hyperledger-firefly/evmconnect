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
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const samplePrepareDeployTX = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "DeployContract"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"value": "12345678901234567890123456789",
	"contract": "0xdeadbeef",
	"definition": [{
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			},
		    {
			    "internalType":" address",
			    "name": "y",
			    "type": "address"
		    },
		    {
			    "internalType":" string",
			    "name": "z",
			    "type": "string"
		    }
		],
		"outputs":[],
		"type":"constructor"
	}],
	"params": [ 4276993775, "0x5f906824E562B6a0F278D910D388728b833a43bB", "some-text" ]
}`

const samplePrepareDeployTXLargeInputParams = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "DeployContract"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"value": "12345678901234567890123456789",
	"contract": "0xdeadbeef",
	"definition": [{
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			},
		    {
			    "internalType":" string",
			    "name": "y",
			    "type": "string"
		    }
		],
		"outputs":[],
		"type":"constructor"
	}],
	"params": [ 10000000000000000000000001, "some-text" ]
}`

const samplePrepareDeployTXScientificNotation = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "DeployContract"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"nonce": "111",
	"value": "12345678901234567890123456789",
	"contract": "0xdeadbeef",
	"definition": [{
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			},
		    {
			    "internalType":" string",
			    "name": "y",
			    "type": "string"
		    }
		],
		"outputs":[],
		"type":"constructor"
	}],
	"params": [ 1.0000000000000000000000001e+25, "some-text" ]
}`

func TestDeployContractPrepareOkNoEstimate(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	assert.NoError(t, err)
	res, reason, err := c.DeployContractPrepare(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.Equal(t, int64(1000000), res.Gas.Int64())

	// Basic check that our input param 4276993775 is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "feedbeef"))
	// Basic check that our input param "some-text" is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, strings.ToLower("736f6d652d74657874")))
	// Basic check that our input param address is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, strings.ToLower("5f906824E562B6a0F278D910D388728b833a43bB")))
}

func TestDeployContractPrepareOkLargeInputParam(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTXLargeInputParams), &req)
	assert.NoError(t, err)
	res, reason, err := c.DeployContractPrepare(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.Equal(t, int64(1000000), res.Gas.Int64())

	// Basic check that our input param 10000000000000000000000001 is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "84595161401484a000001"))
	// Basic check that our input param "some-text" is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "736f6d652d74657874"))
}

func TestDeployContractPrepareOkScientificNotationParam(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTXScientificNotation), &req)
	assert.NoError(t, err)
	res, reason, err := c.DeployContractPrepare(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.Equal(t, int64(1000000), res.Gas.Int64())
	// Basic check that our input param 1.0000000000000000000000001e+25 is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "84595161401484a000001"))
	// Basic check that our input param "some-text" is in the TX data
	assert.True(t, strings.Contains(res.TransactionData, "736f6d652d74657874"))
}

func TestDeployContractPrepareWithEstimateRevert(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").Run(
		func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000114d75707065747279206465746563746564000000000000000000000000000000")
		},
	).Return(nil)

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	assert.NoError(t, err)
	req.Gas = nil
	res, reason, err := c.DeployContractPrepare(ctx, &req)
	assert.Regexp(t, "FF23021", err)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Nil(t, res)

	mRPC.AssertExpectations(t)

}

func TestDeployContractPrepareBadBytecode(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Contract = fftypes.JSONAnyPtr(`! not a string containing bytecode`)
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23047", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadBytecodeNotHex(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Contract = fftypes.JSONAnyPtr(`"!hex"`)
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23047", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadABIDefinition(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Definition = fftypes.JSONAnyPtr(`[`)
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23013", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadErrorABI(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Errors = []*fftypes.JSONAny{fftypes.JSONAnyPtr(`[`)}
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23050", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareEstimateNoConstructor(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_estimateGas", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*(args[1].(*ethtypes.HexInteger)) = *ethtypes.NewHexInteger64(12345)
	})

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	assert.NoError(t, err)
	req.Definition = fftypes.JSONAnyPtr(`[]`)
	req.Gas = nil
	res, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.NoError(t, err)
	assert.Empty(t, reason)

	fGasEstimate, _ := c.gasEstimationFactor.Float64()
	assert.Equal(t, int64(float64(12345)*fGasEstimate), res.Gas.Int64())

	mRPC.AssertExpectations(t)

}

func TestDeployContractPrepareBadParamsJSON(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Params = []*fftypes.JSONAny{fftypes.JSONAnyPtr(`"!wrong`)}
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23014", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadParamType(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Params = []*fftypes.JSONAny{fftypes.JSONAnyPtr(`"!wrong"`), fftypes.JSONAnyPtr(`"0x90eB678C3586103805a676d21721Cc6883a6c3AE"`), fftypes.JSONAnyPtr(`"helloworld"`)}
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF22030", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadFrom(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.From = "!not an address"
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF23019", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestDeployContractPrepareBadABIType(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.ContractDeployPrepareRequest
	err := json.Unmarshal([]byte(samplePrepareDeployTX), &req)
	req.Definition = fftypes.JSONAnyPtr(`[{
		"type": "constructor",
		"inputs": [{
			"type": "!wrong"
		}]
	}]`)
	assert.NoError(t, err)
	_, reason, err := c.DeployContractPrepare(ctx, &req)

	assert.Regexp(t, "FF22025", err)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}
