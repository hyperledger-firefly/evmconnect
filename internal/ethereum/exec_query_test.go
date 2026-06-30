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
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleExecQuery = `{
  "ffcapi": {
    "version": "v1.0.0",
    "id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
    "type": "exec_query"
  },
  "from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
  "to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
  "nonce": "222",
  "method": {
    "inputs": [
      {
        "internalType":" uint256",
        "name": "x",
        "type": "uint256"
      }
    ],
    "name":"set",
    "outputs":[
      {
        "internalType":"uint256",
        "name": "",
        "type": "uint256"
      },
      {
        "type": "string"
      }
    ],
    "stateMutability":"nonpayable",
    "type":"function"
  },
  "errors": [
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "x",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "y",
          "type": "uint256"
        }
      ],
      "name": "GreaterThanTen",
      "type": "error"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "x",
          "type": "string"
        }
      ],
      "name": "LessThanOne",
      "type": "error"
    }
  ],
  "params": [ 4276993775 ]
}`

func TestExecQueryOKResponse(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		}),
		"0x12345").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x00000000000000000000000000000000000000000000000000000000baadf00d0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000b68656c6c6f20776f726c64000000000000000000000000000000000000000000")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	req.BlockNumber = strPtr("0x12345")
	assert.NoError(t, err)

	res, reason, err := c.QueryInvoke(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.JSONEq(t, `{"output": "3131961357", "output1":"hello world"}`, res.Outputs.String())

}

func TestExecQueryOKNilResponse(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call",
		mock.MatchedBy(func(tx *ethsigner.Transaction) bool {
			assert.Equal(t, "0x60fe47b100000000000000000000000000000000000000000000000000000000feedbeef", tx.Data.String())
			return true
		}),
		"latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	res, reason, err := c.QueryInvoke(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)
	assert.JSONEq(t, "null", res.Outputs.String())

}

func TestExecQueryCustomErrorRevertData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x391ad4e000000000000000000000000000000000000000000000000000000000000000140000000000000000000000000000000000000000000000000000000000000014")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	expectedError := i18n.NewError(ctx, msgs.MsgReverted, `GreaterThanTen("20","20")`)
	assert.Equal(t, expectedError.Error(), err.Error())

}

func TestExecQueryCustomErrorRevertDataExceedsBalance(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002645524332303a207472616e7366657220616d6f756e7420657863656564732062616c616e63650000000000000000000000000000000000000000000000000000")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	expectedError := i18n.NewError(ctx, msgs.MsgReverted, `Error("ERC20: transfer amount exceeds balance")`)
	assert.Equal(t, expectedError.Error(), err.Error())

}

func TestExecQueryCustomErrorRevertDataNotEnoughEther(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000001a4e6f7420656e6f7567682045746865722070726f76696465642e000000000000")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	expectedError := i18n.NewError(ctx, msgs.MsgReverted, `Error("Not enough Ether provided.")`)
	assert.Equal(t, expectedError.Error(), err.Error())

}

func TestExecQueryCustomErrorRevertDataTransferFromZeroAddress(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002545524332303a207472616e736665722066726f6d20746865207a65726f2061646472657373000000000000000000000000000000000000000000000000000000")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	expectedError := i18n.NewError(ctx, msgs.MsgReverted, `Error("ERC20: transfer from the zero address")`)
	assert.Equal(t, expectedError.Error(), err.Error())

}

func TestExecQueryCustomErrorRevertDataBadOutput(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x053b20290000000000000000000000000000000000000000000000000000000000000020")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	expectedError := i18n.NewError(ctx, msgs.MsgReverted, `0x053b20290000000000000000000000000000000000000000000000000000000000000020`)
	assert.Equal(t, expectedError.Error(), err.Error())

}

func TestExecQueryCustomErrorBadRevertData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Return(&rpcbackend.RPCError{Message: "pop", Data: "bad data"}).
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x053b20290000000000000000000000000000000000000000000000000000000000000020")
		})

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, _, err = c.QueryInvoke(ctx, &req)
	assert.Error(t, err)
	assert.Regexp(t, "pop", err)

}

func TestExecQueryBadErrorsABI(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	req.Errors = []*fftypes.JSONAny{fftypes.JSONAnyPtr(`[`)}
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonInvalidInputs, reason)

}

func TestExecQueryBadRevertData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x08c379a000000000000000000000000000000000000000000000000000000000baadf00d")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Equal(t, ffcapi.ErrorReasonTransactionReverted, reason)
	assert.Regexp(t, "FF23021.*0x08c379a000000000000000000000000000000000000000000000000000000000baadf00d", err)

}

func TestExecQueryBadReturnData(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").
		Run(func(args mock.Arguments) {
			*(args[1].(*ethtypes.HexBytes0xPrefix)) = ethtypes.MustNewHexBytes0xPrefix("0x00000000000000000000000000000000000000000000000000000000baadf00d")
		}).
		Return(nil)

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(`{
			"ffcapi": {
				"version": "v1.0.0",
				"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
				"type": "exec_query"
			},
			"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
			"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
			"nonce": "222",
			"method": {
				"inputs": [],
				"name":"set",
				"outputs":[{"type":"uint256[10]"}],
				"stateMutability":"nonpayable",
				"type":"function"
			},
			"params": [ ]
		}`), &req)
	assert.NoError(t, err)

	_, reason, err := c.QueryInvoke(ctx, &req)
	assert.Empty(t, reason)
	assert.Regexp(t, "FF23023", err)

}

func TestExecQueryFailCall(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_call", mock.Anything, "latest").Return(&rpcbackend.RPCError{Message: "pop"})

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(sampleExecQuery), &req)
	assert.NoError(t, err)
	_, _, err = c.QueryInvoke(ctx, &req)
	assert.Regexp(t, "pop", err)

}

func TestExecQueryFailBadToAddress(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(`{
		"ffcapi": {
			"version": "v1.0.0",
			"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
			"type": "exec_query"
		},
		"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
		"to": "wrong",
		"nonce": "222",
		"method": {
			"inputs": [],
			"name":"set",
			"outputs":[],
			"stateMutability":"nonpayable",
			"type":"function"
		},
		"params": [ ]
	}`), &req)
	assert.NoError(t, err)
	_, _, err = c.QueryInvoke(ctx, &req)
	assert.Regexp(t, "FF23020", err)

}

func TestProcessRevertReasonNestedErrorString(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Outer Error(string) wrapping "outer: " + raw inner Error(string) ABI bytes.
	// Simulates: catch (bytes memory reason) { revert(string.concat("outer: ", string(reason))); }
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a00000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000006b" +
			"6f757465723a20" +
			"08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"0000000000000000000000000000000000000000000000000000000000000013" +
			"696e6e6572206572726f72206d65737361676500000000000000000000000000" +
			"000000000000000000000000000000000000000000")

	result := processRevertReason(ctx, revertData, nil)
	assert.Equal(t, `outer: Error("inner error message")`, result)
}

func TestProcessRevertReasonDoubleNestedErrorString(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Three levels: Error("level1: " + Error("level2: " + Error("deepest error")))
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"00000000000000000000000000000000000000000000000000000000000000cc" +
			"6c6576656c313a20" + // "level1: "
			"08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000006c" +
			"6c6576656c323a20" + // "level2: "
			"08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000000d" +
			"64656570657374206572726f720000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")

	result := processRevertReason(ctx, revertData, nil)
	assert.Equal(t, `level1: level2: Error("deepest error")`, result)
}

func TestProcessRevertReasonNestedCustomError(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Define the custom error ABI first so we can use its real selector
	customErr := &abi.Entry{
		Type: abi.Error,
		Name: "MyCustomError",
		Inputs: abi.ParameterArray{
			{Type: "bytes"},
		},
	}
	customSelector := hex.EncodeToString(customErr.FunctionSelectorBytes())

	// Error("[404]01d - caught bytes:" + MyCustomError(0xdeadbeef) raw ABI bytes)
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000007c" +
			"5b3430345d303164202d206361756768742062797465733a" + // "[404]01d - caught bytes:"
			customSelector +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"0000000000000000000000000000000000000000000000000000000000000004" +
			"deadbeef00000000000000000000000000000000000000000000000000000000" +
			"00000000")

	// With no error ABIs, the custom error can't be decoded — the outer Error(string)
	// is formatted directly (binary content JSON-escaped inside the string).
	result := processRevertReason(ctx, revertData, nil)
	assert.True(t, strings.HasPrefix(result, `Error("[404]01d`))
	assert.NotContains(t, result, "\x00")

	// Now provide the custom error ABI so it CAN be decoded
	result = processRevertReason(ctx, revertData, []*abi.Entry{customErr})
	assert.Equal(t, `[404]01d - caught bytes:MyCustomError("0xdeadbeef")`, result)
}

func TestProcessRevertReasonUnknownNestedBinaryFallback(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Error("[404]01d - caught bytes:" + unknown error selector + binary payload)
	// The embedded selector ac8ae0 is NOT in our error ABIs, so the function
	// falls back to readable prefix + hex-encoded binary remainder.
	revertData := ethtypes.MustNewHexBytes0xPrefix("0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000007b5b3430345d303164202d2063617567687420627974" +
		"65733aac8ae000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000004deadbeef000000000000000000000000000000000000000000000000000000000000000000")

	result := processRevertReason(ctx, revertData, nil)

	// Unknown nested selector: outer Error(string) is still decoded; binary tail is JSON-escaped.
	assert.True(t, strings.HasPrefix(result, `Error("[404]01d`))
	assert.NotContains(t, result, "\x00")
}

func TestProcessRevertReasonPlainStringUnchanged(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// A normal Error(string) with no nested binary data should pass through unchanged
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000001a" +
			"4e6f7420656e6f7567682045746865722070726f76696465642e000000000000")

	result := processRevertReason(ctx, revertData, nil)
	assert.Equal(t, `Error("Not enough Ether provided.")`, result)
}

// ---- processRevertReason behavioral tests ----

func TestProcessRevertReasonNonRevertData(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Data whose length is a multiple of 32 is NOT revert data
	data32 := ethtypes.MustNewHexBytes0xPrefix("0x" + strings.Repeat("ab", 32))
	assert.Equal(t, "", processRevertReason(ctx, data32, nil))

	data64 := ethtypes.MustNewHexBytes0xPrefix("0x" + strings.Repeat("ab", 64))
	assert.Equal(t, "", processRevertReason(ctx, data64, nil))
}

func TestProcessRevertReasonEmptyData(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	assert.Equal(t, "", processRevertReason(ctx, ethtypes.HexBytes0xPrefix{}, nil))
	assert.Equal(t, "", processRevertReason(ctx, nil, nil))
}

func TestProcessRevertReasonBareSelector(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Just 4 bytes — valid error selector but no params to decode
	data := ethtypes.MustNewHexBytes0xPrefix("0x08c379a0")
	result := processRevertReason(ctx, data, nil)
	assert.Equal(t, "0x08c379a0", result)
}

func TestProcessRevertReasonCustomErrorStringParam(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	customErr := &abi.Entry{
		Type:   abi.Error,
		Name:   "LessThanOne",
		Inputs: abi.ParameterArray{{Name: "x", Type: "string"}},
	}
	errData, err := customErr.EncodeCallDataValues([]string{"bad value"})
	assert.NoError(t, err)

	result := processRevertReason(ctx, errData, []*abi.Entry{customErr})
	assert.Equal(t, `LessThanOne("bad value")`, result)
}

func TestProcessRevertReasonCustomErrorAddressParam(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	customErr := &abi.Entry{
		Type:   abi.Error,
		Name:   "Unauthorized",
		Inputs: abi.ParameterArray{{Name: "caller", Type: "address"}},
	}
	errData, err := customErr.EncodeCallDataJSON([]byte(`{"caller":"0x03706Ff580119B130E7D26C5e816913123C24d89"}`))
	assert.NoError(t, err)

	result := processRevertReason(ctx, errData, []*abi.Entry{customErr})
	assert.Equal(t, `Unauthorized("0x03706ff580119b130e7d26c5e816913123c24d89")`, result)
}

func TestProcessRevertReasonMultipleCustomErrorsCorrectMatch(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	errA := &abi.Entry{
		Type:   abi.Error,
		Name:   "ErrAlpha",
		Inputs: abi.ParameterArray{{Type: "uint256"}},
	}
	errB := &abi.Entry{
		Type:   abi.Error,
		Name:   "ErrBeta",
		Inputs: abi.ParameterArray{{Type: "string"}},
	}

	// Encode errB and verify errA doesn't accidentally match
	errData, err := errB.EncodeCallDataValues([]string{"beta triggered"})
	assert.NoError(t, err)
	result := processRevertReason(ctx, errData, []*abi.Entry{errA, errB})
	assert.Equal(t, `ErrBeta("beta triggered")`, result)

	// Now encode errA and verify it matches
	errData, err = errA.EncodeCallDataValues([]string{"42"})
	assert.NoError(t, err)
	result = processRevertReason(ctx, errData, []*abi.Entry{errA, errB})
	assert.Equal(t, `ErrAlpha("42")`, result)
}

func TestProcessRevertReasonUnknownSelectorFallsThrough(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Selector that doesn't match any known error — returns raw hex
	data := ethtypes.MustNewHexBytes0xPrefix(
		"0xdeadbeef" + strings.Repeat("00", 32))

	result := processRevertReason(ctx, data, nil)
	assert.Equal(t, "0xdeadbeef"+strings.Repeat("00", 32), result)
}

func TestProcessRevertReasonErrorSelectorMalformedData(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Error(string) selector but ABI data is garbage — should fall through to hex
	data := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"00000000000000000000000000000000000000000000000000000000baadf00d")

	result := processRevertReason(ctx, data, nil)
	assert.Equal(t, "0x08c379a000000000000000000000000000000000000000000000000000000000baadf00d", result)
}

func TestProcessRevertReasonCustomErrorTruncatedData(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	customErr := &abi.Entry{
		Type:   abi.Error,
		Name:   "NeedsTwoWords",
		Inputs: abi.ParameterArray{{Type: "uint256"}, {Type: "uint256"}},
	}

	// Only 1 word of data — not enough for the error's 2 params
	data := ethtypes.MustNewHexBytes0xPrefix(
		"0x" + hex.EncodeToString(customErr.FunctionSelectorBytes()) +
			strings.Repeat("00", 32))

	result := processRevertReason(ctx, data, []*abi.Entry{customErr})
	assert.True(t, strings.HasPrefix(result, "0x"))
}

func TestProcessRevertReasonNilErrorAbis(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// "hello world!" = 12 bytes, padded to 32.
	// Total: 4 (sel) + 32 (offset) + 32 (len) + 32 (data) = 100. 100%32 = 4 ✓
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000000c" +
			"68656c6c6f20776f726c64210000000000000000000000000000000000000000")

	assert.Equal(t, `Error("hello world!")`, processRevertReason(ctx, revertData, nil))
}

func TestProcessRevertReasonEmptyErrorAbis(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"000000000000000000000000000000000000000000000000000000000000000c" +
			"68656c6c6f20776f726c64210000000000000000000000000000000000000000")

	assert.Equal(t, `Error("hello world!")`, processRevertReason(ctx, revertData, []*abi.Entry{}))
}

func TestProcessRevertReasonRealWorldNestedData(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// Real revert data from the original bug report (the hex from logs).
	// This contains deeply nested Error(string) chains from Solidity catch-and-rethrow
	// with string(reason). The outer Error(string) declares string length 0x73=115 bytes,
	// which captures the first-level prefix "[OCPE]404/98 - " plus the START of the
	// inner Error(string) ABI encoding. The inner encoding declares length 0x212=530 but
	// only ~32 bytes fit in the outer string, so the inner decode correctly fails and
	// the remainder is represented as JSON-escaped bytes inside Error(string).
	//
	// The critical requirement: the output must NOT contain null bytes (\x00) which
	// was the root cause of the PostgreSQL "invalid byte sequence" bug.
	revertData := ethtypes.MustNewHexBytes0xPrefix("0x08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000735b4f4350455d3430342f3938202d2008c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000002125b544d4d5d3430342f3136653a2008c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000001b45b4c544d4d525d3430342f3236713a2008c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000001435b4b44574c5d3430342f313061202d205b4350485d3430342f5b3078616638333233336638626462323834333235386235653234663261326464636133356666323738625d3339613a205b4c4f43435d3430342f3137613a205b4350485d3430342f5b3078393638343634366535383033313539623061396338623163396538646237316361373062643236615d3239633a5b4c4f43535d3430342f32333a08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000555b44574c5d3430342f3737633a205b4b4841415d3430342f303161202d205b4350485d3430332f30343a2030786633363438306137643036356137366666623366366531633939613137313232353464656538353500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")

	result := processRevertReason(ctx, revertData, nil)

	// Critical: must not contain null bytes (the original bug)
	assert.NotContains(t, result, "\x00")

	// First level is decoded to readable text
	assert.Contains(t, result, "[OCPE]404/98")

	// Inner nested fragments appear inside the formatted Error(string) (binary JSON-escaped).
	assert.Contains(t, result, "[TMM]404")
}

func TestProcessRevertReasonCustomErrorWithMultipleParams(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	customErr := &abi.Entry{
		Type: abi.Error,
		Name: "DetailedError",
		Inputs: abi.ParameterArray{
			{Name: "code", Type: "uint256"},
			{Name: "message", Type: "string"},
		},
	}
	errData, err := customErr.EncodeCallDataJSON([]byte(`{"code":404,"message":"not found"}`))
	assert.NoError(t, err)

	result := processRevertReason(ctx, errData, []*abi.Entry{customErr})
	assert.Equal(t, `DetailedError("404","not found")`, result)
}

func TestProcessRevertReasonDefaultErrorTakesPriorityOverCustom(t *testing.T) {
	ctx, _, _, done := newTestConnector(t)
	defer done()

	// "default error msg" = 17 bytes, padded to 32.
	// Total: 4 + 32 + 32 + 32 = 100. 100%32 = 4 ✓
	revertData := ethtypes.MustNewHexBytes0xPrefix(
		"0x08c379a0" +
			"0000000000000000000000000000000000000000000000000000000000000020" +
			"0000000000000000000000000000000000000000000000000000000000000011" +
			"64656661756c74206572726f72206d7367000000000000000000000000000000")

	customErr := &abi.Entry{
		Type:   abi.Error,
		Name:   "SomeOtherError",
		Inputs: abi.ParameterArray{{Type: "uint256"}},
	}
	result := processRevertReason(ctx, revertData, []*abi.Entry{customErr})
	assert.Equal(t, `Error("default error msg")`, result)
}

func TestExecQueryFailBadToParams(t *testing.T) {

	ctx, c, _, done := newTestConnector(t)
	defer done()

	var req ffcapi.QueryInvokeRequest
	err := json.Unmarshal([]byte(`{
		"ffcapi": {
			"version": "v1.0.0",
			"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
			"type": "exec_query"
		},
		"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
		"to": "wrong",
		"nonce": "222",
		"method": {
			"inputs": [],
			"name":"set",
			"outputs":[],
			"stateMutability":"nonpayable",
			"type":"function"
		},
		"params": [ "unexpected extra param" ]
	}`), &req)
	assert.NoError(t, err)
	_, _, err = c.QueryInvoke(ctx, &req)
	assert.Regexp(t, "FF22037", err)

}
