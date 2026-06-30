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

package ethereum

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) DeployContractPrepare(ctx context.Context, req *ffcapi.ContractDeployPrepareRequest) (res *ffcapi.TransactionPrepareResponse, reason ffcapi.ErrorReason, err error) {

	// Parse the input JSON data, to build the call data
	callData, constructor, err := c.prepareDeployData(ctx, req)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	// Build the base transaction object
	tx, err := c.buildTx(ctx, txTypeDeployContract, req.From, "", req.Nonce, req.Gas, req.Value, callData)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	// Parse the optional errors JSON spec, if available
	errors, err := buildErrorsABI(ctx, req.Errors)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	if req.Gas, reason, err = c.ensureGasEstimate(ctx, tx, constructor, errors, req.Gas); err != nil {
		return nil, reason, err
	}
	log.L(ctx).Infof("Prepared deploy transaction dataLen=%d gas=%s", len(callData), req.Gas.Int())

	return &ffcapi.TransactionPrepareResponse{
		Gas:             req.Gas,
		TransactionData: ethtypes.HexBytes0xPrefix(callData).String(),
	}, "", nil

}

func (c *ethConnector) prepareDeployData(ctx context.Context, req *ffcapi.ContractDeployPrepareRequest) ([]byte, *abi.Entry, error) {
	// Parse the bytecode as a hex string, or fallback to Base64
	var bytecodeString string
	if err := req.Contract.Unmarshal(ctx, &bytecodeString); err != nil {
		return nil, nil, i18n.NewError(ctx, msgs.MsgDecodeBytecodeFailed)
	}
	bytecode, err := hex.DecodeString(strings.TrimPrefix(bytecodeString, "0x"))
	if err != nil {
		bytecode, err = base64.StdEncoding.DecodeString(bytecodeString)
		if err != nil {
			return nil, nil, i18n.NewError(ctx, msgs.MsgDecodeBytecodeFailed)
		}
	}

	// Parse the ABI
	var a *abi.ABI
	err = json.Unmarshal(req.Definition.Bytes(), &a)
	if err != nil {
		return nil, nil, i18n.NewError(ctx, msgs.MsgUnmarshalABIMethodFail, err)
	}

	// Find the constructor in the ABI
	method := a.Constructor()
	if method == nil {
		// Constructors are optional, so if there is none, simply return the bytecode as the calldata
		return bytecode, nil, nil
	}

	// Parse the params into the standard semantics of Go JSON unmarshalling, with []interface{}
	ethParams := make([]interface{}, len(req.Params))
	for i, p := range req.Params {
		if p != nil {
			err := p.Unmarshal(ctx, &ethParams[i])
			if err != nil {
				return nil, nil, i18n.NewError(ctx, msgs.MsgUnmarshalParamFail, i, err)
			}
		}
	}

	// Match the parameters to the ABI call data for the method.
	// Note the FireFly ABI decoding package handles formatting errors / translation etc.
	var callData []byte
	paramValues, err := method.Inputs.ParseExternalDataCtx(ctx, ethParams)
	if err == nil {
		callData, err = paramValues.EncodeABIData()
	}
	if err != nil {
		return nil, nil, err
	}

	// Concatenate bytecode and constructor args for deployment transaction
	callData = append(bytecode, callData...)

	return callData, method, err
}
