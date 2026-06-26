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
	"encoding/json"
	"fmt"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type txType int

const (
	txTypeQuery txType = iota
	txTypeDeployContract
	txTypeInvokeContract
	txTypePrePrepared
)

func (c *ethConnector) TransactionPrepare(ctx context.Context, req *ffcapi.TransactionPrepareRequest) (res *ffcapi.TransactionPrepareResponse, reason ffcapi.ErrorReason, err error) {

	// Parse the input JSON data, to build the call data
	callData, method, err := c.prepareCallData(ctx, &req.TransactionInput)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	// Build the base transaction object
	tx, err := c.buildTx(ctx, txTypeInvokeContract, req.From, req.To, req.Nonce, req.Gas, req.Value, callData)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	// Parse the optional errors JSON spec, if available
	errors, err := buildErrorsABI(ctx, req.TransactionInput.Errors)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, err
	}

	if req.Gas, reason, err = c.ensureGasEstimate(ctx, tx, method, errors, req.Gas); err != nil {
		return nil, reason, err
	}
	log.L(ctx).Infof("Prepared transaction method=%s dataLen=%d gas=%s", method.String(), len(callData), req.Gas.Int())

	return &ffcapi.TransactionPrepareResponse{
		Gas:             req.Gas,
		TransactionData: ethtypes.HexBytes0xPrefix(callData).String(),
	}, "", nil

}

func buildErrorsABI(ctx context.Context, errorSpecs []*fftypes.JSONAny) ([]*abi.Entry, error) {
	errors := make([]*abi.Entry, len(errorSpecs))
	for i, e := range errorSpecs {
		err := json.Unmarshal(e.Bytes(), &errors[i])
		if err != nil {
			return nil, i18n.NewError(ctx, msgs.MsgUnmarshalABIErrorsFail, err)
		}
	}
	return errors, nil
}

func (c *ethConnector) ensureGasEstimate(ctx context.Context, tx *ethsigner.Transaction, method *abi.Entry, errors []*abi.Entry, gasRequest *fftypes.FFBigInt) (*fftypes.FFBigInt, ffcapi.ErrorReason, error) {
	if gasRequest == nil || gasRequest.Int().Sign() == 0 {
		// If a value for gas has not been supplied, do a gas estimate
		gas, reason, err := c.gasEstimate(ctx, tx, method, errors)
		if err != nil {
			return nil, reason, err
		}
		gasRequest = (*fftypes.FFBigInt)(gas)
	}
	return gasRequest, ffcapi.ErrorReason(""), nil
}

func (c *ethConnector) prepareCallData(ctx context.Context, req *ffcapi.TransactionInput) ([]byte, *abi.Entry, error) {

	// Parse the method ABI
	var method *abi.Entry
	err := json.Unmarshal(req.Method.Bytes(), &method)
	if err != nil {
		return nil, nil, i18n.NewError(ctx, msgs.MsgUnmarshalABIMethodFail, err)
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
		callData, err = method.EncodeCallDataCtx(ctx, paramValues)
	}
	if err != nil {
		return nil, nil, err
	}

	return callData, method, err
}

func (c *ethConnector) buildTx(ctx context.Context, txType txType, fromString, toString string, nonce, gas, value *fftypes.FFBigInt, data []byte) (tx *ethsigner.Transaction, err error) {
	tx = &ethsigner.Transaction{
		Nonce:    (*ethtypes.HexInteger)(nonce),
		GasLimit: (*ethtypes.HexInteger)(gas),
		Value:    (*ethtypes.HexInteger)(value),
		Data:     data,
	}

	// Parse the from address
	from, err := ethtypes.NewAddress(fromString)
	if err != nil {
		if txType != txTypeQuery {
			// ignore the error if query, from is optional for query
			return nil, i18n.NewError(ctx, msgs.MsgInvalidFromAddress, fromString, err)
		}
	} else {
		tx.From = json.RawMessage(fmt.Sprintf(`"%s"`, from))
	}

	// Parse the to address - required for preparing an invoke, and must be valid if set
	var to *ethtypes.Address0xHex
	if txType != txTypeDeployContract && (txType != txTypePrePrepared || toString != "") {
		to, err = ethtypes.NewAddress(toString)
		if err != nil {
			return nil, i18n.NewError(ctx, msgs.MsgInvalidToAddress, toString, err)
		}
		tx.To = to
	}

	return tx, nil

}
