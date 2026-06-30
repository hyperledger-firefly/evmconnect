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
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) TransactionSend(ctx context.Context, req *ffcapi.TransactionSendRequest) (*ffcapi.TransactionSendResponse, ffcapi.ErrorReason, error) {
	var rpcError *rpcbackend.RPCError
	var txHash ethtypes.HexBytes0xPrefix
	if req.PreSigned {
		rpcError = c.backend.CallRPC(ctx, &txHash, "eth_sendRawTransaction", req.TransactionData)
	} else {
		txData, err := hex.DecodeString(strings.TrimPrefix(req.TransactionData, "0x"))
		if err != nil {
			return nil, ffcapi.ErrorReasonInvalidInputs, i18n.NewError(ctx, msgs.MsgInvalidTXData, req.TransactionData, err)
		}

		tx, err := c.buildTx(ctx, txTypePrePrepared, req.From, req.To, req.Nonce, req.Gas, req.Value, txData)
		if err != nil {
			return nil, ffcapi.ErrorReasonInvalidInputs, err
		}

		err = c.mapGasPrice(ctx, req.GasPrice, tx)
		if err != nil {
			return nil, ffcapi.ErrorReasonInvalidInputs, err
		}
		rpcError = c.backend.CallRPC(ctx, &txHash, "eth_sendTransaction", tx)
	}

	if rpcError == nil && len(txHash) != 32 {
		rpcError = &rpcbackend.RPCError{Message: i18n.NewError(ctx, msgs.MsgInvalidTXHashReturned, len(txHash)).Error()}
	}
	if rpcError != nil {
		// send transaction responses never returns error details, only the error message
		// so no need to parse the error data
		return nil, etherrors.MapError(etherrors.SendRPCMethods, rpcError.Error()), rpcError.Error()
	}
	return &ffcapi.TransactionSendResponse{
		TransactionHash: txHash.String(),
	}, "", nil

}

// mapGasPrice handles a variety of inputs from the Transaction Manager policy engine
//
//	sending the FFCAPI request. Specifically:
//	- {"maxFeePerGas": "12345", "maxPriorityFeePerGas": "2345"} - EIP-1559 gas price
//	- {"gasPrice": "12345"} - legacy gas price
//	- "12345" - same as  {"gasPrice": "12345"}
//	- nil - same as {"gasPrice": "0"}
//
// Anything else will return an error
func (c *ethConnector) mapGasPrice(ctx context.Context, input *fftypes.JSONAny, tx *ethsigner.Transaction) error {
	if input == nil {
		tx.GasPrice = ethtypes.NewHexInteger64(0)
		return nil
	}
	log.L(ctx).Debugf("Gas Price json to parse: %s", input)
	gasPriceObject := input.JSONObjectNowarn()
	maxPriorityFeePerGas := (*ethtypes.HexInteger)(gasPriceObject.GetInteger("maxPriorityFeePerGas"))
	maxFeePerGas := (*ethtypes.HexInteger)(gasPriceObject.GetInteger("maxFeePerGas"))
	if maxPriorityFeePerGas.BigInt().Sign() > 0 || maxFeePerGas.BigInt().Sign() > 0 {
		tx.MaxPriorityFeePerGas = maxPriorityFeePerGas
		tx.MaxFeePerGas = maxFeePerGas
		log.L(ctx).Debugf("maxPriorityFeePerGas=%s maxFeePerGas=%s", tx.MaxPriorityFeePerGas, tx.MaxFeePerGas)
		return nil
	}
	tx.GasPrice = (*ethtypes.HexInteger)(gasPriceObject.GetInteger("gasPrice"))
	log.L(ctx).Debugf("maxPriorityFeePerGas=%s maxFeePerGas=%s gasPrice=%s", tx.MaxPriorityFeePerGas, tx.MaxFeePerGas, tx.GasPrice)
	if tx.GasPrice.BigInt().Sign() == 0 {
		err := json.Unmarshal(input.Bytes(), &tx.GasPrice)
		if err != nil {
			return i18n.NewError(ctx, msgs.MsgGasPriceError, input.String())
		}
	}
	log.L(ctx).Debugf("gasPrice=%s", tx.GasPrice)
	return nil
}
