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
	"math/big"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethsigner"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) GasEstimate(ctx context.Context, transaction *ffcapi.TransactionInput) (*ffcapi.GasEstimateResponse, ffcapi.ErrorReason, error) {

	tx := &ethsigner.Transaction{
		Nonce:    (*ethtypes.HexInteger)(transaction.Nonce),
		GasLimit: (*ethtypes.HexInteger)(transaction.Gas),
		Value:    (*ethtypes.HexInteger)(transaction.Value),
	}

	// Parse the from address
	from, err := ethtypes.NewAddress(transaction.From)
	if err != nil {
		return nil, ffcapi.ErrorReasonInvalidInputs, i18n.NewError(ctx, msgs.MsgInvalidFromAddress, transaction.From, err)
	}
	tx.From = json.RawMessage(fmt.Sprintf(`"%s"`, from))

	// Parse the to address - required for preparing an invoke, and must be valid if set
	var to *ethtypes.Address0xHex
	if transaction.To != "" {
		to, err = ethtypes.NewAddress(transaction.To)
		if err != nil {
			return nil, ffcapi.ErrorReasonInvalidInputs, i18n.NewError(ctx, msgs.MsgInvalidToAddress, transaction.To, err)
		}
		tx.To = to
	}

	// Do the gas estimation
	gasEstimate, reason, err := c.gasEstimate(ctx, tx, nil, nil)
	if err != nil {
		return nil, reason, err
	}
	return &ffcapi.GasEstimateResponse{GasEstimate: (*fftypes.FFBigInt)(gasEstimate)}, "", nil
}

func (c *ethConnector) gasEstimate(ctx context.Context, tx *ethsigner.Transaction, method *abi.Entry, errors []*abi.Entry) (*ethtypes.HexInteger, ffcapi.ErrorReason, error) {

	// Do the gas estimation
	var gasEstimate ethtypes.HexInteger
	rpcErr := c.backend.CallRPC(ctx, &gasEstimate, "eth_estimateGas", tx)
	if rpcErr != nil {
		if reason, revertErr := c.attemptProcessingRevertData(ctx, errors, rpcErr); revertErr != nil {
			return nil, reason, revertErr
		}

		// If it fails, fall back to an eth_call to see if we get a reverted reason
		_, reason, errCall := c.callTransaction(ctx, tx, method, errors, nil)
		if reason == ffcapi.ErrorReasonTransactionReverted {
			return nil, reason, errCall
		}
		log.L(ctx).Errorf("Gas estimation failed for a non-revert reason: %s (call result: %v)", rpcErr.Message, errCall)
		// Return the original error - as the eth_call did not give us a revert result (it might even
		// have succeeded). So we need to fall back to the original error.
		return nil, etherrors.MapError(etherrors.CallRPCMethods, rpcErr.Error()), rpcErr.Error()
	}

	// Multiply the gas estimate by the configured factor
	fGasEstimate := new(big.Float).SetInt(gasEstimate.BigInt())
	_ = fGasEstimate.Mul(fGasEstimate, c.gasEstimationFactor)
	scaledLimit, _ := fGasEstimate.Int(nil)

	// If the gas estimate is larger than the maximum gas limit on the block, then we fail at this
	// early stage. Because otherwise we'll accept the transaction in, but fail to actually submit
	// it to the mempool of the blockchain. It's odd that the default configuration of chains (Besu inc.)
	// allow the gas estimate max to be so much higher than the block max - but they do.
	if c.blockListener != nil {
		gasLimit := c.blockListener.GetBlockGasLimit()
		if gasLimit != nil && gasLimit.BigInt().Cmp(scaledLimit) < 0 {
			return nil, "", i18n.NewError(ctx, msgs.MsgTransactionEstimateTooLargeForBlock,
				scaledLimit.String(), c.gasEstimationFactor, gasEstimate.BigInt().String(), gasLimit.BigInt().String())
		}
	}

	return (*ethtypes.HexInteger)(scaledLimit), "", nil
}
