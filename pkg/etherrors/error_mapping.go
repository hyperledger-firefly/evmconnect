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

package etherrors

import (
	"strings"

	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type RPCMethodCategory int

const (
	FilterRPCMethods RPCMethodCategory = iota
	SendRPCMethods
	CallRPCMethods
	BlockRPCMethods
	NetVersionRPCMethods
)

// etherrors.MapErrorToReason provides a common place for mapping Ethereum client
// error strings, to a more consistent set of cross-client (and
// cross blockchain) reasons for errors defined by FFCPI for use by
// FireFly Transaction Manager.
//
// Sadly there is no place in Ethereum JSON/RPC where these are
// formally defined. So this logic may get complex over time to
// deal with the differences between client implementations.
func MapError(methodType RPCMethodCategory, err error) ffcapi.ErrorReason {

	errString := strings.ToLower(err.Error())

	switch methodType {
	case FilterRPCMethods:
		if strings.Contains(errString, "filter not found") {
			return ffcapi.ErrorReasonNotFound
		}
	case SendRPCMethods:
		switch {
		case strings.Contains(errString, "nonce too low"):
			return ffcapi.ErrorReasonNonceTooLow
		case strings.Contains(errString, "insufficient funds"):
			return ffcapi.ErrorReasonInsufficientFunds
		case strings.Contains(errString, "transaction underpriced"):
			return ffcapi.ErrorReasonTransactionUnderpriced
		case strings.Contains(errString, "known transaction"):
			return ffcapi.ErrorKnownTransaction
		case strings.Contains(errString, "already known"):
			return ffcapi.ErrorKnownTransaction
		}
	case CallRPCMethods:
		if strings.Contains(errString, "execution reverted") {
			return ffcapi.ErrorReasonTransactionReverted
		}
	case BlockRPCMethods:
		// https://docs.avax.network/quickstart/integrate-exchange-with-avalanche#determining-finality
		if strings.Contains(errString, "cannot query unfinalized data") {
			return ffcapi.ErrorReasonNotFound
		}
	case NetVersionRPCMethods:
		if strings.Contains(errString, "the method net_version does not exist/is not available") {
			return ffcapi.ErrorReasonNotFound
		}
	}

	// Best default in FFCAPI is to provide no mapping
	return ""
}
