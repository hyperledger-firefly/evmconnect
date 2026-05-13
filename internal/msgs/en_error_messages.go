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

package msgs

import (
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"golang.org/x/text/language"
)

var ffe = func(key, translation string, statusHint ...int) i18n.ErrorMessageKey {
	return i18n.FFE(language.AmericanEnglish, key, translation, statusHint...)
}

//revive:disable
var (
	MsgRequestTypeNotImplemented                = ffe("FF23010", "FFCAPI request '%s' not currently supported")
	MsgBlockNotAvailable                        = ffe("FF23011", "Block not available")
	MsgReceiptNotAvailable                      = ffe("FF23012", "Receipt not available for transaction '%s'")
	MsgUnmarshalABIMethodFail                   = ffe("FF23013", "Failed to parse method ABI: %s")
	MsgUnmarshalParamFail                       = ffe("FF23014", "Failed to parse parameter %d: %s")
	MsgGasPriceError                            = ffe("FF23015", `The gasPrice '%s' could not be parsed. Please supply a numeric string, or an object with 'gasPrice' field, or 'maxFeePerGas'/'maxPriorityFeePerGas' fields (EIP-1559)`)
	MsgInvalidOutputType                        = ffe("FF23016", "Invalid output type: %s")
	MsgInvalidGasPrice                          = ffe("FF23017", "Failed to parse gasPrice '%s': %s")
	MsgInvalidTXData                            = ffe("FF23018", "Failed to parse transaction data as hex '%s': %s")
	MsgInvalidFromAddress                       = ffe("FF23019", "Invalid 'from' address '%s': %s")
	MsgInvalidToAddress                         = ffe("FF23020", "Invalid 'to' address '%s': %s")
	MsgReverted                                 = ffe("FF23021", "EVM reverted: %s")
	MsgReturnDataInvalid                        = ffe("FF23023", "EVM return data invalid: %s")
	MsgNotInitialized                           = ffe("FF23024", "Not initialized")
	MsgMissingBackendURL                        = ffe("FF23025", "URL must be set for the backend JSON/RPC endpoint")
	MsgBadVersion                               = ffe("FF23026", "Bad FFCAPI Version '%s': %s")
	MsgUnsupportedVersion                       = ffe("FF23027", "Unsupported FFCAPI Version '%s'")
	MsgUnsupportedRequestType                   = ffe("FF23028", "Unsupported FFCAPI request type '%s'")
	MsgMissingRequestID                         = ffe("FF23029", "Missing FFCAPI request id")
	MsgUnknownConnector                         = ffe("FF23031", "Unknown connector type: '%s'")
	MsgBadDataFormat                            = ffe("FF23032", "Unknown data format option '%s' supported: %s")
	MsgInvalidListenerOptions                   = ffe("FF23033", "Invalid listener options supplied: %v")
	MsgInvalidFromBlock                         = ffe("FF23034", "Invalid fromBlock '%s'")
	MsgMissingEventFilter                       = ffe("FF23035", "Missing event filter - must specify one or more event filters")
	MsgInvalidEventFilter                       = ffe("FF23036", "Invalid event filter: %s")
	MsgMissingEventInFilter                     = ffe("FF23037", "Each filter must have an 'event' child containing the ABI definition of the event")
	MsgListenerAlreadyStarted                   = ffe("FF23038", "Listener already started: %s")
	MsgInvalidCheckpoint                        = ffe("FF23039", "Invalid checkpoint: %s")
	MsgCacheInitFail                            = ffe("FF23040", "Failed to initialize %s cache")
	MsgStreamNotStarted                         = ffe("FF23041", "Event stream %s not started")
	MsgStreamAlreadyStarted                     = ffe("FF23042", "Event stream %s already started")
	MsgListenerNotStarted                       = ffe("FF23043", "Event listener %s not started in event stream %s")
	MsgListenerNotInitialized                   = ffe("FF23044", "Event listener %s not initialized in event stream %s")
	MsgStreamNotStopped                         = ffe("FF23045", "Event stream %s not stopped")
	MsgTimedOutQueryingChainHead                = ffe("FF23046", "Timed out waiting for chain head block number")
	MsgDecodeBytecodeFailed                     = ffe("FF23047", "Failed to decode 'bytecode' as hex or Base64")
	MsgInvalidTXHashReturned                    = ffe("FF23048", "Received invalid transaction hash from node len=%d")
	MsgUnmarshalErrorFail                       = ffe("FF23049", "Failed to parse error %d: %s")
	MsgUnmarshalABIErrorsFail                   = ffe("FF23050", "Failed to parse errors ABI: %s")
	MsgInvalidRegex                             = ffe("FF23051", "Invalid regular expression for auto-backoff catchup error: %s")
	MsgUnableToCallDebug                        = ffe("FF23052", "Failed to call debug_traceTransaction to get error detail: %s")
	MsgReturnValueNotDecoded                    = ffe("FF23053", "Error return value for custom error: %s")
	MsgReturnValueNotAvailable                  = ffe("FF23054", "Error return value unavailable")
	MsgInvalidProtocolID                        = ffe("FF23055", "Invalid protocol ID in event log: %s")
	MsgFailedToRetrieveChainID                  = ffe("FF23056", "Failed to retrieve chain ID for event enrichment")
	MsgFailedToRetrieveTransactionInfo          = ffe("FF23057", "Failed to retrieve transaction info for transaction hash '%s'")
	MsgFailedToQueryReceipt                     = ffe("FF23058", "Failed to query receipt for transaction %s")
	MsgFailedToQueryBlockInfo                   = ffe("FF23059", "Failed to query block info using hash %s")
	MsgFailedToBuildConfirmationQueue           = ffe("FF23060", "Failed to build confirmation")
	MsgTransactionNotFound                      = ffe("FF23061", "Transaction not found: %s")
	MsgInMemoryPartialChainNotCaughtUp          = ffe("FF23062", "In-memory partial chain is waiting for the transaction block %d (%s) to be indexed")
	MsgFailedToBuildExistingConfirmationInvalid = ffe("FF23063", "Failed to build confirmations, existing confirmations are not valid")
	MsgFromBlockInvalid                         = ffe("FF23064", "From block invalid. Must be 'earliest', 'latest' or a decimal: %s", http.StatusBadRequest)
	MsgInvalidJSONFormatOptions                 = ffe("FF23065", "The JSON formatting options must be a valid set of key=value pairs in URL query string format '%s'")
	MsgUnknownJSONFormatOptions                 = ffe("FF23066", "JSON formatting option unknown %s=%s")
	MsgObservedPanic                            = ffe("FF23067", "Observed panic: %v")
	MsgReturnedBlockHashMismatch                = ffe("FF23068", "Returned block %d hash %s does not match requested hash %s")
	MsgInvalidChainTrackingMode                 = ffe("FF23069", "Invalid chain tracking mode '%s': must be 'light' or 'full'")
	MsgTransactionNotIncludedInChainHead        = ffe("FF23070", "Transaction '%s' cannot be reconciled because chain head %d is before receipt block %s")
	MsgTransactionEstimateTooLargeForBlock      = ffe("FF23071", "Gas estimate %s (scaled at %.2f from estimate %s) too large for the current block gas limit %s")
	MsgMonitoredHeadLengthInvalid               = ffe("FF23072", "Monitored head length must be greater than or equal to 1 value=%d")
)
