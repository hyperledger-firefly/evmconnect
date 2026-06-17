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
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"golang.org/x/text/language"
)

var ffc = func(key, translation string, fieldType string) i18n.ConfigMessageKey {
	return i18n.FFC(language.AmericanEnglish, key, translation, fieldType)
}

//revive:disable
var (
	_ = ffc("config.connector.url", "URL of JSON/RPC endpoint for the Ethereum node/gateway", "string")
	_ = ffc("config.connector.ws.enabled", "When true a WebSocket is established for block listening, in addition to the HTTP RPC connections used for other functions", i18n.BooleanType)
	_ = ffc("config.connector.dataFormat", "Configure the JSON data format for query output and events", "map,flat_array,self_describing")
	_ = ffc("config.connector.gasEstimationFactor", "The factor to apply to the gas estimation to determine the gas limit", i18n.FloatType)
	_ = ffc("config.connector.blockCacheSize", "Maximum of blocks to hold in the block info cache", i18n.IntType)
	_ = ffc("config.connector.receiptCacheSize", "Maximum of transaction receipts to hold in the receipt cache", i18n.IntType)
	_ = ffc("config.connector.blockPollingInterval", "Interval for polling to check for new blocks", i18n.TimeDurationType)
	_ = ffc("config.connector.queryLoopRetry.initialDelay", "Initial delay for retrying query requests to the RPC endpoint, applicable to all the query loops", i18n.TimeDurationType)
	_ = ffc("config.connector.queryLoopRetry.factor", "Factor to increase the delay by, between each query request retry to the RPC endpoint, applicable to all the query loops", i18n.FloatType)
	_ = ffc("config.connector.queryLoopRetry.maxDelay", "Maximum delay for between each query request retry to the RPC endpoint, applicable to all the query loops", i18n.TimeDurationType)
	_ = ffc("config.connector.retry.initialDelay", "(Deprecated) Please refer to `connector.queryLoopRetry.initialDelay` to understand its original purpose and use that instead", i18n.TimeDurationType)
	_ = ffc("config.connector.retry.factor", "(Deprecated) Please refer to `connector.queryLoopRetry.factor` to understand its original purpose and use that instead", i18n.FloatType)
	_ = ffc("config.connector.retry.maxDelay", "(Deprecated) Please refer to `connector.queryLoopRetry.maxDelay` to understand its original purpose and use that instead", i18n.TimeDurationType)
	_ = ffc("config.connector.events.blockTimestamps", "Whether to include the block timestamps in the event information", i18n.BooleanType)
	_ = ffc("config.connector.events.catchupPageSize", "Number of blocks to query per poll when catching up to the head of the blockchain", i18n.IntType)
	_ = ffc("config.connector.events.catchupThreshold", "How many blocks behind the chain head an event stream or listener must be on startup, to enter catchup mode", i18n.IntType)
	_ = ffc("config.connector.events.catchupDownscaleRegex", "An error pattern to check for from JSON/RPC providers if they limit response sizes to eth_getLogs(). If an error is returned from eth_getLogs() and that error matches the configured pattern, the number of logs requested (catchupPageSize) will be reduced automatically.", "string")
	_ = ffc("config.connector.events.checkpointBlockGap", "The number of blocks at the head of the chain that should be considered unstable (could be dropped from the canonical chain after a re-org). Unless events with a full set of confirmations are detected, the restart checkpoint will this many blocks behind the chain head.", i18n.IntType)
	_ = ffc("config.connector.events.filterPollingInterval", "The interval between polling calls to a filter, when checking for newly arrived events", i18n.TimeDurationType)
	_ = ffc("config.connector.txCacheSize", "Maximum of transactions to hold in the transaction info cache", i18n.IntType)
	_ = ffc("config.connector.maxConcurrentRequests", "Maximum of concurrent requests to be submitted to the blockchain", i18n.IntType)
	_ = ffc("config.connector.hederaCompatibilityMode", "Compatibility mode for Hedera, allowing non-standard block header hashes to be processed", i18n.BooleanType)
	_ = ffc("config.connector.traceTXForRevertReason", "Enable the use of transaction trace functions (e.g. debug_traceTransaction) to obtain transaction revert reasons. This can place a high load on the EVM client.", i18n.BooleanType)
	_ = ffc("config.connector.maxAsyncBlockFetchConcurrency", "Maximum concurrency when using asynchronous block downloading (minium 1)", i18n.IntType)
	_ = ffc("config.connector.useGetBlockReceipts", "When true, the eth_getBlockReceipts call is available for this connector to use", i18n.BooleanType)
	_ = ffc("config.connector.chainTrackingMode", "Tracking mode for connector block progression. light: fetches head block numbers only, does not download block details, disables block listener support, and confirmation results include only the confirmation count. full: fetches head block numbers and block details, maintains an in-memory partial chain, enables block listener support, and confirmation results include both confirmation count and block details.", "`light` or `full`")
)
