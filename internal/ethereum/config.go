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
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

const (
	ConfigGasEstimationFactor   = "gasEstimationFactor"
	ConfigDataFormat            = "dataFormat"
	BlockPollingInterval        = "blockPollingInterval"
	BlockCacheSize              = "blockCacheSize"
	ReceiptCacheSize            = "receiptCacheSize"
	ChainTrackingMode           = "chainTrackingMode"
	EventsCatchupPageSize       = "events.catchupPageSize"
	EventsCatchupThreshold      = "events.catchupThreshold"
	EventsCatchupDownscaleRegex = "events.catchupDownscaleRegex"
	EventsCheckpointBlockGap    = "events.checkpointBlockGap"
	EventsBlockTimestamps       = "events.blockTimestamps"
	EventsFilterPollingInterval = "events.filterPollingInterval"
	RetryInitDelay              = "queryLoopRetry.initialDelay"
	RetryMaxDelay               = "queryLoopRetry.maxDelay"
	RetryFactor                 = "queryLoopRetry.factor"

	DeprecatedRetryInitDelay = "retry.initialDelay"
	DeprecatedRetryMaxDelay  = "retry.maxDelay"
	DeprecatedRetryFactor    = "retry.factor"

	RetryEnabled = "retry.enabled"

	MaxConcurrentRequests         = "maxConcurrentRequests"
	TxCacheSize                   = "txCacheSize"
	HederaCompatibilityMode       = "hederaCompatibilityMode"
	TraceTXForRevertReason        = "traceTXForRevertReason"
	WebSocketsEnabled             = "ws.enabled"
	MaxAsyncBlockFetchConcurrency = "maxAsyncBlockFetchConcurrency"
	UseGetBlockReceipts           = "useGetBlockReceipts"
)

const (
	DefaultListenerPort        = 5102
	DefaultGasEstimationFactor = 1.5

	DefaultCatchupPageSize             = 500
	DefaultEventsCatchupThreshold      = 500
	DefaultEventsCatchupDownscaleRegex = "Response size is larger than.*limit"
	DefaultEventsCheckpointBlockGap    = 50

	DefaultRetryInitDelay   = "100ms"
	DefaultRetryMaxDelay    = "30s"
	DefaultRetryDelayFactor = 2.0
)

func InitConfig(conf config.Section) {
	wsclient.InitConfig(conf)
	conf.AddKnownKey(WebSocketsEnabled, false)
	conf.AddKnownKey(BlockCacheSize, 250)
	conf.AddKnownKey(ReceiptCacheSize, 5000)
	conf.AddKnownKey(BlockPollingInterval, "1s")
	conf.AddKnownKey(ChainTrackingMode, ffcapi.ChainTrackingModeFull)
	conf.AddKnownKey(ConfigDataFormat, "map")
	conf.AddKnownKey(ConfigGasEstimationFactor, DefaultGasEstimationFactor)
	conf.AddKnownKey(EventsBlockTimestamps, true)
	conf.AddKnownKey(EventsFilterPollingInterval, "1s")
	conf.AddKnownKey(EventsCatchupPageSize, DefaultCatchupPageSize)
	conf.AddKnownKey(EventsCatchupThreshold, DefaultEventsCatchupThreshold)
	conf.AddKnownKey(EventsCatchupDownscaleRegex, DefaultEventsCatchupDownscaleRegex)
	conf.AddKnownKey(EventsCheckpointBlockGap, DefaultEventsCheckpointBlockGap)
	conf.AddKnownKey(RetryFactor, DefaultRetryDelayFactor)
	conf.AddKnownKey(RetryInitDelay, DefaultRetryInitDelay)
	conf.AddKnownKey(RetryMaxDelay, DefaultRetryMaxDelay)
	conf.AddKnownKey(DeprecatedRetryFactor)
	conf.AddKnownKey(DeprecatedRetryInitDelay)
	conf.AddKnownKey(DeprecatedRetryMaxDelay)
	conf.AddKnownKey(MaxConcurrentRequests, 50)
	conf.AddKnownKey(TxCacheSize, 250)
	conf.AddKnownKey(HederaCompatibilityMode, false)
	conf.AddKnownKey(TraceTXForRevertReason, false)
	conf.AddKnownKey(MaxAsyncBlockFetchConcurrency, 25)
	conf.AddKnownKey(UseGetBlockReceipts, true /* likely consumers of this package will want to set this default to true */)

	// FireFly Common default for retry enabled is false,
	// but we want to enable it by default
	conf.SetDefault(RetryEnabled, true)
}
