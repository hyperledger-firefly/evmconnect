// Copyright © 2026 Kaleido, Inl.c.
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
	"bytes"
	"context"
	"math"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type eventEnricher struct {
	connector     *ethConnector
	extractSigner bool
}

// We rely on -1 in our logic, so we don't actually support the full range of uint64
func trimUint64(v uint64) int64 {
	if v > math.MaxInt64 {
		panic("block number too large to fit in int64")
	}
	return (int64)(v)
}

func (ee *eventEnricher) filterEnrichEthLog(ctx context.Context, f *eventFilter, methods []*abi.Entry, ethLog *ethrpc.LogJSONRPC) (_ *ffcapi.Event, matched bool, decoded bool, err error) {

	// Check the block for this event is at our high water mark, as we might have rewound for other listeners
	blockNumber := trimUint64(ethLog.BlockNumber.Uint64())
	transactionIndex := trimUint64(ethLog.TransactionIndex.Uint64())
	logIndex := trimUint64(ethLog.LogIndex.Uint64())
	protoID := getEventProtoID(blockNumber, transactionIndex, logIndex)

	// Apply a post-filter check to the event
	topicMatches := len(ethLog.Topics) > 0 && bytes.Equal(ethLog.Topics[0], f.Topic0)
	addrMatches := f.Address == nil || bytes.Equal(ethLog.Address[:], f.Address[:])
	if !topicMatches || !addrMatches {
		log.L(ctx).Debugf("skipping event '%s' topicMatches=%t addrMatches=%t", protoID, topicMatches, addrMatches)
		return nil, matched, decoded, nil
	}
	matched = true

	log.L(ctx).Infof("detected event '%s'", protoID)
	data, decoded := ee.decodeLogData(ctx, f.Event, ethLog.Topics, ethLog.Data)

	if len(ee.connector.chainID) == 0 {
		// by calling IsReady, ee.connector.chainID SHOULD be set to the chain ID when the connector is ready
		resp, _, err := ee.connector.IsReady(ctx)
		if err != nil {
			log.L(ctx).Errorf("Failed to set chain ID due to failed to query chain readiness: %+v", err)
			return nil, matched, decoded, err
		}
		if !resp.Ready {
			log.L(ctx).Errorf("Failed to set chain ID due to the connector is not ready")
			return nil, matched, decoded, i18n.NewError(ctx, msgs.MsgFailedToRetrieveChainID)
		}
	}

	info := eventInfo{
		LogJSONRPC: *ethLog,
		ChainID:    ee.connector.chainID,
	}

	var timestamp *fftypes.FFTime
	if ee.connector.eventBlockTimestamps {
		bi, err := ee.connector.blockListener.GetBlockInfoByHash(ctx, ethLog.BlockHash.String())
		if err != nil {
			log.L(ctx).Errorf("Failed to get block info timestamp for block '%s': %v", ethLog.BlockHash, err)
			return nil, matched, decoded, err // This is an error condition, rather than just something we cannot enrich
		}
		if bi == nil {
			log.L(ctx).Errorf("Failed to get block info timestamp for block '%s': block not found", ethLog.BlockHash)
			return nil, matched, decoded, i18n.NewError(ctx, msgs.MsgBlockNotAvailable)
		}
		timestamp = fftypes.UnixTime(int64(bi.Timestamp.Uint64())) // nolint:gosec // this is what it is - we don't accept negative time, and unix doesn't support full uint64 range
	}

	if len(methods) > 0 || ee.extractSigner {
		txInfo, err := ee.connector.getTransactionInfo(ctx, ethLog.TransactionHash)
		if err != nil {
			log.L(ctx).Errorf("Failed to get transaction info for transaction hash '%s': %v", ethLog.TransactionHash, err)
			return nil, matched, decoded, err // This is an error condition, rather than just something we cannot enrich
		}
		if txInfo == nil {
			log.L(ctx).Errorf("Failed to get transaction info for transaction hash '%s': transaction hash not found", ethLog.TransactionHash)
			return nil, matched, decoded, i18n.NewError(ctx, msgs.MsgFailedToRetrieveTransactionInfo, ethLog.TransactionHash)
		}
		if ee.extractSigner {
			info.InputSigner = txInfo.From
		}
		if len(methods) > 0 {
			ee.matchMethod(ctx, methods, txInfo, &info)
		}
	}

	signature := f.Signature
	return &ffcapi.Event{
		ID: ffcapi.EventID{
			Signature:        signature,
			BlockHash:        ethLog.BlockHash.String(),
			TransactionHash:  ethLog.TransactionHash.String(),
			BlockNumber:      fftypes.FFuint64(ethLog.BlockNumber),
			TransactionIndex: fftypes.FFuint64(ethLog.TransactionIndex),
			LogIndex:         fftypes.FFuint64(ethLog.LogIndex),
			Timestamp:        timestamp,
		},
		Info: &info,
		Data: data,
	}, matched, decoded, nil
}

func (ee *eventEnricher) decodeLogData(ctx context.Context, event *abi.Entry, topics []ethtypes.HexBytes0xPrefix, data ethtypes.HexBytes0xPrefix) (*fftypes.JSONAny, bool) {
	var b []byte
	v, err := event.DecodeEventDataCtx(ctx, topics, data)
	if err == nil {
		b, err = ee.connector.serializer.SerializeJSONCtx(ctx, v)
	}
	if err != nil {
		log.L(ctx).Errorf("Failed to process event log: %s", err)
		return nil, false
	}
	return fftypes.JSONAnyPtrBytes(b), true
}

func (ee *eventEnricher) matchMethod(ctx context.Context, methods []*abi.Entry, txInfo *ethrpc.TxInfoJSONRPC, info *eventInfo) {
	if len(txInfo.Input) < 4 {
		log.L(ctx).Debugf("No function selector available for TX '%s'", txInfo.Hash)
		return
	}
	functionID := txInfo.Input[0:4]
	var method *abi.Entry
	for _, m := range methods {
		if bytes.Equal(m.FunctionSelectorBytes(), functionID) {
			method = m
			break
		}
	}
	if method == nil {
		log.L(ctx).Debugf("Function selector '%s' for TX '%s' does not match any of the supplied methods", functionID.String(), txInfo.Hash)
		return
	}
	info.InputMethod = method.String()
	v, err := method.DecodeCallDataCtx(ctx, txInfo.Input)
	var b []byte
	if err == nil {
		b, err = ee.connector.serializer.SerializeJSONCtx(ctx, v)
	}
	if err != nil {
		log.L(ctx).Warnf("Failed to decode input for TX '%s' using '%s'", txInfo.Hash, info.InputMethod)
		return
	}
	info.InputArgs = fftypes.JSONAnyPtrBytes(b)
}
