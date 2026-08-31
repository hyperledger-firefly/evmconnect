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
	"context"
	"encoding/json"
	"math/big"
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// listenerCheckpoint is our Ethereum specific custom options that can be specified when creating a listener
type listenerOptions struct {
	Methods []*abi.Entry `json:"methods,omitempty"` // An optional array of ABI methods. If specified and the input data for a transaction matches, the decoded inputs will be included in the event
	Signer  bool         `json:"signer,omitempty"`  // An optional boolean for whether to extract the signer of the transaction that emitted the event
}

// listenerCheckpoint is our Ethereum specific checkpoint structure
type listenerCheckpoint struct {
	Block            int64 `json:"block"`
	TransactionIndex int64 `json:"transactionIndex"`
	LogIndex         int64 `json:"logIndex"`
}

// listenerConfig is the configuration parsed from generic FFCAPI connector framework JSON, into our Ethereum specific options
type listenerConfig struct {
	name      string
	fromBlock string
	options   *listenerOptions
	filters   []*eventFilter
	signature string
}

// listener is the state we hold in memory for each individual listener that has been added
type listener struct {
	id              *fftypes.UUID
	c               *ethConnector
	es              *eventStream
	ee              *eventEnricher
	hwmMux          sync.Mutex          // Protects hwmBlock and lastDetected. May hold ES lock when taking this, must NOT attempt to obtain ES lock while holding this
	hwmBlock        int64               // the scan position - the block we have polled for events up to (exclusive)
	lastDetected    *listenerCheckpoint // the checkpoint of the highest event we have pushed to FFTM - see ffcapi.EventListenerHWMResponse
	config          listenerConfig
	removed         bool
	catchup         bool
	catchupLoopDone chan struct{}
}

func (cp *listenerCheckpoint) LessThan(b ffcapi.EventListenerCheckpoint) bool {
	bcp := b.(*listenerCheckpoint)
	return cp.Block < bcp.Block ||
		(cp.Block == bcp.Block &&
			(cp.TransactionIndex < bcp.TransactionIndex ||
				(cp.TransactionIndex == bcp.TransactionIndex && (cp.LogIndex < bcp.LogIndex))))
}

func (l *listener) getInitialBlock(ctx context.Context, fromBlockInstruction string) (uint64, error) {
	if fromBlockInstruction == ffcapi.FromBlockLatest || fromBlockInstruction == "" {
		// Get the latest block number of the chain
		chainHead, ok := l.c.blockListener.GetHighestBlock(ctx)
		if !ok {
			return 0, i18n.NewError(ctx, msgs.MsgTimedOutQueryingChainHead)
		}
		return chainHead, nil
	}
	num, ok := new(big.Int).SetString(fromBlockInstruction, 0)
	if !ok {
		return 0, i18n.NewError(ctx, msgs.MsgInvalidFromBlock, fromBlockInstruction)
	}
	return num.Uint64(), nil
}

func parseListenerOptions(ctx context.Context, o *fftypes.JSONAny) (*listenerOptions, error) {
	var options listenerOptions
	if o != nil {
		err := json.Unmarshal(o.Bytes(), &options)
		if err != nil {
			return nil, i18n.NewError(ctx, msgs.MsgInvalidListenerOptions, err)
		}
	}
	return &options, nil
}

func (l *listener) ensureHWM(ctx context.Context) error {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	if l.hwmBlock < 0 {
		firstBlock, err := l.getInitialBlock(ctx, l.config.fromBlock)
		if err != nil {
			log.L(ctx).Errorf("Failed to initialize listener: %s", err)
			return err
		}
		// HWM is the configured fromBlock
		l.hwmBlock = int64(firstBlock) //nolint:gosec // convert to int64 to match the type of hwmBlock, we should change the type of hwmBlock to uint64
	}
	return nil
}

func (l *listener) checkReadyForLeadPackOrRemoved(ctx context.Context) (bool, bool) {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	// The head block is atomic, as we cannot take the eventStream lock here (our caller may or
	// may not already hold it in the mutex hierarchy)
	headBlock := l.es.headBlock.Load()
	blockGap := headBlock - l.hwmBlock
	readyForLead := headBlock >= 0 && blockGap < l.c.catchupThreshold
	log.L(ctx).Debugf("Listener %s head=%d hwm=%d (gap=%d) readyForLead=%t", l.id, headBlock, l.hwmBlock, blockGap, readyForLead)
	return readyForLead, l.removed
}

// getHWM returns under the hmwMux lock as consistent set of:
// 1. Scan position - where event polling is up to, for detection of new events
// 2. Last detected - the checkpoint of the highest event pushed to the FFTM channel (or nil)
// See ffcapi.EventListenerHWMResponse for the contract defined by FFTM for these
func (l *listener) getHWM() (scanned ffcapi.EventListenerCheckpoint, lastDetected ffcapi.EventListenerCheckpoint) {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	// Generate a checkpoint before the first transaction, in the high watermark block
	log.L(l.es.ctx).Debugf("HWM checkpoint block for '%s': %d (lastDetected=%+v)", l.id, l.hwmBlock, l.lastDetected)
	scanned = &listenerCheckpoint{
		Block:            l.hwmBlock,
		TransactionIndex: -1,
		LogIndex:         -1,
	}
	if l.lastDetected != nil {
		lastDetected = l.lastDetected
	}
	return scanned, lastDetected
}

// markDetected records the checkpoint of an event, and must be called pushing the event to FFTM
func (l *listener) markDetected(cp *listenerCheckpoint) {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	// Only ever move forwards - a re-detection (such as after a re-org, or a filter reset) must not
	// lower the bar FFTM uses to decide the scan position is safe to record as a checkpoint.
	if l.lastDetected == nil || l.lastDetected.LessThan(cp) {
		l.lastDetected = cp
	}
}

func (l *listener) moveHWMForwards(hwmBlock int64) {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	if hwmBlock > l.hwmBlock {
		l.hwmBlock = hwmBlock // check against moving backwards
	}
}

func (l *listener) getHWMBlock() int64 {
	l.hwmMux.Lock()
	defer l.hwmMux.Unlock()
	return l.hwmBlock
}

// listenerCatchupLoop reads pages of blocks at a time, until it gets within the configured catchup-threshold
// of the head of the blockchain.
// Then it moves this listener into the head-set of listeners, which share a common filter, listening
// for new events to arrive at the head of the chain.
func (l *listener) listenerCatchupLoop() {
	defer close(l.catchupLoopDone)

	// Only filtering on a single listener
	ctx := log.WithLogFields(l.es.ctx, "listener", l.id.String())
	al := l.es.buildAggregatedListener([]*listener{l})

	failCount := 0
	for {
		if l.c.retry.DoFailureDelay(ctx, failCount) {
			log.L(ctx).Debugf("Listener catchup loop loop exiting")
			return
		}

		readyForLead, removed := l.checkReadyForLeadPackOrRemoved(ctx)
		if removed {
			log.L(ctx).Infof("Listener removed during catchup")
			return
		}
		if readyForLead {
			// We're done with catchup for this listener - it can join the main group
			l.es.rejoinLeadGroup(l)
			log.L(ctx).Infof("Listener completed catchup, and rejoined lead group")
			return
		}

		// Never advance our HWM past the position of the lead group - we must join it rather than
		// scan past it, otherwise we mark the re-org unstable blocks it deliberately holds back
		// from as already scanned, and can never re-detect events a re-org introduces into them.
		headBlock, established := l.es.catchupCeiling()
		fromBlock := l.getHWMBlock()
		toBlock := fromBlock + l.c.catchupPageSize - 1
		if established && toBlock >= headBlock {
			toBlock = headBlock - 1 // the resulting HWM (toBlock+1) is at most headBlock
		}
		if !established || fromBlock > toBlock {
			// Either the lead group's position is not yet established - in which case we are
			// deliberately held in catchup (see checkReadyForLeadPackOrRemoved) - or we have caught
			// up to it and are waiting to be classified as ready to join it. Either way, do not scan.
			select {
			case <-time.After(l.c.eventFilterPollingInterval):
				continue
			case <-ctx.Done():
				log.L(ctx).Infof("Listener catchup loop exiting as stream is stopping")
				return
			}
		}
		events, err := l.es.getBlockRangeEvents(ctx, al, fromBlock, toBlock)
		if err != nil {
			if l.c.catchupDownscaleRegex.String() != "" && l.c.catchupDownscaleRegex.MatchString(err.Error()) {
				log.L(ctx).Warnf("Failed to query block range fromBlock=%d toBlock=%d. Error %s matches configured downscale regex, catchup page size will automatically be reduced", fromBlock, toBlock, err.Error())
				if l.c.catchupPageSize > 1 {
					l.c.catchupPageSize /= 2

					if l.c.catchupPageSize < 20 {
						log.L(ctx).Warnf("Catchup page size auto-reduced to extremely low value %d. The connector may never catch up with the head of the chain.", l.c.catchupPageSize)
					}
				}
			} else {
				log.L(ctx).Errorf("Failed to query block range fromBlock=%d toBlock=%d: %s", fromBlock, toBlock, err)
			}
			failCount++ // for exponential backoff calculation
			continue
		}
		log.L(ctx).Infof("Listener catchup fromBlock=%d toBlock=%d events=%d", fromBlock, toBlock, len(events))

		for _, event := range events {
			log.L(ctx).Debugf("Detected event %s (listener catchup)", event.Event)
			select {
			case l.es.events <- event:
			case <-l.es.ctx.Done():
				log.L(ctx).Infof("Listener catchup loop exiting as stream is stopping")
				return
			}
		}
		l.moveHWMForwards(toBlock + 1)
		failCount = 0 // Reset on success
	}
}

func (l *listener) filterEnrichEthLog(ctx context.Context, f *eventFilter, methods []*abi.Entry, ethLog *ethrpc.LogJSONRPC) (*ffcapi.ListenerEvent, bool, error) {

	// Check the block for this event is at our high water mark, as we might have rewound for other listeners
	blockNumber := trimUint64(ethLog.BlockNumber.Uint64())
	transactionIndex := trimUint64(ethLog.TransactionIndex.Uint64())
	logIndex := trimUint64(ethLog.LogIndex.Uint64())
	hwmBlock := l.getHWMBlock()
	if blockNumber < hwmBlock {
		log.L(ctx).Debugf("Listener %s already delivered event '%s' hwm=%d", l.id, getEventProtoID(blockNumber, transactionIndex, logIndex), hwmBlock)
		return nil, false, nil
	}

	e, matched, _, err := l.ee.filterEnrichEthLog(ctx, f, methods, ethLog)
	if !matched || err != nil || e == nil {
		return nil, false, err
	}

	e.ID.ListenerID = l.id
	return &ffcapi.ListenerEvent{
		Checkpoint: &listenerCheckpoint{
			Block:            blockNumber,
			TransactionIndex: transactionIndex,
			LogIndex:         logIndex,
		},
		Event: e,
	}, true, nil
}
