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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/abi"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// eventFilter is our Ethereum specific filter options - an array of these can be configured on each listener
type eventFilter struct {
	Event     *abi.Entry                `json:"event"`             // The ABI spec of the event to listen to
	Address   *ethtypes.Address0xHex    `json:"address,omitempty"` // An optional address to restrict the
	Topic0    ethtypes.HexBytes0xPrefix `json:"topic0"`            // Topic 0 match
	Signature string                    `json:"signature"`         // The cached signature of this event
}

// eventInfo is the top-level structure we pass to applications for each event (through the FFCAPI framework)
type eventInfo struct {
	ethrpc.LogJSONRPC
	InputMethod string                 `json:"inputMethod,omitempty"` // the method invoked, if it matched one of the signatures in the listener definition
	InputArgs   *fftypes.JSONAny       `json:"inputArgs,omitempty"`   // the method parameters, if the method matched one of the signatures in the listener definition
	InputSigner *ethtypes.Address0xHex `json:"inputSigner,omitempty"` // the signing `from` address of the transaction
	ChainID     string                 `json:"chainId,omitempty"`     // an identifier for the chain this event relates to
}

// eventStream is the state we hold in memory for each eventStream
type eventStream struct {
	id             *fftypes.UUID
	ctx            context.Context
	c              *ethConnector
	events         chan<- *ffcapi.ListenerEvent
	mux            sync.Mutex
	updateCount    int
	listeners      map[fftypes.UUID]*listener
	headBlock      int64
	streamLoopDone chan struct{}
	catchup        bool
}

// aggregatedListener is a generated structure that allows use to query/filter logs efficiently across a large number of listeners,
// while minimizing the number of JSON/RPC calls we need to make to the node/gateway.  This is very important when dealing with an
// industrial scale of listeners, that might share event signatures. For example listening to 1000 different "transfer" events for
// different contract addresses.
type aggregatedListener struct {
	signatureSet      []ethtypes.HexBytes0xPrefix // a list of unique topic[0] event signatures to listener for
	listenersByTopic0 map[string][]*listener      // a map of all listeners that are interested in an event signature - they may not be interested in the event itself (depending on sub-selection)
	listeners         []*listener                 // list of all listeners
}

func parseEventFilters(ctx context.Context, filters []fftypes.JSONAny) (string, []*eventFilter, error) {
	if len(filters) < 1 {
		return "", nil, i18n.NewError(ctx, msgs.MsgMissingEventFilter)
	}
	ethFilters := make([]*eventFilter, len(filters))
	sigStrings := make([]string, len(filters))
	for i, f := range filters {
		err := json.Unmarshal(f.Bytes(), &ethFilters[i])
		if err != nil {
			return "", nil, i18n.NewError(ctx, msgs.MsgInvalidEventFilter, f.Bytes())
		}
		if ethFilters[i].Event == nil {
			return "", nil, i18n.NewError(ctx, msgs.MsgMissingEventFilter)
		}
		ethFilters[i].Topic0, err = ethFilters[i].Event.SignatureHashCtx(ctx)
		ethFilters[i].Signature = ethFilters[i].Event.String()
		if err != nil {
			return "", nil, i18n.NewError(ctx, msgs.MsgInvalidEventFilter, err)
		}
		if ethFilters[i].Address != nil {
			sigStrings[i] = ethFilters[i].Address.String() + ":" + ethFilters[i].Event.String()
		} else {
			sigStrings[i] = "*:" + ethFilters[i].Event.String()
		}
	}
	var signature string
	if len(sigStrings) == 1 {
		signature = sigStrings[0]
	} else {
		signature = "[" + strings.Join(sigStrings, ",") + "]"
	}
	return signature, ethFilters, nil
}

func (es *eventStream) addEventListener(ctx context.Context, req *ffcapi.EventListenerAddRequest) (*listener, error) {
	es.mux.Lock()
	defer es.mux.Unlock()
	_, ok := es.listeners[*req.ListenerID]
	if ok {
		return nil, i18n.NewError(ctx, msgs.MsgListenerAlreadyStarted, req.ListenerID)
	}

	var checkpoint *listenerCheckpoint
	if req.Checkpoint != nil {
		checkpoint = req.Checkpoint.(*listenerCheckpoint)
	}

	signature, filters, err := parseEventFilters(ctx, req.Filters)
	if err != nil || req.Options == nil {
		// Should not happen as we've previously been called with EventListenerVerifyOptions
		return nil, i18n.NewError(ctx, msgs.MsgInvalidListenerOptions, err)
	}

	options, err := parseListenerOptions(ctx, req.Options)
	if err != nil {
		return nil, err
	}

	l := &listener{
		id:       req.ListenerID,
		c:        es.c,
		es:       es,
		hwmBlock: -1,
		config: listenerConfig{
			name:      req.Name,
			fromBlock: req.FromBlock,
			options:   options,
			filters:   filters,
			signature: signature,
		},
	}
	l.ee = &eventEnricher{
		connector:     l.c,
		extractSigner: l.config.options.Signer,
	}
	if checkpoint != nil {
		l.hwmBlock = checkpoint.Block
	}
	if err := l.ensureHWM(ctx); err != nil {
		return nil, err
	}
	log.L(es.ctx).Infof("Initialized listener '%s' (FromBlock=%s) Block=%d Checkpoint=%+v", l.id, l.config.fromBlock, l.hwmBlock, checkpoint)

	es.updateCount++
	es.listeners[*req.ListenerID] = l

	return l, nil
}

func (es *eventStream) startEventListener(l *listener) {
	readyForLead, removed := l.checkReadyForLeadPackOrRemoved(es.ctx)
	l.catchup = !readyForLead
	if l.catchup && !removed {
		l.catchupLoopDone = make(chan struct{})
		go l.listenerCatchupLoop()
	}
}

func (es *eventStream) removeEventListener(listenerID *fftypes.UUID) {
	es.mux.Lock()
	defer es.mux.Unlock()

	l := es.listeners[*listenerID]
	if l != nil {
		es.updateCount++
		delete(es.listeners, *listenerID)
		l.hwmMux.Lock()
		l.removed = true
		l.hwmMux.Unlock()
		log.L(es.ctx).Infof("Listener '%s' removed", listenerID)
	}
}

func (es *eventStream) rejoinLeadGroup(l *listener) {
	l.es.mux.Lock()
	defer l.es.mux.Unlock()
	l.es.updateCount++
	l.catchup = false
}

func (es *eventStream) buildReuseLeadGroupListener(lastUpdate *int, ag **aggregatedListener) bool {
	es.mux.Lock()
	defer es.mux.Unlock()
	listenerChanged := false
	if *lastUpdate != es.updateCount {
		listeners := make([]*listener, 0, len(es.listeners))
		for _, l := range es.listeners {
			if !l.catchup {
				listeners = append(listeners, l)
			}
		}
		*ag = es.buildAggregatedListener(listeners)
		listenerChanged = true
		*lastUpdate = es.updateCount
	}
	return listenerChanged
}

// leadGroupCatchup is called whenever the steam loop restarts, to see how far it is behind the head of the
// chain and if it's a way behind then we catch up all this head group as one set (rather than with individual
// catchup routines as is the case if one listener starts a way behind the pack)
func (es *eventStream) leadGroupCatchup() bool {

	// For API status, we keep a track of whether we're in catchup mode or not
	es.catchup = true
	defer func() { es.catchup = false }()

	var ag *aggregatedListener
	lastUpdate := -1
	failCount := 0
	for {
		if es.c.retry.DoFailureDelay(es.ctx, failCount) {
			log.L(es.ctx).Debugf("Stream catchup loop exiting")
			return true
		}

		chainHeadBlock, ok := es.c.blockListener.GetHighestBlock(es.ctx)
		if !ok {
			log.L(es.ctx).Debugf("Stream catchup exiting (closed checking block height)")
			return true
		}

		// Build the aggregated listener list (doesn't matter if it's changed, as we build the list each time)
		_ = es.buildReuseLeadGroupListener(&lastUpdate, &ag)

		if len(ag.listeners) == 0 {
			log.L(es.ctx).Infof("Lead group is currently empty")
			return false
		}

		// Determine the earliest block we need to poll from
		fromBlock := int64(-1)
		for _, l := range ag.listeners {
			if fromBlock < 0 || l.hwmBlock < fromBlock {
				fromBlock = l.hwmBlock
			}
		}

		// Check if we're ready to exit catchup mode
		headGap := (int64(chainHeadBlock) - fromBlock) //nolint:gosec // convert to int64 to match the type of headGap
		if headGap < es.c.catchupThreshold {
			log.L(es.ctx).Infof("Stream head is up to date with chain fromBlock=%d chainHead=%d headGap=%d", fromBlock, chainHeadBlock, headGap)
			return false
		}

		// Poll in the range for events
		toBlock := fromBlock + es.c.catchupPageSize - 1
		events, err := es.getBlockRangeEvents(es.ctx, ag, fromBlock, toBlock)
		if err != nil {
			log.L(es.ctx).Errorf("Failed to query block range fromBlock=%d toBlock=%d headBlock=%d: %s", fromBlock, toBlock, chainHeadBlock, err)
			failCount++
			continue
		}
		log.L(es.ctx).Infof("Stream catchup fromBlock=%d toBlock=%d headBlock=%d events=%d listeners=%d", fromBlock, toBlock, chainHeadBlock, len(events), len(ag.listeners))

		// Dispatch the events
		if es.dispatchSetHWMCheckExit(ag, events, toBlock+1 /* hwm is the next block after our poll */) {
			log.L(es.ctx).Debugf("Stream catchup loop exiting")
			return true
		}

		// Reset retry count for a successful loop
		failCount = 0
	}

}

func (es *eventStream) uninstallFilter(filter *string) {
	if *filter != "" {
		var res bool
		if err := es.c.backend.CallRPC(es.ctx, &res, "eth_uninstallFilter", filter); err != nil {
			log.L(es.ctx).Warnf("Error uninstalling filter '%v': %s", filter, err.Message)
		} else {
			log.L(es.ctx).Debugf("Uninstalled filter '%v': %t", filter, res)
		}
		*filter = ""
	}
}

func (es *eventStream) leadGroupSteadyState() bool {
	var filter string
	defer es.uninstallFilter(&filter)

	// Then we move into the head mode, where we establish a long-lived filter, and keep polling for changes on it.
	var ag *aggregatedListener
	lastUpdate := -1
	failCount := 0
	filterResetRequired := false
	filterRPCMethodToUse := ""
	for {
		if es.c.retry.DoFailureDelay(es.ctx, failCount) {
			log.L(es.ctx).Debugf("Stream loop exiting")
			return true
		}

		// Build the aggregated listener list if it has changed
		listenerChanged := es.buildReuseLeadGroupListener(&lastUpdate, &ag) || filterResetRequired

		// No need to poll for events, if we don't have any listeners
		if len(ag.signatureSet) > 0 {

			// High water mark is a point safely behind the head of the chain in this case,
			// where re-orgs are not expected.
			bh, _ := es.c.blockListener.GetHighestBlock(es.ctx) /* note we know we're initialized here and will not block */
			hwmBlock := int64(bh) - es.c.checkpointBlockGap     //nolint:gosec // convert to int64 to match the type of hwmBlock
			if hwmBlock < 0 {
				hwmBlock = 0
			}

			// Re-establish the filter if we need to
			if filter == "" || listenerChanged {
				// Uninstall any existing filter
				if filter != "" {
					es.uninstallFilter(&filter)
				}
				filterResetRequired = false
				filterRPCMethodToUse = "eth_getFilterLogs" // first JSON/RPC for a new filter ID fetches all the historical logs to ensure no gaps
				// Determine the earliest block we need to poll from
				fromBlock := int64(-1)
				for _, l := range ag.listeners {
					if fromBlock < 0 || l.hwmBlock < fromBlock {
						fromBlock = l.hwmBlock
					}
				}

				// Check we're not outside of the steady state window, and need to fall back to catchup mode
				chainHeadBlock, _ := es.c.blockListener.GetHighestBlock(es.ctx) /* note we know we're initialized here and will not block */
				blockGapEstimate := (int64(chainHeadBlock) - fromBlock)         //nolint:gosec // convert to int64 to match the type of blockGapEstimate
				if blockGapEstimate > es.c.catchupThreshold {
					log.L(es.ctx).Warnf("Block gap estimate reached %d (above threshold of %d) - reverting to catchup mode", blockGapEstimate, es.c.catchupThreshold)
					return false
				}

				// Create the new filter
				err := es.c.backend.CallRPC(es.ctx, &filter, "eth_newFilter", &ethrpc.LogFilterJSONRPC{
					FromBlock: ethtypes.NewHexInteger64(fromBlock),
					Topics: [][]ethtypes.HexBytes0xPrefix{
						ag.signatureSet,
					},
				})
				// If we fail to create the filter, we need to keep retrying
				if err != nil {
					log.L(es.ctx).Errorf("Failed to establish filter: %s", err.Message)
					failCount++
					continue
				}
				log.L(es.ctx).Infof("Filter '%v' established", filter)
			}
			// Get the next batch of logs
			var ethLogs []*ethrpc.LogJSONRPC
			rpcErr := es.c.backend.CallRPC(es.ctx, &ethLogs, filterRPCMethodToUse, filter)
			// If we fail to query we just retry - setting filter to nil if not found
			if rpcErr != nil {
				if etherrors.MapError(etherrors.FilterRPCMethods, rpcErr.Error()) == ffcapi.ErrorReasonNotFound {
					log.L(es.ctx).Infof("Filter '%v' reset: %s", filter, rpcErr.Message)
					filter = ""
				}
				log.L(es.ctx).Errorf("Failed to query filter (%s): %s", filterRPCMethodToUse, rpcErr.Message)
				failCount++
				continue
			}
			filterRPCMethodToUse = "eth_getFilterChanges" // subsequent JSON/RPC calls after the initial fetch, this fetches only the new logs
			// Enrich the events
			events, enrichErr := es.filterEnrichSort(es.ctx, ag, ethLogs)
			if enrichErr != nil {
				log.L(es.ctx).Errorf("Failed to enrich events: %v", enrichErr)
				// We have to reset our filter, as otherwise we'll skip past these events.
				filterResetRequired = true
				failCount++
				continue
			}

			// Dispatch the events
			if es.dispatchSetHWMCheckExit(ag, events, hwmBlock) {
				log.L(es.ctx).Debugf("Stream loop exiting")
				return true
			}

			// Update the head block to be the hwm block
			es.mux.Lock()
			es.headBlock = hwmBlock
			es.mux.Unlock()
		}

		// Reset failure count if we reach here
		failCount = 0

		// Sleep for the polling interval
		select {
		case <-time.After(es.c.eventFilterPollingInterval):
		case <-es.ctx.Done():
			log.L(es.ctx).Debugf("Stream loop stopping")
			return true
		}
	}
}

func (es *eventStream) preStartProcessing() {
	ctx := es.ctx
	chainHead, ok := es.c.blockListener.GetHighestBlock(ctx)
	if !ok {
		log.L(ctx).Warnf("Event stream closed before establishing block height")
		return
	}
	for _, l := range es.listeners {
		// During initial start we move the "head" block forwards to be the highest of all the initial streams
		if l.hwmBlock > es.headBlock {
			if l.hwmBlock > int64(chainHead) { //nolint:gosec // convert to int64 to match the type of headBlock
				es.headBlock = int64(chainHead) //nolint:gosec // convert to int64 to match the type of headBlock
			} else {
				es.headBlock = l.hwmBlock
			}
		}
	}

	// Now we've done that, we can start all the listeners
	for _, l := range es.listeners {
		es.startEventListener(l)
	}
}

func (es *eventStream) streamLoop() {
	defer close(es.streamLoopDone)

	es.preStartProcessing()

	for {
		// When we first start, we might find our leading pack of listeners are all way behind
		// the head of the chain. So we run a catchup mode loop to ensure we don't ask the blockchain
		// node to process an excessive amount of logs
		if es.leadGroupCatchup() {
			return
		}

		// We then transition to our steady state, filtering from the front of the chain.
		// But we might fall behind and need to go back to the catchup mode.
		if es.leadGroupSteadyState() {
			return
		}
	}

}

func (es *eventStream) dispatchSetHWMCheckExit(ag *aggregatedListener, events ffcapi.ListenerEvents, hwm int64) (exiting bool) {

	// Dispatch the events, updating the in-memory checkpoint for all listeners.
	if len(events) == 0 {
		select {
		case <-es.ctx.Done():
			return true
		default:
		}
	} else {
		for _, event := range events {
			log.L(es.ctx).Debugf("Detected event %s", event.Event)
			select {
			case es.events <- event:
			case <-es.ctx.Done():
				return true
			}
		}
	}

	// Move the HWM on all each listener forwards, if they are behind the base HWM for the event stream itself
	for _, l := range ag.listeners {
		l.moveHWM(hwm)
	}

	return false

}

func (es *eventStream) buildAggregatedListener(listeners []*listener) *aggregatedListener {
	ag := &aggregatedListener{
		listeners:         listeners,
		listenersByTopic0: make(map[string][]*listener),
	}
	for _, l := range listeners {
		for _, f := range l.config.filters {
			sigStr := f.Topic0.String()
			topicListeners, existing := ag.listenersByTopic0[sigStr]
			if !existing {
				ag.signatureSet = append(ag.signatureSet, f.Topic0)
			}
			ag.listenersByTopic0[sigStr] = append(topicListeners, l)
		}
	}
	return ag
}

func getEventProtoID(blockNumber, transactionIndex, logIndex int64) string {
	return fmt.Sprintf("%.12d/%.6d/%.6d", blockNumber, transactionIndex, logIndex)
}

func (es *eventStream) filterEnrichSort(ctx context.Context, ag *aggregatedListener, ethLogs []*ethrpc.LogJSONRPC) (ffcapi.ListenerEvents, error) {
	updates := make(ffcapi.ListenerEvents, 0, len(ethLogs))
	for _, ethLog := range ethLogs {
		listeners := ag.listenersByTopic0[ethLog.Topics[0].String()]
		for _, l := range listeners {
			for _, f := range l.config.filters {
				lu, matches, err := l.filterEnrichEthLog(ctx, f, l.config.options.Methods, ethLog)
				if err != nil {
					return nil, err
				}
				if matches {
					updates = append(updates, lu)
					break // A single listener cannot emit the event twice
				}
			}
		}
	}
	sort.Sort(updates)
	return updates, nil
}

func (es *eventStream) getBlockRangeEvents(ctx context.Context, ag *aggregatedListener, fromBlock, toBlock int64) (ffcapi.ListenerEvents, error) {
	var ethLogs []*ethrpc.LogJSONRPC
	logFilterJSONRPCReq := &ethrpc.LogFilterJSONRPC{
		FromBlock: ethtypes.NewHexInteger64(fromBlock),
		ToBlock:   ethtypes.NewHexInteger64(toBlock),
		Topics: [][]ethtypes.HexBytes0xPrefix{
			ag.signatureSet,
		},
	}

	if len(ag.listeners) == 1 && len(ag.listeners[0].config.filters) == 1 && ag.listeners[0].config.filters[0].Address != nil {
		logFilterJSONRPCReq.Address = []*ethtypes.Address0xHex{ag.listeners[0].config.filters[0].Address}
	}

	rpcErr := es.c.backend.CallRPC(ctx, &ethLogs, "eth_getLogs", logFilterJSONRPCReq)
	if rpcErr != nil {
		return nil, rpcErr.Error()
	}
	return es.filterEnrichSort(ctx, ag, ethLogs)
}

func (es *eventStream) getListenerHWM(ctx context.Context, listenerID *fftypes.UUID) (*ffcapi.EventListenerHWMResponse, ffcapi.ErrorReason, error) {
	es.mux.Lock()
	l := es.listeners[*listenerID]
	es.mux.Unlock()
	if l == nil {
		return nil, ffcapi.ErrorReasonNotFound, i18n.NewError(ctx, msgs.MsgListenerNotStarted, listenerID, es.id)
	}
	return &ffcapi.EventListenerHWMResponse{
		Checkpoint: l.getHWMCheckpoint(),
		Catchup:    l.catchup || es.catchup, // dirty read of whether the listener is in catchup, or the head group of the stream is in catchup
	}, "", nil
}
