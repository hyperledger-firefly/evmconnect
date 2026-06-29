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

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethblocklistener"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) EventStreamStart(ctx context.Context, req *ffcapi.EventStreamStartRequest) (*ffcapi.EventStreamStartResponse, ffcapi.ErrorReason, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	es := c.eventStreams[*req.ID]
	if es != nil {
		return nil, ffcapi.ErrorReason(""), i18n.NewError(ctx, msgs.MsgStreamAlreadyStarted, req.ID)
	}

	es = &eventStream{
		id:             req.ID,
		c:              c,
		ctx:            req.StreamContext,
		events:         req.EventStream,
		headBlock:      -1,
		listeners:      make(map[fftypes.UUID]*listener),
		streamLoopDone: make(chan struct{}),
	}

	// We add all the initial event listeners, checking for errors, before kicking off the streamLoop().
	for _, il := range req.InitialListeners {
		// Add, but do NOT start, the listener.
		// We do pre-startup processing on this special set in streamLoop() after we've established
		// the chain head, then start them all after that.
		_, err := es.addEventListener(ctx, il)
		if err != nil {
			return nil, "", err
		}
	}

	// From this point we consider ourselves as in started state, but we might not actually be ready
	c.eventStreams[*req.ID] = es

	// Start the listener head routine, which reads events for all listeners that are not in catchup mode
	go es.streamLoop()

	// Add the block consumer
	c.blockListener.AddConsumer(req.StreamContext, &ethblocklistener.BlockUpdateConsumer{
		ID:      es.id,
		Ctx:     req.StreamContext,
		Updates: req.BlockListener,
	})

	return &ffcapi.EventStreamStartResponse{}, "", nil
}

func (c *ethConnector) EventStreamStopped(ctx context.Context, req *ffcapi.EventStreamStoppedRequest) (*ffcapi.EventStreamStoppedResponse, ffcapi.ErrorReason, error) {
	c.mux.Lock()
	es := c.eventStreams[*req.ID]
	c.mux.Unlock()
	if es != nil {
		select {
		case <-es.ctx.Done():
			// This is good, it is stopped
		default:
			return nil, ffcapi.ErrorReason(""), i18n.NewError(ctx, msgs.MsgStreamNotStopped, req.ID)
		}

		c.mux.Lock()
		delete(c.eventStreams, *req.ID)
		listeners := make([]*listener, 0)
		for _, l := range es.listeners {
			listeners = append(listeners, l)
		}
		c.mux.Unlock()
		// Wait for stream loop to complete
		<-es.streamLoopDone
		// Wait for any listener catchup loops
		for _, l := range listeners {
			if l.catchupLoopDone != nil {
				<-l.catchupLoopDone
			}
		}
	}
	return &ffcapi.EventStreamStoppedResponse{}, "", nil
}

func (c *ethConnector) EventListenerVerifyOptions(ctx context.Context, req *ffcapi.EventListenerVerifyOptionsRequest) (*ffcapi.EventListenerVerifyOptionsResponse, ffcapi.ErrorReason, error) {

	signature, _, err := parseEventFilters(ctx, req.Filters)
	if err != nil {
		return nil, "", err
	}

	options, err := parseListenerOptions(ctx, req.Options)
	if err != nil {
		return nil, "", err
	}

	ob, _ := json.Marshal(&options)
	return &ffcapi.EventListenerVerifyOptionsResponse{
		ResolvedSignature: signature,
		ResolvedOptions:   fftypes.JSONAny(ob),
	}, "", nil

}

func (c *ethConnector) EventListenerAdd(ctx context.Context, req *ffcapi.EventListenerAddRequest) (*ffcapi.EventListenerAddResponse, ffcapi.ErrorReason, error) {
	c.mux.Lock()
	es := c.eventStreams[*req.StreamID]
	c.mux.Unlock()
	if es == nil {
		return nil, ffcapi.ErrorReasonNotFound, i18n.NewError(ctx, msgs.MsgStreamNotStarted, req.StreamID)
	}
	l, err := es.addEventListener(ctx, req)
	if err != nil {
		return nil, ffcapi.ErrorReason(""), err
	}
	// We start this listener straight away
	es.startEventListener(l)
	return &ffcapi.EventListenerAddResponse{}, ffcapi.ErrorReason(""), nil
}

func (c *ethConnector) EventListenerRemove(ctx context.Context, req *ffcapi.EventListenerRemoveRequest) (*ffcapi.EventListenerRemoveResponse, ffcapi.ErrorReason, error) {
	c.mux.Lock()
	es := c.eventStreams[*req.StreamID]
	c.mux.Unlock()
	if es == nil {
		return nil, ffcapi.ErrorReasonNotFound, i18n.NewError(ctx, msgs.MsgStreamNotStarted, req.StreamID)
	}
	es.removeEventListener(req.ListenerID)
	return &ffcapi.EventListenerRemoveResponse{}, ffcapi.ErrorReason(""), nil
}

func (c *ethConnector) EventStreamNewCheckpointStruct() ffcapi.EventListenerCheckpoint {
	return &listenerCheckpoint{}
}

func (c *ethConnector) EventListenerHWM(ctx context.Context, req *ffcapi.EventListenerHWMRequest) (*ffcapi.EventListenerHWMResponse, ffcapi.ErrorReason, error) {
	c.mux.Lock()
	es := c.eventStreams[*req.StreamID]
	c.mux.Unlock()
	if es == nil {
		return nil, ffcapi.ErrorReasonNotFound, i18n.NewError(ctx, msgs.MsgStreamNotStarted, req.StreamID)
	}
	return es.getListenerHWM(ctx, req.ListenerID)
}
