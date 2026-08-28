// Copyright © 2022 Kaleido, Inc.
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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func testEventStream(t *testing.T, listeners ...*ffcapi.EventListenerAddRequest) (*eventStream, chan *ffcapi.ListenerEvent, *rpcbackendmocks.Backend, func()) {
	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)
	return testEventStreamExistingConnector(t, ctx, done, c, mRPC, listeners...)
}

func testEventStreamExistingConnector(t *testing.T, ctx context.Context, done func(), c *ethConnector, mRPC *rpcbackendmocks.Backend, listeners ...*ffcapi.EventListenerAddRequest) (*eventStream, chan *ffcapi.ListenerEvent, *rpcbackendmocks.Backend, func()) {
	events := make(chan *ffcapi.ListenerEvent)
	esID := fftypes.NewUUID()
	c.chainID = "12345" // set chainID before streamLoop starts, so enrich does not call net_version
	c.eventFilterPollingInterval = 1 * time.Millisecond
	c.retry.MaximumDelay = 1 * time.Microsecond
	_, _, err := c.EventStreamStart(ctx, &ffcapi.EventStreamStartRequest{
		ID:               esID,
		StreamContext:    ctx,
		EventStream:      events,
		BlockListener:    make(chan<- *ffcapi.BlockHashEvent),
		InitialListeners: listeners,
	})
	assert.NoError(t, err)
	es := c.eventStreams[*esID]
	assert.NotNil(t, es)

	es.preStartProcessing()

	return es, events, mRPC, func() {
		done()
		_, _, err := c.EventStreamStopped(ctx, &ffcapi.EventStreamStoppedRequest{
			ID: esID,
		})
		assert.NoError(t, err)
	}
}

func TestAddEventListenerMissingFilters(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	_, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:             es.id,
		ListenerID:           fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{},
	})
	assert.Regexp(t, "FF23035", err)

}

func TestAddEventListenerMissingFilterEvent(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	_, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{}`),
			},
		},
	})
	assert.Regexp(t, "FF23035", err)

}

func TestAddEventListenerBadFilterEvent(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	_, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":{"inputs":[{"type":"wrong"}]}}`),
			},
		},
	})
	assert.Regexp(t, "FF23033", err)

}

func TestAddEventListenerMultipleEvents(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	l, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0xe48C2eF8263fE160BF384cf621AAc36B82a49CE0","event":` + abiTransferEvent + `}`),
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options: fftypes.JSONAnyPtr(`{}`),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "[0xe48c2ef8263fe160bf384cf621aac36b82a49ce0:Transfer(address,address,uint256),*:Transfer(address,address,uint256)]", l.config.signature)

}

func TestAddEventListenerBadOptions(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	_, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options: fftypes.JSONAnyPtr(`{"bad json!`),
		},
	})
	assert.Regexp(t, "FF23033", err)

}

func TestAddEventListenerBadInitialBlock(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	_, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "wrong",
		},
	})
	assert.Regexp(t, "FF23034", err)

}

func TestStartHeadBlockLimitedByChainHead(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "50000000", // will limit to chain head
		},
	}

	es, _, _, done := testEventStream(t, l1req)
	defer done()

	// The listener is way ahead of the chain, so the head position is the safe point behind the
	// chain head - which is also where the steady state loop parks it, so this is stable
	assert.Equal(t, int64(testHighBlock)-es.c.checkpointBlockGap, es.headBlock.Load())
}

func TestCatchupThenRejoinLeadGroup(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "12001", // this will establish the position of the head group, starting in catchup, then moving to normal
		},
	}

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)

	closed := false
	listenerCaughtUp := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		ethLogs := make([]*ethrpc.LogJSONRPC, 0)
		filter := *args[3].(*ethrpc.LogFilterJSONRPC)
		fromBlock := filter.FromBlock.BigInt().Int64()
		switch fromBlock {
		case 1000:
			ethLogs = append(ethLogs, &ethrpc.LogJSONRPC{
				BlockNumber:      ethtypes.HexUint64(1024),
				TransactionIndex: ethtypes.HexUint64(64),
				LogIndex:         ethtypes.HexUint64(2),
				BlockHash:        ethtypes.MustNewHexBytes0xPrefix("0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c"),
				Topics: []ethtypes.HexBytes0xPrefix{
					ethtypes.MustNewHexBytes0xPrefix("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					ethtypes.MustNewHexBytes0xPrefix("0x0000000000000000000000003968ef051b422d3d1cdc182a88bba8dd922e6fa4"),
					ethtypes.MustNewHexBytes0xPrefix("0x000000000000000000000000d0f2f5103fd050739a9fb567251bc460cc24d091"),
				},
				Data: ethtypes.MustNewHexBytes0xPrefix("0x00000000000000000000000000000000000000000000000000000000000003e8"),
			})
		case 500 /*default catch up page size*/ + 1000:
			if !closed {
				close(listenerCaughtUp)
				closed = true
			}
		default:
			<-listenerCaughtUp // hold the main group back until we've done the listener catchup
		}
		*args[1].(*[]*ethrpc.LogJSONRPC) = ethLogs
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", "0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number: ethtypes.HexUint64(1024),
			Hash:   ethtypes.MustNewHexBytes0xPrefix("0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c"),
		}}
	})
	es, events, mRPC, done := testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)

	defer done()

	l2req := &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "1000",
		},
	}

	_, _, err := es.c.EventListenerAdd(es.ctx, l2req)
	assert.NoError(t, err)
	es.mux.Lock()
	l := es.listeners[*l2req.ListenerID]
	inCatchup := l.catchup
	es.mux.Unlock()
	assert.True(t, inCatchup)

	e := <-events
	assert.Equal(t, fftypes.FFuint64(1024), e.Event.ID.BlockNumber)
	assert.Equal(t, fftypes.FFuint64(64), e.Event.ID.TransactionIndex)
	assert.Equal(t, fftypes.FFuint64(2), e.Event.ID.LogIndex)
	assert.Equal(t, int64(1024), e.Checkpoint.(*listenerCheckpoint).Block)
	assert.Equal(t, int64(64), e.Checkpoint.(*listenerCheckpoint).TransactionIndex)
	assert.Equal(t, int64(2), e.Checkpoint.(*listenerCheckpoint).LogIndex)
	assert.NotNil(t, e.Event)
	assert.Equal(t, "0x3968ef051b422d3d1cdc182a88bba8dd922e6fa4", e.Event.Data.JSONObject().GetString("from"))
	assert.Equal(t, "0xd0f2f5103fd050739a9fb567251bc460cc24d091", e.Event.Data.JSONObject().GetString("to"))
	assert.Equal(t, "1000", e.Event.Data.JSONObject().GetString("value"))

	<-listenerCaughtUp

	// Confirm the listener joins the group
	started := time.Now()
	for {
		es.mux.Lock()
		inCatchup = l.catchup
		es.mux.Unlock()
		t.Logf("Catchup=%t HeadBlock=%d", inCatchup, es.headBlock.Load())
		select {
		case <-events:
			t.Logf("Noting duplicate event detection of unconfirmed event after listener rejoined head group")
		default:
		}
		if time.Since(started) > 1*time.Second {
			assert.Fail(t, "Never exited catchup")
		}
		if inCatchup {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if es.headBlock.Load() != testHighBlock-es.c.checkpointBlockGap {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		break
	}
}

func TestListenerNotAdoptedByLeadGroupBeforeStart(t *testing.T) {

	// An initial listener at the head of the chain, so preStartProcessing establishes es.headBlock
	// (with no initial listeners at all, headBlock is only established later by the stream loop)
	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "latest",
		},
	}

	es, _, mRPC, done := testEventStream(t, l1req)
	defer done()
	es.mux.Lock()
	assert.GreaterOrEqual(t, es.headBlock.Load(), int64(testHighBlock)-es.c.checkpointBlockGap)
	es.mux.Unlock()

	var callMux sync.Mutex
	getLogsCalls := 0
	firstPageDone := make(chan struct{})  // closed when the 2nd eth_getLogs call arrives - i.e. the 1st catchup page has been scanned, and the HWM moved
	releaseCatchup := make(chan struct{}) // gates the 2nd eth_getLogs call, so we can assert the mid-catchup state deterministically
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callMux.Lock()
		thisCall := getLogsCalls + 1
		getLogsCalls = thisCall
		callMux.Unlock()
		if thisCall == 2 {
			close(firstPageDone)
			<-releaseCatchup
		}
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{}
	}).Maybe()

	es.mux.Lock()
	updateCountBeforeAdd := es.updateCount
	es.mux.Unlock()

	// Add (but do not yet start) a listener far behind the head of the chain
	l, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "1000",
		},
	})
	assert.NoError(t, err)

	// The listener must be published in catchup mode, without prodding the lead group to rebuild,
	// so there is no window where the lead group can adopt it and move its HWM to the head of the chain
	es.mux.Lock()
	assert.True(t, l.catchup)
	assert.Equal(t, updateCountBeforeAdd, es.updateCount)
	es.mux.Unlock()
	leadGroupContains := func(ag *aggregatedListener, target *listener) bool {
		for _, member := range ag.listeners {
			if member == target {
				return true
			}
		}
		return false
	}
	lastUpdate := -1
	var ag *aggregatedListener
	es.buildReuseLeadGroupListener(&lastUpdate, &ag)
	assert.False(t, leadGroupContains(ag, l))
	l.hwmMux.Lock()
	assert.Equal(t, int64(1000), l.hwmBlock) // the HWM must still be the configured fromBlock
	l.hwmMux.Unlock()

	// Start it - as it is far behind the head it must enter a catchup loop, not join the lead group
	es.startEventListener(l)
	<-firstPageDone // a catchup iteration has executed (fromBlock=1000), and the loop is now gated before page 2

	es.mux.Lock()
	assert.True(t, l.catchup)
	updateCountMidCatchup := es.updateCount
	catchupLoopDone := l.catchupLoopDone
	es.mux.Unlock()
	require.NotNil(t, catchupLoopDone)

	// The HWM must reflect the scanned range (one page on from the configured fromBlock) - not the head of the chain
	l.hwmMux.Lock()
	assert.Equal(t, int64(1000)+es.c.catchupPageSize, l.hwmBlock)
	l.hwmMux.Unlock()

	// Still not adopted by the lead group mid-catchup
	lastUpdate = -1
	es.buildReuseLeadGroupListener(&lastUpdate, &ag)
	assert.False(t, leadGroupContains(ag, l))

	// startEventListener must be idempotent - a second call (as happens for initial listeners, which are
	// started by both the stream loop and preStartProcessing in some paths) must not spawn a second
	// catchup loop, or prod the lead group to rebuild
	es.startEventListener(l)
	es.mux.Lock()
	assert.Equal(t, updateCountMidCatchup, es.updateCount)
	assert.True(t, catchupLoopDone == l.catchupLoopDone)
	es.mux.Unlock()

	// Release the catchup loop and let it scan to the head of the chain - it must then rejoin the lead group
	close(releaseCatchup)
	<-catchupLoopDone

	es.mux.Lock()
	assert.False(t, l.catchup)
	assert.NotEqual(t, updateCountMidCatchup, es.updateCount)
	es.mux.Unlock()
	lastUpdate = -1
	es.buildReuseLeadGroupListener(&lastUpdate, &ag)
	assert.True(t, leadGroupContains(ag, l))
}

func TestHeadBlockEstablishedWithNoInitialListeners(t *testing.T) {

	es, _, _, done := testEventStream(t)
	defer done()

	// With no listeners the lead group loops never store a head position, so this is purely what
	// preStartProcessing established - the safe point behind the chain head
	assert.Equal(t, int64(testHighBlock)-es.c.checkpointBlockGap, es.headBlock.Load())
}

func TestListenerHeldInCatchupUntilHeadEstablished(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)
	defer done()
	c.eventFilterPollingInterval = 1 * time.Millisecond
	c.retry.MaximumDelay = 1 * time.Microsecond

	var callMux sync.Mutex
	getLogsCalls := 0
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callMux.Lock()
		getLogsCalls++
		callMux.Unlock()
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{}
	}).Maybe()

	es := &eventStream{
		id:        fftypes.NewUUID(),
		c:         c,
		ctx:       ctx,
		events:    make(chan<- *ffcapi.ListenerEvent),
		listeners: make(map[fftypes.UUID]*listener),
	}
	es.headBlock.Store(-1) // the stream's head position is not yet established

	// A listener at the head of the chain
	l, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "latest",
		},
	})
	assert.NoError(t, err)

	es.startEventListener(l)

	// It must be held in catchup (not adopted by the lead group against an unestablished head)
	es.mux.Lock()
	assert.True(t, l.catchup)
	catchupLoopDone := l.catchupLoopDone
	es.mux.Unlock()
	require.NotNil(t, catchupLoopDone)

	// While the head position is unestablished, the catchup loop cannot know where the lead group
	// will land - so it must not scan at all, and must leave the HWM where it is
	time.Sleep(50 * time.Millisecond) // many polling intervals
	callMux.Lock()
	assert.Zero(t, getLogsCalls)
	callMux.Unlock()
	assert.Equal(t, int64(testHighBlock), l.getHWMBlock())

	// Once the head position is established, the listener joins the lead group rather than
	// scanning past it
	es.headBlock.Store(testHighBlock)
	<-catchupLoopDone
	es.mux.Lock()
	assert.False(t, l.catchup)
	es.mux.Unlock()
	callMux.Lock()
	assert.Zero(t, getLogsCalls)
	callMux.Unlock()
	assert.Equal(t, int64(testHighBlock), l.getHWMBlock()) // never advanced past the lead group
}

func TestListenerHeldInCatchupExitsWhenStreamStops(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)
	defer done()
	// Long polling interval, so the only way out of the catchup wait is the stream stopping
	c.eventFilterPollingInterval = 1 * time.Hour

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	es := &eventStream{
		id:        fftypes.NewUUID(),
		c:         c,
		ctx:       streamCtx,
		events:    make(chan<- *ffcapi.ListenerEvent),
		listeners: make(map[fftypes.UUID]*listener),
	}
	es.headBlock.Store(-1) // the stream's head position is never established

	l, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "latest",
		},
	})
	assert.NoError(t, err)

	es.startEventListener(l)
	es.mux.Lock()
	catchupLoopDone := l.catchupLoopDone
	es.mux.Unlock()
	require.NotNil(t, catchupLoopDone)

	// Parked waiting for the head position to be established - stopping the stream must exit it
	cancelStream()
	<-catchupLoopDone
}

func TestCatchupDoesNotOvershootLeadGroup(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)
	defer done()
	c.eventFilterPollingInterval = 1 * time.Millisecond
	c.retry.MaximumDelay = 1 * time.Microsecond
	// Deliberately configure a page size larger than the threshold - the normalization in
	// NewEthereumConnector prevents this, but the catchup loop must not rely on that to avoid
	// paging past the lead group
	c.catchupThreshold = 10
	c.catchupPageSize = 100

	// The lead group is parked at the safe point behind the chain head
	headBlock := int64(testHighBlock) - c.checkpointBlockGap

	var callMux sync.Mutex
	var maxToBlock int64 = -1
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		toBlock := args[3].(*ethrpc.LogFilterJSONRPC).ToBlock.BigInt().Int64()
		callMux.Lock()
		if toBlock > maxToBlock {
			maxToBlock = toBlock
		}
		callMux.Unlock()
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{}
	}).Maybe()

	es := &eventStream{
		id:        fftypes.NewUUID(),
		c:         c,
		ctx:       ctx,
		events:    make(chan<- *ffcapi.ListenerEvent),
		listeners: make(map[fftypes.UUID]*listener),
	}
	es.headBlock.Store(headBlock)

	// A listener half a page behind the lead group - so a full unclamped page would take its HWM
	// past the lead group, into the re-org unstable blocks the lead group is deliberately behind
	l, err := es.addEventListener(es.ctx, &ffcapi.EventListenerAddRequest{
		StreamID:   es.id,
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.FormatInt(headBlock-50, 10),
		},
	})
	assert.NoError(t, err)

	// The gap (50) is above the threshold (10), so it starts in individual catchup
	es.startEventListener(l)
	es.mux.Lock()
	assert.True(t, l.catchup)
	catchupLoopDone := l.catchupLoopDone
	es.mux.Unlock()
	require.NotNil(t, catchupLoopDone)

	// It catches up to the lead group and joins it, rather than scanning past it
	<-catchupLoopDone
	es.mux.Lock()
	assert.False(t, l.catchup)
	es.mux.Unlock()
	assert.Equal(t, headBlock, l.getHWMBlock())

	// No query ever reached the lead group's block, so the HWM could never advance past it
	callMux.Lock()
	assert.Equal(t, headBlock-1, maxToBlock)
	callMux.Unlock()
}

func TestLeadGroupSteadyStateFallbackToCatchup(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)
	defer done()

	es := &eventStream{
		id:     fftypes.NewUUID(),
		c:      c,
		ctx:    ctx,
		events: make(chan<- *ffcapi.ListenerEvent),
		listeners: map[fftypes.UUID]*listener{
			*fftypes.NewUUID(): {
				id: fftypes.NewUUID(),
				config: listenerConfig{
					filters: []*eventFilter{
						{},
					},
				},
			},
		},
		streamLoopDone: make(chan struct{}),
	}
	es.headBlock.Store(-1)

	endedDueToExit := es.leadGroupSteadyState()
	assert.False(t, endedDueToExit)
}

func TestExitDuringCatchup(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "12001", // this will establish the position of the head group, starting in catchup, then moving to normal
		},
	}

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)

	completed := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		ethLogs := make([]*ethrpc.LogJSONRPC, 0)
		filter := *args[3].(*ethrpc.LogFilterJSONRPC)
		fromBlock := filter.FromBlock.BigInt().Int64()
		switch fromBlock {
		default:
			go func() {
				done()
				close(completed)
			}()
		}
		*args[1].(*[]*ethrpc.LogJSONRPC) = ethLogs
	}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(nil).Maybe()

	_, _, mRPC, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)

	<-completed
}

// TestGetBlockRangeEventsFilterNoAddress ensures eth_getLogs is not sent with address:[null]
// when the listener has one event filter (e.g. Transfer) and no contract address.
func TestGetBlockRangeEventsFilterNoAddress(t *testing.T) {
	transferTopic0 := ethtypes.MustNewHexBytes0xPrefix("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`), // no "address" in filter
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "1000",
		},
	}

	ctx, c, mRPC, done := newTestConnector(t)
	mockStreamLoopEmpty(mRPC)

	ethGetLogsCalled := make(chan *ethrpc.LogFilterJSONRPC, 1)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.MatchedBy(func(filter *ethrpc.LogFilterJSONRPC) bool {
		// Must not send address with a nil entry (would serialize as [null] and cause "Invalid filter params")
		if len(filter.Address) > 0 {
			for _, a := range filter.Address {
				assert.NotNil(t, a, "eth_getLogs filter must not contain address:[null] when filter has no address")
				if a == nil {
					return false
				}
			}
		}
		// Must filter by Transfer topic0
		assert.Len(t, filter.Topics, 1, "topics should have one element (topic0)")
		if len(filter.Topics) == 1 {
			assert.Contains(t, filter.Topics[0], transferTopic0, "topic0 should contain Transfer signature")
		}
		select {
		case ethGetLogsCalled <- filter:
		default:
		}
		return true
	})).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	})

	_, _, _, done2 := testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done2()

	// Wait until we've seen at least one eth_getLogs call with the expected filter shape
	var captured *ethrpc.LogFilterJSONRPC
	select {
	case captured = <-ethGetLogsCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("eth_getLogs was not called with the expected filter")
	}
	assert.NotNil(t, captured)
	assert.Empty(t, captured.Address, "address should be omitted or empty when listener has no address filter")
}

func TestLeadGroupDeliverEvents(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0xc89E46EEED41b777ca6625d37E1Cc87C5c037828","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}

	ctx, c, mRPC, done := newTestConnector(t)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(*string) = testLogsFilterID1
		}).Once()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{
			{
				BlockNumber:      ethtypes.HexUint64(212122),
				TransactionIndex: ethtypes.HexUint64(64),
				LogIndex:         ethtypes.HexUint64(2),
				BlockHash:        ethtypes.MustNewHexBytes0xPrefix("0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c"),
				Address:          ethtypes.MustNewAddress("0xc89E46EEED41b777ca6625d37E1Cc87C5c037828"),
				Topics: []ethtypes.HexBytes0xPrefix{
					ethtypes.MustNewHexBytes0xPrefix("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					ethtypes.MustNewHexBytes0xPrefix("0x0000000000000000000000003968ef051b422d3d1cdc182a88bba8dd922e6fa4"),
					ethtypes.MustNewHexBytes0xPrefix("0x000000000000000000000000d0f2f5103fd050739a9fb567251bc460cc24d091"),
				},
				Data: ethtypes.MustNewHexBytes0xPrefix("0x00000000000000000000000000000000000000000000000000000000000003e8"),
			},
		}
	}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{}
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", "0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c", false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
			Number: ethtypes.HexUint64(212122),
			Hash:   ethtypes.MustNewHexBytes0xPrefix("0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c"),
		}}
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*bool) = true
	}).Maybe()

	_, events, _, done := testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	e := <-events
	assert.Equal(t, fftypes.FFuint64(212122), e.Event.ID.BlockNumber)
	assert.Equal(t, fftypes.FFuint64(64), e.Event.ID.TransactionIndex)
	assert.Equal(t, fftypes.FFuint64(2), e.Event.ID.LogIndex)
	assert.Equal(t, int64(212122), e.Checkpoint.(*listenerCheckpoint).Block)
	assert.Equal(t, int64(64), e.Checkpoint.(*listenerCheckpoint).TransactionIndex)
	assert.Equal(t, int64(2), e.Checkpoint.(*listenerCheckpoint).LogIndex)
	assert.NotNil(t, e.Event)
	assert.Equal(t, "0x3968ef051b422d3d1cdc182a88bba8dd922e6fa4", e.Event.Data.JSONObject().GetString("from"))
	assert.Equal(t, "0xd0f2f5103fd050739a9fb567251bc460cc24d091", e.Event.Data.JSONObject().GetString("to"))
	assert.Equal(t, "1000", e.Event.Data.JSONObject().GetString("value"))
	mRPC.AssertExpectations(t)
}

func TestLeadGroupNearBlockZeroEnsureNonNegative(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0xc89E46EEED41b777ca6625d37E1Cc87C5c037828","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "0",
		},
	}

	ctx, c, mRPC, done := newTestConnector(t)

	filtered := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexInteger64(10)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			assert.Equal(t, int64(0), args[3].(*ethrpc.LogFilterJSONRPC).FromBlock.BigInt().Int64())
			*args[1].(*string) = testLogsFilterID1
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Once().Run(func(args mock.Arguments) {
		close(filtered)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*bool) = true
	}).Maybe()

	_, _, _, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	<-filtered
	mRPC.AssertExpectations(t)
}

func TestLeadGroupCatchupRetry(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "0",
		},
	}
	ctx, c, mRPC, done := newTestConnector(t)

	retried := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		hbh := args[1].(*ethtypes.HexInteger)
		*hbh = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"}).
		Run(func(args mock.Arguments) {
			close(retried)
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getLogs", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"})

	_, _, mRPC, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	<-retried

}
func TestLeadGroupCatchupExitWhenNoBlockHeightEstablished(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: "0",
		},
	}
	ctx, c, mRPC, cDone := newTestConnector(t)

	attempted := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"}).Run(func(args mock.Arguments) {
		close(attempted)
		cDone()
	}).Once()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"}).Maybe()
	_, _, mRPC, done := testEventStreamExistingConnector(t, ctx, cDone, c, mRPC, l1req)
	defer done()
	<-attempted

}

func TestStreamLoopNewFilterFail(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}
	ctx, c, mRPC, done := newTestConnector(t)

	retried := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		hbh := args[1].(*ethtypes.HexInteger)
		*hbh = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"}).
		Run(func(args mock.Arguments) {
			close(retried)
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"}).Maybe()

	_, _, mRPC, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	<-retried

}

func TestStreamCleanupFilterOK(t *testing.T) {

	mRPC := &rpcbackendmocks.Backend{}
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil)

	es := &eventStream{
		ctx: context.Background(),
		c: &ethConnector{
			backend: mRPC,
		},
	}

	filterID := "filter1"
	es.uninstallFilter(&filterID)

	assert.Empty(t, filterID)

}

func TestStreamCleanupFilterFailLog(t *testing.T) {

	mRPC := &rpcbackendmocks.Backend{}
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(&rpcbackend.RPCError{Message: "pop"})

	es := &eventStream{
		ctx: context.Background(),
		c: &ethConnector{
			backend: mRPC,
		},
	}

	filterID := "filter1"
	es.uninstallFilter(&filterID)

	assert.Empty(t, filterID)

}

func TestStreamLoopChangeFilter(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0x171AE0BDd882F7b4C84D5b7FBFA994E39C5a3129","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}
	l2req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0xc1552c7E527f8cb51bbca69c6849a192598FAFe6","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}
	ctx, c, mRPC, done := newTestConnector(t)

	esChl := make(chan *eventStream)
	reestablishedFilter := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		hbh := args[1].(*ethtypes.HexInteger)
		*hbh = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			es := <-esChl
			l2req.StreamID = es.id
			_, _, err := c.EventListenerAdd(ctx, l2req)
			assert.NoError(t, err)
			*args[1].(*string) = testLogsFilterID1
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(*string) = testLogsFilterID2
			close(reestablishedFilter)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*bool) = true
	}).Maybe()

	es, _, mRPC, done := testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()
	esChl <- es

	<-reestablishedFilter

}

func TestStreamLoopFilterReset(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0x171AE0BDd882F7b4C84D5b7FBFA994E39C5a3129","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}
	ctx, c, mRPC, done := newTestConnector(t)
	reestablishedFilter := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		hbh := args[1].(*ethtypes.HexInteger)
		*hbh = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(*string) = testLogsFilterID1
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(*string) = testLogsFilterID2
			close(reestablishedFilter)
		})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(&rpcbackend.RPCError{Message: "filter not found"}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*bool) = true
	}).Maybe()

	_, _, mRPC, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	<-reestablishedFilter

}

func TestStreamLoopEnrichFail(t *testing.T) {

	l1req := &ffcapi.EventListenerAddRequest{
		ListenerID: fftypes.NewUUID(),
		EventListenerOptions: ffcapi.EventListenerOptions{
			Filters: []fftypes.JSONAny{
				*fftypes.JSONAnyPtr(`{"address":"0x171AE0BDd882F7b4C84D5b7FBFA994E39C5a3129","event":` + abiTransferEvent + `}`),
			},
			Options:   fftypes.JSONAnyPtr(`{}`),
			FromBlock: strconv.Itoa(testHighBlock),
		},
	}
	ctx, c, mRPC, done := newTestConnector(t)
	c.eventBlockTimestamps = true

	errorReturned := make(chan struct{})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		hbh := args[1].(*ethtypes.HexInteger)
		*hbh = *ethtypes.NewHexInteger64(testHighBlock)
	})
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).Return(nil).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = testBlockFilterID1
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newFilter", mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			*args[1].(*string) = testLogsFilterID1
		}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterLogs", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = []*ethrpc.LogJSONRPC{
			{
				BlockNumber:      ethtypes.HexUint64(212122),
				TransactionIndex: ethtypes.HexUint64(64),
				LogIndex:         ethtypes.HexUint64(2),
				BlockHash:        ethtypes.MustNewHexBytes0xPrefix("0x6b012339fbb85b70c58ecfd97b31950c4a28bcef5226e12dbe551cb1abaf3b4c"),
				Address:          ethtypes.MustNewAddress("0x171AE0BDd882F7b4C84D5b7FBFA994E39C5a3129"),
				Topics: []ethtypes.HexBytes0xPrefix{
					ethtypes.MustNewHexBytes0xPrefix("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					ethtypes.MustNewHexBytes0xPrefix("0x0000000000000000000000003968ef051b422d3d1cdc182a88bba8dd922e6fa4"),
					ethtypes.MustNewHexBytes0xPrefix("0x000000000000000000000000d0f2f5103fd050739a9fb567251bc460cc24d091"),
				},
				Data: ethtypes.MustNewHexBytes0xPrefix("0x00000000000000000000000000000000000000000000000000000000000003e8"),
			},
		}
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", mock.Anything, false).
		Run(func(args mock.Arguments) {
			close(errorReturned)
		}).
		Return(&rpcbackend.RPCError{Message: "pop"}).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]*ethrpc.LogJSONRPC) = make([]*ethrpc.LogJSONRPC, 0)
	}).Maybe()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_uninstallFilter", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*bool) = true
	}).Maybe()

	_, _, mRPC, done = testEventStreamExistingConnector(t, ctx, done, c, mRPC, l1req)
	defer done()

	<-errorReturned

}

func TestDispatchListenerDone(t *testing.T) {

	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()
	es := &eventStream{
		ctx:    doneCtx,
		events: make(chan<- *ffcapi.ListenerEvent),
	}
	exiting := es.dispatchSetHWMCheckExit(&aggregatedListener{}, ffcapi.ListenerEvents{
		{},
	}, -1)
	assert.True(t, exiting)

}

func TestGetListenerHWMNotFound(t *testing.T) {

	es := &eventStream{
		ctx:       context.Background(),
		events:    make(chan<- *ffcapi.ListenerEvent),
		listeners: make(map[fftypes.UUID]*listener),
	}
	_, rc, err := es.getListenerHWM(context.Background(), fftypes.NewUUID())
	assert.Regexp(t, "FF23043", err)
	assert.Equal(t, ffcapi.ErrorReasonNotFound, rc)

}
