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

package ethblocklistener

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"github.com/hyperledger/firefly-common/pkg/wsclient"
	"github.com/hyperledger/firefly-evmconnect/mocks/rpcbackendmocks"
	"github.com/hyperledger/firefly-evmconnect/pkg/ethrpc"
	"github.com/hyperledger/firefly-signer/pkg/ethtypes"
	"github.com/hyperledger/firefly-signer/pkg/rpcbackend"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testBlockFilterID1 = "block_filter_1"
const testBlockFilterID2 = "block_filter_2"

const shortDelay = 10 * time.Millisecond

func newTestBlockListener(t *testing.T, confSetup ...func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc)) (context.Context, *blockListener, *rpcbackendmocks.Backend, func()) {
	ctx, cancelCtx := context.WithCancel(context.Background())

	mRPC := &rpcbackendmocks.Backend{}
	logrus.SetLevel(logrus.DebugLevel)
	conf := &BlockListenerConfig{
		BlockPollingInterval:    1 * time.Hour,
		MonitoredHeadLength:     50,
		HederaCompatibilityMode: false,
		BlockCacheSize:          250,
	}
	for _, fn := range confSetup {
		fn(conf, mRPC, cancelCtx)
	}
	ibl, err := NewBlockListenerSupplyBackend(ctx, &retry.Retry{
		InitialDelay: shortDelay,
		MaximumDelay: 50 * time.Millisecond,
		Factor:       2.0,
	}, conf, mRPC, nil)
	require.NoError(t, err)

	require.Equal(t, conf.MonitoredHeadLength, ibl.GetMonitoredHeadLength())

	return ctx, ibl.(*blockListener), mRPC, func() {
		cancelCtx()
		mRPC.AssertExpectations(t)
		ibl.WaitClosed()
	}
}

func hexNumber(i uint64) string {
	hn := ethtypes.HexUint64(i)
	return hn.String()
}

// Simple util to allow a routine to block until another routine reaches a given point
type testLatch struct {
	mux    sync.Mutex
	closed bool
	c      chan struct{}
}

func newTestLatch() *testLatch {
	return &testLatch{c: make(chan struct{}, 1)}
}

func (tl *testLatch) complete() {
	tl.mux.Lock()
	defer tl.mux.Unlock()
	if !tl.closed {
		tl.closed = true
		close(tl.c)
	}
}

func (tl *testLatch) waitComplete() {
	<-tl.c
}

// testBlockHashFor generates a deterministic 32-byte block hash for a given height.
// Optional mods create alternate-fork hashes at the same height.
func testBlockHashFor(height uint64, mods ...uint64) ethtypes.HexBytes0xPrefix {
	seed := height
	for _, mod := range mods {
		seed = seed*31337 + mod
	}
	h := fmt.Sprintf("0x%016x%016x%016x%016x", 0xfeed000000000000+height, seed, ^seed, ^height)
	return ethtypes.MustNewHexBytes0xPrefix(h)
}

// mockInitialBlockHeight mocks one eth_blockNumber call returning height.
func mockInitialBlockHeight(mRPC *rpcbackendmocks.Backend, height uint64) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(height)
	}).Once()
}

// mockSeedBlockNotFound mocks eth_getBlockByNumber at seedHeight returning nil (block not found).
func mockSeedBlockNotFound(mRPC *rpcbackendmocks.Backend, seedHeight uint64) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", hexNumber(seedHeight), false).Return(nil).Once()
}

// mockSeedBlock mocks eth_getBlockByNumber at height returning a block with the given hash.
func mockSeedBlock(mRPC *rpcbackendmocks.Backend, height uint64, hash ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", hexNumber(height), false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{
			BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
				Number: ethtypes.HexUint64(height),
				Hash:   hash,
			},
		}
	})
}

// mockNewBlockFilter mocks eth_newBlockFilter returning filterID.
func mockNewBlockFilter(mRPC *rpcbackendmocks.Backend, filterID string) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*string) = filterID
	})
}

// mockFilterChanges mocks eth_getFilterChanges for filterID, optionally waiting on latch, returning hashes.
func mockFilterChanges(mRPC *rpcbackendmocks.Backend, filterID string, latch *testLatch, hashes ...ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", filterID).Return(nil).Run(func(args mock.Arguments) {
		if latch != nil {
			latch.waitComplete()
		}
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = hashes
	})
}

// mockFilterChangesEmpty mocks any eth_getFilterChanges returning empty, optionally running fns.
func mockFilterChangesEmpty(mRPC *rpcbackendmocks.Backend, fns ...func()) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
		for _, fn := range fns {
			fn()
		}
	})
}

// mockBlockByHash mocks eth_getBlockByHash for hash returning a block at height with parentHash.
func mockBlockByHash(mRPC *rpcbackendmocks.Backend, height uint64, hash, parentHash ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", hash.String(), false).Return(nil).Run(func(args mock.Arguments) {
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{
			BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
				Number:     ethtypes.HexUint64(height),
				Hash:       hash,
				ParentHash: parentHash,
			},
		}
	})
}

// mockBlockByHashNotFound mocks eth_getBlockByHash for hash returning nil (block not found).
func mockBlockByHashNotFound(mRPC *rpcbackendmocks.Backend, hash ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", hash.String(), false).Return(nil)
}

// mockBlockByHashFail mocks eth_getBlockByHash for hash returning an RPC error.
func mockBlockByHashFail(mRPC *rpcbackendmocks.Backend, hash ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", hash.String(), false).
		Return(&rpcbackend.RPCError{Message: "pop"})
}

// mockBlockByNumber mocks eth_getBlockByNumber at height. Pass nil hash to return nil (not found).
// When hash is non-nil, parentHash is derived as testBlockHashFor(height-1).
func mockBlockByNumber(mRPC *rpcbackendmocks.Backend, height uint64, hash *ethtypes.HexBytes0xPrefix) *mock.Call {
	return mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", hexNumber(height), false).Return(nil).Run(func(args mock.Arguments) {
		if hash == nil {
			return
		}
		*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{
			BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
				Number:     ethtypes.HexUint64(height),
				Hash:       *hash,
				ParentHash: testBlockHashFor(height - 1),
			},
		}
	})
}

// mockBlockRangeWithHash mocks sequential eth_getBlockByNumber and eth_getBlockByHash for heights start..end
// using deterministic hashes from testBlockHashFor.
func mockBlockRangeWithHash(mRPC *rpcbackendmocks.Backend, start, end uint64, mods ...uint64) {
	for height := start; height <= end; height++ {
		hash := testBlockHashFor(height, mods...)
		mockBlockByNumber(mRPC, height, &hash).Maybe()
		mockBlockByHash(mRPC, height, hash, testBlockHashFor(height-1, mods...)).Maybe()
	}
}

func TestBlockListenerConstructorFailMonitoredHeadLength(t *testing.T) {
	_, err := NewBlockListener(context.Background(), &retry.Retry{}, &BlockListenerConfig{
		BlockCacheSize:      -1,
		MonitoredHeadLength: -1,
	}, &ffresty.Config{}, &wsclient.WSConfig{})
	require.Regexp(t, "FF23072", err)
}

func TestBlockListenerConstructorFailCacheConfig(t *testing.T) {
	_, err := NewBlockListener(context.Background(), &retry.Retry{}, &BlockListenerConfig{
		BlockCacheSize:      -1,
		MonitoredHeadLength: 1,
	}, &ffresty.Config{}, &wsclient.WSConfig{})
	require.Regexp(t, "FF23040", err)
}

func TestBlockListenerStartGettingHighestBlockRetry(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").
		Return(&rpcbackend.RPCError{Message: "pop"}).Once()
	mockInitialBlockHeight(mRPC, 12345)
	mockSeedBlockNotFound(mRPC, 12345-(50-1)).Maybe()
	mockNewBlockFilter(mRPC, testBlockFilterID1).Maybe()
	mockFilterChangesEmpty(mRPC).Maybe()

	h, ok := bl.GetHighestBlock(bl.ctx)
	assert.Equal(t, uint64(12345), h)
	assert.True(t, ok)
	done()

	<-bl.listenLoopDone

	mRPC.AssertExpectations(t)
}

func TestBlockListenerStartGettingHighestBlockFailBeforeStop(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	mockNewBlockFilter(mRPC, testBlockFilterID1).Maybe()
	mockFilterChangesEmpty(mRPC).Maybe()
	done() // Stop before we start

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").
		Return(&rpcbackend.RPCError{Message: "pop"}).Once()

	h, ok := bl.GetHighestBlock(bl.ctx)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), h)

	<-bl.listenLoopDone

	mRPC.AssertExpectations(t)
}

func TestBlockListenerSeedMonitoredHead_BlockFound(t *testing.T) {
	block951Hash := testBlockHashFor(951)

	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.MonitoredHeadLength = 50 // seed at block 1000-50+1 = 951
		mockSeedBlock(mRPC, 951, block951Hash).Once()
	})
	defer done()

	bl.canonicalChainLock.Lock()
	bl.highestBlock = 1000
	bl.highestBlockSet = true
	bl.canonicalChainLock.Unlock()

	bi := bl.seedMonitoredHead()

	require.NotNil(t, bi)
	assert.Equal(t, uint64(951), bi.Number.Uint64())
	assert.Equal(t, block951Hash, bi.Hash)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerSeedMonitoredHead_ReconcileAndDispatch(t *testing.T) {
	block951Hash := testBlockHashFor(951)

	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.MonitoredHeadLength = 50 // seed at block 1000-50+1 = 951

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlock(mRPC, 951, block951Hash).Once()
		mockNewBlockFilter(mRPC, testBlockFilterID1).Once()
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
			cancelCtx()
		}).Maybe()
	})
	defer done()

	// Register consumer directly to avoid AddConsumer's waitUntilStarted race.
	consumerID := fftypes.NewUUID()
	updates := make(chan *ffcapi.BlockHashEvent, 10)
	bl.consumerMux.Lock()
	bl.consumers[*consumerID] = &BlockUpdateConsumer{ID: consumerID, Ctx: context.Background(), Updates: updates}
	bl.consumerMux.Unlock()

	bl.checkAndStartListenerLoop()

	ev := <-updates
	assert.Equal(t, []string{block951Hash.String()}, ev.BlockHashes)

	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerOKSequential(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002Hash := testBlockHashFor(1002)
	block1003Hash := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.MonitoredHeadLength = 2 // seed at 1000-2+1 = 999

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 999)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002Hash).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001Hash)
		// block1003 has GasLimit set — inline to capture the extra field
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByHash", block1003Hash.String(), false).Return(nil).Run(func(args mock.Arguments) {
			*args[1].(**ethrpc.EVMBlockWithTxHashesJSONRPC) = &ethrpc.EVMBlockWithTxHashesJSONRPC{BlockHeaderJSONRPC: ethrpc.BlockHeaderJSONRPC{
				Number:        1003,
				Hash:          block1003Hash,
				ParentHash:    block1002Hash,
				GasLimit:      ethtypes.NewHexInteger64(10000),
				BaseFeePerGas: ethtypes.NewHexInteger64(7),
			}}
		})
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	consumerID := fftypes.NewUUID()
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      consumerID,
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String(), block1002Hash.String()}, bu.BlockHashes)
	bu = <-updates
	assert.Equal(t, []string{block1003Hash.String()}, bu.BlockHashes)
	assert.False(t, bu.GapPotential)

	bl.RemoveConsumer(context.Background(), consumerID)
	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
	assert.Len(t, bl.SnapshotMonitoredHeadChain(), bl.MonitoredHeadLength)
	require.Equal(t, int64(10000), bl.GetBlockGasLimit().Int64())
	headBlockInfo, ok := bl.GetHighestBlockInfo(context.Background())
	require.True(t, ok)
	require.True(t, headBlockInfo.SupportsEIP1559())
	require.Equal(t, int64(10000), headBlockInfo.GasLimit.Int64())
}

func TestBlockListenerWSShoulderTap(t *testing.T) {

	failedConnectOnce := false
	failedSubOnce := false
	toServer, fromServer, url, wsDone := wsclient.NewTestWSServer(func(req *http.Request) {
		if !failedConnectOnce {
			failedConnectOnce = true
			panic("fail once here")
		}
	})

	ctx, bl, _, done := newTestBlockListener(t)
	bl.wsBackend = rpcbackend.NewWSRPCClient(&wsclient.WSConfig{
		HTTPURL:                url,
		InitialConnectAttempts: 0,
	})
	svrDone := make(chan struct{})

	pingerDone := make(chan struct{})
	complete := false
	go func() {
		defer close(svrDone)
		for {
			select {
			case rpcStr := <-toServer:
				var rpcReq rpcbackend.RPCRequest
				err := json.Unmarshal([]byte(rpcStr), &rpcReq)
				assert.NoError(t, err)
				rpcRes := &rpcbackend.RPCResponse{
					JSONRpc: rpcReq.JSONRpc,
					ID:      rpcReq.ID,
				}
				switch rpcReq.Method {
				case "eth_blockNumber":
					rpcRes.Result = fftypes.JSONAnyPtr(`"0x12345"`)
				case "eth_subscribe":
					assert.Equal(t, "newHeads", rpcReq.Params[0].AsString())
					if !failedSubOnce {
						failedSubOnce = true
						rpcRes.Error = &rpcbackend.RPCError{
							Code:    int64(rpcbackend.RPCCodeInternalError),
							Message: "pop",
						}
					} else {
						rpcRes.Result = fftypes.JSONAnyPtr(fmt.Sprintf(`"%s"`, fftypes.NewUUID()))
						go func() {
							defer close(pingerDone)
							for !complete {
								time.Sleep(100 * time.Microsecond)
								if bl.newHeadsSub != nil {
									bl.newHeadsSub.Notifications() <- &rpcbackend.RPCSubscriptionNotification{
										CurrentSubID: bl.newHeadsSub.LocalID().String(),
										Result:       fftypes.JSONAnyPtr(`"anything"`),
									}
								}
							}
						}()
					}
				case "eth_getBlockByNumber":
					rpcRes.Result = fftypes.JSONAnyPtr("null") // seed block not found
				case "eth_newBlockFilter":
					rpcRes.Result = fftypes.JSONAnyPtr(fmt.Sprintf(`"%s"`, fftypes.NewUUID()))
				case "eth_getFilterChanges":
					complete = true
					<-pingerDone
					go done()
				default:
					assert.Fail(t, "unexpected RPC call: %+v", rpcReq)
				}
				b, err := json.Marshal(rpcRes)
				assert.NoError(t, err)
				fromServer <- string(b)
			case <-ctx.Done():
				return
			}
		}
	}()

	bl.checkAndStartListenerLoop()

	<-bl.listenLoopDone
	assert.True(t, failedConnectOnce)
	assert.True(t, failedSubOnce)

	wsDone()
	<-svrDone
}

func TestBlockListenerOKDuplicates(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002Hash := testBlockHashFor(1002)
	block1003Hash := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002Hash).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		// Third call returns duplicates and signals shutdown
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
			*args[1].(*[]ethtypes.HexBytes0xPrefix) = []ethtypes.HexBytes0xPrefix{block1002Hash, block1003Hash}
			cancelCtx()
		}).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001Hash)
		mockBlockByHash(mRPC, 1003, block1003Hash, block1002Hash)
	})
	defer done()

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	require.Equal(t, []string{block1001Hash.String(), block1002Hash.String()}, bu.BlockHashes)
	bu = <-updates
	require.Equal(t, []string{block1003Hash.String()}, bu.BlockHashes)
	require.False(t, bu.GapPotential)

	<-bl.listenLoopDone

	require.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgKeepLatestHeadInSameBatch(t *testing.T) {

	block1001HashA := testBlockHashFor(1001, 1111)
	block1001HashB := testBlockHashFor(1001)
	block1002Hash := testBlockHashFor(1002)
	block1003Hash := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001HashA, block1001HashB, block1002Hash, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001HashA, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1001, block1001HashB, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001HashB)
		mockBlockByHash(mRPC, 1003, block1003Hash, block1002Hash)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001HashB.String(), block1002Hash.String(), block1003Hash.String()}, bu.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgKeepLatestHeadInSameBatchValidHashFirst(t *testing.T) {

	// "Valid" (canonical) hash arrives first in the filter batch; the stale hash arrives after,
	// forcing a rebuild to confirm the canonical chain.
	block1001HashB := testBlockHashFor(1001)       // canonical — arrives first
	block1001HashA := testBlockHashFor(1001, 1111) // stale fork — arrives second
	block1002Hash := testBlockHashFor(1002)
	block1003Hash := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001HashB, block1001HashA, block1002Hash, block1003Hash)
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001HashB, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1001, block1001HashA, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001HashB)

		// Rebuild fetches 1001–1003 by number then hits nil at 1004
		mockBlockByNumber(mRPC, 1001, &block1001HashB)
		mockBlockByNumber(mRPC, 1002, &block1002Hash)
		mockBlockByNumber(mRPC, 1003, &block1003Hash)
		mockBlockByNumber(mRPC, 1004, nil)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001HashB.String(), block1002Hash.String(), block1003Hash.String()}, bu.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgKeepLatestMiddleInSameBatch(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002HashA := testBlockHashFor(1002, 1111)
	block1002HashB := testBlockHashFor(1002)
	block1003Hash := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002HashA, block1002HashB, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002HashA, block1001Hash)
		mockBlockByHash(mRPC, 1002, block1002HashB, block1001Hash)
		mockBlockByHash(mRPC, 1003, block1003Hash, block1002HashB)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String(), block1002HashB.String(), block1003Hash.String()}, bu.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgKeepLatestTailInSameBatch(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002Hash := testBlockHashFor(1002)
	block1003HashA := testBlockHashFor(1003, 1111)
	block1003HashB := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002Hash, block1003HashA, block1003HashB).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001Hash)
		mockBlockByHash(mRPC, 1003, block1003HashA, block1002Hash)
		mockBlockByHash(mRPC, 1003, block1003HashB, block1002Hash)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String(), block1002Hash.String(), block1003HashB.String()}, bu.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

// TestTrimToLastValidBlockRemovesOnlyInvalidSuffix covers the case where the in-memory tail
// diverges from the node but an older prefix still matches. The suffix must be removed without
// dropping the last matching block (regression for incorrect removal variable in the trim loop).
func TestTrimToLastValidBlockRemovesInvalidTail(t *testing.T) {
	h98 := testBlockHashFor(98)
	h99 := testBlockHashFor(99)
	h100 := testBlockHashFor(100)
	h101Stale := testBlockHashFor(101, 999)
	h102Stale := testBlockHashFor(102, 999)
	h101 := testBlockHashFor(101)
	h102 := testBlockHashFor(102)

	_, bl, mRPC, done := newTestBlockListener(t)
	defer done()

	mockBlockByNumber(mRPC, 102, &h102).Once()
	mockBlockByNumber(mRPC, 101, &h101).Once()
	mockBlockByNumber(mRPC, 100, &h100).Once()

	b99 := &ethrpc.BlockInfoJSONRPC{Number: ethtypes.HexUint64(99), Hash: h99, ParentHash: h98}
	b100 := &ethrpc.BlockInfoJSONRPC{Number: ethtypes.HexUint64(100), Hash: h100, ParentHash: h99}
	b101 := &ethrpc.BlockInfoJSONRPC{Number: ethtypes.HexUint64(101), Hash: h101Stale, ParentHash: h100}
	b102 := &ethrpc.BlockInfoJSONRPC{Number: ethtypes.HexUint64(102), Hash: h102Stale, ParentHash: h101Stale}

	bl.canonicalChainLock.Lock()
	bl.canonicalChain.PushBack(b99)
	bl.canonicalChain.PushBack(b100)
	bl.canonicalChain.PushBack(b101)
	bl.canonicalChain.PushBack(b102)

	lastValid := bl.trimToLastValidBlock()
	bl.canonicalChainLock.Unlock()

	require.NotNil(t, lastValid)
	require.Equal(t, uint64(100), lastValid.Number.Uint64())
	require.True(t, lastValid.Hash.Equals(h100))
	require.Equal(t, 2, bl.canonicalChain.Len())

	front := bl.canonicalChain.Front().Value.(*ethrpc.BlockInfoJSONRPC)
	require.Equal(t, uint64(99), front.Number.Uint64())
	require.True(t, front.Hash.Equals(h99))

	tail := bl.canonicalChain.Back().Value.(*ethrpc.BlockInfoJSONRPC)
	require.Equal(t, uint64(100), tail.Number.Uint64())
	require.True(t, tail.Hash.Equals(h100))

	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgReplaceTail(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002Hash := testBlockHashFor(1002)
	block1003HashA := testBlockHashFor(1003, 1111)
	block1003HashB := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002Hash).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003HashA).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003HashB).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002Hash, block1001Hash)
		mockBlockByHash(mRPC, 1003, block1003HashA, block1002Hash)
		mockBlockByHash(mRPC, 1003, block1003HashB, block1002Hash)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String(), block1002Hash.String()}, bu.BlockHashes)
	bu = <-updates
	assert.Equal(t, []string{block1003HashA.String()}, bu.BlockHashes)
	assert.False(t, bu.GapPotential)
	bu = <-updates
	assert.Equal(t, []string{block1003HashB.String()}, bu.BlockHashes)
	assert.False(t, bu.GapPotential)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerGap(t *testing.T) {

	// See issue https://github.com/hyperledger/firefly-evmconnect/issues/10
	// We have seen that certain JSON/RPC endpoints might miss blocks during re-orgs, and our listener
	// needs to cope with this. This means winding back when we find a gap and re-building our canonical
	// view of the chain.

	block1001Hash := testBlockHashFor(1001)
	block1002HashA := testBlockHashFor(1002, 1111)
	block1004Hash := testBlockHashFor(1004)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash, block1002HashA).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1004Hash).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1002, block1002HashA, block1001Hash)
		mockBlockByHash(mRPC, 1004, block1004Hash, testBlockHashFor(1003))

		// Rebuild: 1001 same hash, 1002 canonical (different from A), 1003–1005 fill in, 1006 not found
		mockBlockRangeWithHash(mRPC, 1001, 1005)
		mockBlockByNumber(mRPC, 1006, nil)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String(), block1002HashA.String()}, bu.BlockHashes)
	bu = <-updates
	// Rebuild yields canonical 1002, fills 1003, 1004, 1005
	assert.Equal(t, []string{
		testBlockHashFor(1002).String(),
		testBlockHashFor(1003).String(),
		testBlockHashFor(1004).String(),
		testBlockHashFor(1005).String(),
	}, bu.BlockHashes)
	assert.False(t, bu.GapPotential)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1005), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgWhileRebuilding(t *testing.T) {

	block1001Hash := testBlockHashFor(1001)
	block1002HashA := testBlockHashFor(1002, 1111)
	block1003HashA := testBlockHashFor(1003, 1111)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1001Hash).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003HashA).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1001, block1001Hash, testBlockHashFor(1000))
		mockBlockByHash(mRPC, 1003, block1003HashA, block1001Hash)

		// Rebuild: 1001 confirmed, 1002 fork A, 1003 canonical (different parent stops rebuild at 1002A)
		mockBlockByNumber(mRPC, 1001, &block1001Hash)
		mockBlockByNumber(mRPC, 1002, &block1002HashA)
		// Block 1003 from node has canonical parent (testBlockHashFor(1002)), mismatching 1002HashA
		block1003Canon := testBlockHashFor(1003)
		mockBlockByNumber(mRPC, 1003, &block1003Canon)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1001Hash.String()}, bu.BlockHashes)
	bu = <-updates
	assert.Equal(t, []string{block1002HashA.String()}, bu.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReorgReplaceWholeCanonicalChain(t *testing.T) {

	block1002HashA := testBlockHashFor(1002, 1111)
	block1003HashA := testBlockHashFor(1003, 1111)
	block1002HashB := testBlockHashFor(1002)
	block1003HashB := testBlockHashFor(1003)

	startLatch := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, startLatch, block1002HashA, block1003HashA).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003HashB).Once()
		mockFilterChangesEmpty(mRPC)

		mockBlockByHash(mRPC, 1002, block1002HashA, testBlockHashFor(1001))
		mockBlockByHash(mRPC, 1003, block1003HashA, block1002HashA)
		mockBlockByHash(mRPC, 1003, block1003HashB, block1002HashB)

		// Rebuild replaces entire chain: 1002B, 1003B, then nil at 1004
		mockBlockByNumber(mRPC, 1002, &block1002HashB)
		mockBlockByNumber(mRPC, 1003, &block1003HashB)
		mockBlockByNumber(mRPC, 1004, nil)
	})

	updates := make(chan *ffcapi.BlockHashEvent)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	bu := <-updates
	assert.Equal(t, []string{block1002HashA.String(), block1003HashA.String()}, bu.BlockHashes)
	bu = <-updates
	assert.Equal(t, []string{block1002HashB.String(), block1003HashB.String()}, bu.BlockHashes)
	assert.False(t, bu.GapPotential)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1003), bl.highestBlock)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerClosed(t *testing.T) {

	block1003Hash := testBlockHashFor(1003)

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)

		mockBlockByHash(mRPC, 1003, block1003Hash, testBlockHashFor(1002))
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	updates := make(chan *ffcapi.BlockHashEvent)
	cancelledCtx, cCancel := context.WithCancel(context.Background())
	cCancel()
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     cancelledCtx,
		Updates: updates,
	})

	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerBlockNotFound(t *testing.T) {

	block1003Hash := testBlockHashFor(1003)

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)

		mockBlockByHashNotFound(mRPC, block1003Hash)
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerBlockHashFailed(t *testing.T) {

	block1003Hash := testBlockHashFor(1003)

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)

		mockBlockByHashFail(mRPC, block1003Hash)
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerProcessNonStandardHashRejectedWhenNotInHederaCompatibilityMode(t *testing.T) {

	block1003Hash := ethtypes.MustNewHexBytes0xPrefix("0xef177df3b87beed681b1557e8ba7c3ecbd7e4db83d87b66c1e86aa484937ab93f1fae0eb6d4b24ca30aee13f29c83cc9")

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.HederaCompatibilityMode = false

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerProcessNonStandardHashRejectedWhenWrongSizeForHedera(t *testing.T) {

	block1003Hash := ethtypes.MustNewHexBytes0xPrefix("0xef")

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.HederaCompatibilityMode = true

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerProcessNonStandardHashAcceptedWhenInHederaCompatbilityMode(t *testing.T) {

	block1003Hash := ethtypes.MustNewHexBytes0xPrefix("0xef177df3b87beed681b1557e8ba7c3ecbd7e4db83d87b66c1e86aa484937ab93f1fae0eb6d4b24ca30aee13f29c83cc9")
	truncatedBlock1003Hash := ethtypes.MustNewHexBytes0xPrefix("0xef177df3b87beed681b1557e8ba7c3ecbd7e4db83d87b66c1e86aa484937ab93")

	waitCalled := newTestLatch()
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.HederaCompatibilityMode = true

		mockInitialBlockHeight(mRPC, 1000)
		mockSeedBlockNotFound(mRPC, 951)
		mockNewBlockFilter(mRPC, testBlockFilterID1)
		mockFilterChanges(mRPC, testBlockFilterID1, nil, block1003Hash).Once()
		mockFilterChangesEmpty(mRPC, waitCalled.complete)

		mockBlockByHashFail(mRPC, truncatedBlock1003Hash)
	})
	go func() {
		waitCalled.waitComplete()
		done()
	}()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReestablishBlockFilter(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	bl.BlockPollingInterval = shortDelay

	mockInitialBlockHeight(mRPC, 1000)
	mockSeedBlockNotFound(mRPC, 951)
	mockNewBlockFilter(mRPC, testBlockFilterID1).Once()
	mockNewBlockFilter(mRPC, testBlockFilterID2).Once()
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).
		Return(&rpcbackend.RPCError{Message: "filter not found"}).Once()
	mockFilterChangesEmpty(mRPC, func() { go done() })

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerReestablishBlockFilterFail(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	bl.BlockPollingInterval = shortDelay

	mockInitialBlockHeight(mRPC, 1000)
	mockSeedBlockNotFound(mRPC, 951)
	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_newBlockFilter").
		Return(&rpcbackend.RPCError{Message: "pop"}).Run(func(args mock.Arguments) {
		go done()
	})

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerWaitUntilStartedOnlyReturnsAfterEstablishingBlockFilter(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	bl.BlockPollingInterval = shortDelay

	mockInitialBlockHeight(mRPC, 1000)
	mockSeedBlockNotFound(mRPC, 951)
	mockNewBlockFilter(mRPC, testBlockFilterID1)
	mockFilterChangesEmpty(mRPC)

	assert.False(t, bl.isStarted)
	bl.checkAndStartListenerLoop()
	bl.waitUntilStarted(context.Background())
	assert.True(t, bl.isStarted)
	_, ok := <-bl.startDone
	if ok {
		t.Errorf("Expected new block filter established signal channel to be closed")
	}

	done()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

// TestBlockListenerHeadBlockNumber_DispatchesAndSkipsDuplicateHead exercises listenLoop head-only mode:
// eth_blockNumber refresh updates currentChainHead and dispatches BlockHashEvent with HeadBlockNumber;
// when the RPC head is unchanged, no event is sent.
func TestBlockListenerHeadBlockNumber_DispatchesAndSkipsDuplicateHead(t *testing.T) {
	startLatch := newTestLatch()
	var bnCall int
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, _ context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.ChainTrackingMode = ffcapi.ChainTrackingModeLight

		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(nil).Run(func(args mock.Arguments) {
			bnCall++
			var v uint64
			switch bnCall {
			case 1:
				v = 1000 // establishBlockHeightWithRetry
			case 2:
				v = 1000 // first refresh: currentChainHead was 0 → dispatch
			case 3:
				v = 1000 // second refresh: same as currentChainHead → no dispatch
			case 4:
				v = 1001 // head advanced → dispatch
			default:
				v = 1001
			}
			*args[1].(*ethtypes.HexInteger) = *ethtypes.NewHexIntegerU64(v)
		}).Maybe()

		mockNewBlockFilter(mRPC, testBlockFilterID1).Once()

		var getFilterCalls int
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getFilterChanges", testBlockFilterID1).Return(nil).Run(func(args mock.Arguments) {
			getFilterCalls++
			if getFilterCalls == 1 {
				startLatch.waitComplete()
			}
			*args[1].(*[]ethtypes.HexBytes0xPrefix) = nil
		}).Maybe()
	})
	defer done()

	updates := make(chan *ffcapi.BlockHashEvent, 16)
	bl.AddConsumer(context.Background(), &BlockUpdateConsumer{
		ID:      fftypes.NewUUID(),
		Ctx:     context.Background(),
		Updates: updates,
	})
	startLatch.complete()

	ev1 := <-updates
	assert.Equal(t, uint64(1000), ev1.HeadBlockNumber)
	assert.False(t, ev1.GapPotential)
	assert.Empty(t, ev1.BlockHashes)

	ev2 := <-updates
	assert.Equal(t, uint64(1001), ev2.HeadBlockNumber)
	assert.False(t, ev2.GapPotential)
	assert.Empty(t, ev2.BlockHashes)

	done()
	<-bl.listenLoopDone

	assert.Equal(t, uint64(1001), bl.currentChainHead)
	mRPC.AssertExpectations(t)
}

func TestBlockListenerLightModeRefreshChainHeadFailure(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t, func(conf *BlockListenerConfig, mRPC *rpcbackendmocks.Backend, cancelCtx context.CancelFunc) {
		conf.BlockPollingInterval = shortDelay
		conf.ChainTrackingMode = ffcapi.ChainTrackingModeLight

		mockInitialBlockHeight(mRPC, 1000)
		mockNewBlockFilter(mRPC, testBlockFilterID1).Once()
		mockFilterChanges(mRPC, testBlockFilterID1, nil).Once()
		mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").Return(&rpcbackend.RPCError{Message: "pop"}).Run(func(args mock.Arguments) {
			cancelCtx()
		}).Once()
	})
	defer done()

	bl.checkAndStartListenerLoop()
	bl.WaitClosed()
	mRPC.AssertExpectations(t)
}

func TestBlockListenerDispatchStopped(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	done()

	bl.dispatchToConsumers([]*BlockUpdateConsumer{
		{ID: fftypes.NewUUID(), Ctx: context.Background(), Updates: make(chan<- *ffcapi.BlockHashEvent)},
	}, &ffcapi.BlockHashEvent{
		BlockHashes: []string{},
	})
}

func TestBlockListenerRebuildCanonicalChainEmpty(t *testing.T) {

	_, bl, _, done := newTestBlockListener(t)
	defer done()

	res := bl.rebuildCanonicalChain()
	assert.Nil(t, res)
}

func TestBlockListenerRebuildCanonicalFailTerminate(t *testing.T) {

	_, bl, mRPC, done := newTestBlockListener(t)
	bl.canonicalChain.PushBack(&ethrpc.BlockInfoJSONRPC{
		Number:     ethtypes.HexUint64(1000),
		Hash:       testBlockHashFor(1000),
		ParentHash: testBlockHashFor(999),
	})

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getBlockByNumber", mock.Anything, false).
		Return(&rpcbackend.RPCError{Message: "pop"}).
		Run(func(args mock.Arguments) {
			done()
		})

	res := bl.rebuildCanonicalChain()
	assert.Nil(t, res)

	mRPC.AssertExpectations(t)
}

func TestBlockListenerWaitNextIterationInterval(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	done()
	bl.ctx = context.Background()
	bl.newHeadsTap = make(chan struct{}, 1)
	bl.BlockPollingInterval = 10 * time.Microsecond
	bl.waitNextIteration()
}

func TestBlockListenerWaitNextIterationTap(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)
	done()
	bl.ctx = context.Background()
	bl.BlockPollingInterval = 1 * time.Hour

	newHeads := make(chan *rpcbackend.RPCSubscriptionNotification, 1)
	newHeads <- &rpcbackend.RPCSubscriptionNotification{}
	mnh := rpcbackendmocks.NewSubscription(t)
	mnh.On("Notifications").Return(newHeads)
	bl.newHeadsSub = mnh
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		bl.newHeadsSubListener()
	}()

	bl.waitNextIteration()
	close(newHeads)

	<-subDone
}

func TestWaitUntilStartedCancelledCtx(t *testing.T) {
	_, bl, _, done := newTestBlockListener(t)

	bl.UTSetBackend(nil)
	require.Nil(t, bl.GetBackend())

	cancelledFgCtx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()
	bl.waitUntilStarted(cancelledFgCtx)

	done()
	bl.waitUntilStarted(context.Background())
}

func TestCheckAndSetHighestBlock(t *testing.T) {
	_, bl, _, _ := newTestBlockListener(t)

	bi500 := &ethrpc.BlockInfoJSONRPC{
		Number:   500,
		GasLimit: ethtypes.NewHexInteger64(10000),
	}
	bl.canonicalChainLock.Lock()
	bl.checkAndSetHighestBlock(bi500)
	require.Equal(t, uint64(500), bl.highestBlock)
	require.True(t, bl.highestBlockSet)
	require.Same(t, bi500, bl.headBlockInfo)

	bi500EIP1559 := &ethrpc.BlockInfoJSONRPC{
		Number:        500,
		GasLimit:      ethtypes.NewHexInteger64(20000),
		BaseFeePerGas: ethtypes.NewHexInteger64(7),
	}
	bl.checkAndSetHighestBlock(bi500EIP1559)
	require.Same(t, bi500EIP1559, bl.headBlockInfo)

	bl.checkAndSetHighestBlock(&ethrpc.BlockInfoJSONRPC{Number: 499})
	require.Same(t, bi500EIP1559, bl.headBlockInfo)
	bl.canonicalChainLock.Unlock()
}

func TestGetBlockGasLimitFromHeadBlockInfo(t *testing.T) {
	_, bl, _, _ := newTestBlockListener(t)
	require.Nil(t, bl.GetBlockGasLimit())

	bl.canonicalChainLock.Lock()
	bl.headBlockInfo = &ethrpc.BlockInfoJSONRPC{GasLimit: ethtypes.NewHexInteger64(0)}
	bl.canonicalChainLock.Unlock()
	require.Nil(t, bl.GetBlockGasLimit())

	bl.canonicalChainLock.Lock()
	bl.headBlockInfo = &ethrpc.BlockInfoJSONRPC{}
	bl.canonicalChainLock.Unlock()
	require.Nil(t, bl.GetBlockGasLimit())
}

func TestGetHighestBlockInfoBeforeHeadBlockSeen(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)

	mockInitialBlockHeight(mRPC, 500)
	mockSeedBlockNotFound(mRPC, 500-(50-1)).Maybe()
	mockNewBlockFilter(mRPC, testBlockFilterID1).Maybe()
	mockFilterChangesEmpty(mRPC).Maybe()

	_, ok := bl.GetHighestBlock(bl.ctx)
	require.True(t, ok)

	headInfo, ok := bl.GetHighestBlockInfo(bl.ctx)
	require.False(t, ok)
	require.Nil(t, headInfo)

	done()
}

func TestGetHighestBlockInfoReturnsHeadBlock(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)

	mockInitialBlockHeight(mRPC, 123)
	mockSeedBlockNotFound(mRPC, 123-(50-1)).Maybe()
	mockNewBlockFilter(mRPC, testBlockFilterID1).Maybe()
	mockFilterChangesEmpty(mRPC).Maybe()

	bi := &ethrpc.BlockInfoJSONRPC{
		Number:        123,
		BaseFeePerGas: ethtypes.NewHexInteger64(1),
		GasLimit:      ethtypes.NewHexInteger64(5000),
	}
	bl.canonicalChainLock.Lock()
	bl.headBlockInfo = bi
	bl.canonicalChainLock.Unlock()

	headInfo, ok := bl.GetHighestBlockInfo(bl.ctx)
	require.True(t, ok)
	require.Same(t, bi, headInfo)
	require.True(t, headInfo.SupportsEIP1559())
	require.Equal(t, int64(5000), bl.GetBlockGasLimit().Int64())

	done()
}

func TestGetHighestBlockInfoCancelledBeforeInit(t *testing.T) {
	_, bl, mRPC, done := newTestBlockListener(t)
	mockNewBlockFilter(mRPC, testBlockFilterID1).Maybe()
	mockFilterChangesEmpty(mRPC).Maybe()
	done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_blockNumber").
		Return(&rpcbackend.RPCError{Message: "pop"})

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := bl.GetHighestBlockInfo(cancelledCtx)
	require.False(t, ok)

	<-bl.listenLoopDone
}
