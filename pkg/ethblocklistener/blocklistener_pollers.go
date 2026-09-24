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

package ethblocklistener

import (
	"container/list"
	"iter"
	"slices"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// blockPoller performs one iteration of the listen loop, for one combination of chain tracking mode
// and filter polling mode. It logs and records its own failures - the returned error only drives
// the loop's backoff.
type blockPoller interface {
	poll() error
}

// newBlockPoller picks the poller for the configured modes, and does the one-off work before the loop
// starts: seeding the canonical chain, and marking the listener started where no filter has to be
// established first (consumers can only be added once started).
func (bl *blockListener) newBlockPoller() blockPoller {
	if bl.ChainTrackingMode == ffcapi.ChainTrackingModeLight {
		bl.markStarted()
		return &headNumberPoller{bl: bl}
	}
	bl.seedMonitoredHead()
	if bl.FilterPollingMode == FilterPollingModeClient {
		bl.markStarted()
		return &latestBlockPoller{canonicalChainPollerBase{bl: bl, gapPotential: true}}
	}
	return &blockFilterPoller{canonicalChainPollerBase: canonicalChainPollerBase{bl: bl, gapPotential: true}}
}

// headNumberPoller is light chain tracking mode, in either filter polling mode. There is no canonical
// chain being built, so the head we dispatch to consumers is what we report as the canonical height -
// both through GetHeadBlockNumber (used by FFTM's head-number confirmation checks) and GetHighestBlock
// (used by event streams). The head is the highest reading observed, so it is forward-only across
// load-balanced nodes at different heights (see acceptHeadBlockNumber).
type headNumberPoller struct {
	bl *blockListener
}

func (p *headNumberPoller) poll() error {
	bl := p.bl
	head, err := bl.queryBlockHeightFromRPC()
	if err != nil {
		log.L(bl.ctx).Errorf("Failed to refresh chain head: %s", err)
		return err
	}
	if !bl.acceptHeadBlockNumber(head) {
		return nil
	}
	update := &ffcapi.BlockHashEvent{GapPotential: false, Created: fftypes.Now(), HeadBlockNumber: head}
	bl.dispatchToConsumers(bl.snapshotConsumers(), update)
	return nil
}

// canonicalChainPollerBase is the part shared by the full chain tracking pollers: reconciling the blocks a
// poll discovered into the canonical chain, and notifying consumers from the lowest changed position.
type canonicalChainPollerBase struct {
	bl           *blockListener
	gapPotential bool // true until the first successful poll, and again while a lost block filter is re-established
}

func (p *canonicalChainPollerBase) reconcileAndDispatch(blocks iter.Seq[*ethrpc.BlockInfoJSONRPC]) {
	bl := p.bl
	var notifyPos *list.Element
	for bi := range blocks {
		candidate := bl.reconcileCanonicalChain(bi)
		if candidate != nil && (notifyPos == nil || candidate.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64() <= notifyPos.Value.(*ethrpc.BlockInfoJSONRPC).Number.Uint64()) {
			notifyPos = candidate
		}
	}
	if notifyPos != nil {
		// We notify for all hashes from the point of change in the chain onwards
		update := &ffcapi.BlockHashEvent{GapPotential: p.gapPotential, Created: fftypes.Now()}
		for ; notifyPos != nil; notifyPos = notifyPos.Next() {
			update.BlockHashes = append(update.BlockHashes, notifyPos.Value.(*ethrpc.BlockInfoJSONRPC).Hash.String())
		}
		bl.dispatchToConsumers(bl.snapshotConsumers(), update)
	}
	p.gapPotential = false
}

// blockFilterPoller is full chain tracking with server filter polling mode: a node-side block filter
// (re)established with eth_newBlockFilter, and polled with eth_getFilterChanges for new block hashes.
type blockFilterPoller struct {
	canonicalChainPollerBase
	filter string
}

func (p *blockFilterPoller) poll() error {
	bl := p.bl

	// The filter poll never queries the height the node reports, so refresh it for the target metric first
	bl.refreshTargetBlockHeightMetric()

	if p.filter == "" {
		if err := bl.rpc.CallRPC(bl.ctx, &p.filter, "eth_newBlockFilter"); err != nil {
			log.L(bl.ctx).Errorf("Failed to establish new block filter: %s", err.Message)
			bl.incPollFailureMetric("eth_newBlockFilter")
			return err.Error()
		}
		bl.markStarted()
	}

	var blockHashes []ethtypes.HexBytes0xPrefix
	if err := bl.rpc.CallRPC(bl.ctx, &blockHashes, "eth_getFilterChanges", p.filter); err != nil {
		if etherrors.MapError(etherrors.FilterRPCMethods, err.Error()) == ffcapi.ErrorReasonNotFound {
			log.L(bl.ctx).Warnf("Block filter '%v' no longer valid. Recreating filter: %s", p.filter, err.Message)
			p.filter = ""
			p.gapPotential = true
		}
		log.L(bl.ctx).Errorf("Failed to query block filter changes: %s", err.Message)
		bl.incPollFailureMetric("eth_getFilterChanges")
		return err.Error()
	}
	log.L(bl.ctx).Debugf("Block filter received new block hashes: %+v", blockHashes)

	p.reconcileAndDispatch(bl.resolveBlockHashes(blockHashes))
	return nil
}

// resolveBlockHashes looks up the blocks advertised by the block filter (which then go into our cache),
// skipping any that cannot be resolved - a block gone by the time we ask is assumed re-orged away.
// The lookups are lazy, so each happens just before that block is reconciled: a chain rebuild triggered
// by one block fills the cache for the hashes after it.
func (bl *blockListener) resolveBlockHashes(blockHashes []ethtypes.HexBytes0xPrefix) iter.Seq[*ethrpc.BlockInfoJSONRPC] {
	return func(yield func(*ethrpc.BlockInfoJSONRPC) bool) {
		for _, h := range blockHashes {
			if bi := bl.resolveBlockHash(h); bi != nil && !yield(bi) {
				return
			}
		}
	}
}

func (bl *blockListener) resolveBlockHash(h ethtypes.HexBytes0xPrefix) *ethrpc.BlockInfoJSONRPC {
	if len(h) != 32 {
		if !bl.HederaCompatibilityMode {
			log.L(bl.ctx).Errorf("Attempted to index block header with non-standard length: %d", len(h))
			return nil
		}

		if len(h) < 32 {
			log.L(bl.ctx).Errorf("Cannot index block header hash of length: %d", len(h))
			return nil
		}

		h = h[0:32]
	}

	bi, err := bl.GetBlockInfoByHash(bl.ctx, h.String())
	switch {
	case err != nil:
		log.L(bl.ctx).Debugf("Failed to query block '%s': %s", h, err)
		return nil
	case bi == nil:
		log.L(bl.ctx).Debugf("Block '%s' no longer available after notification (assuming due to re-org)", h)
		return nil
	}
	return bi
}

// latestBlockPoller is full chain tracking with client filter polling mode: no node-side state, the head
// block is fetched each poll and reconciled into the canonical chain. A head that does not fit on our
// tail - a gap of blocks, or a re-org - is resolved by the chain rebuild in reconcileCanonicalChain,
// exactly as when a block filter poll skips blocks.
type latestBlockPoller struct {
	canonicalChainPollerBase
}

func (p *latestBlockPoller) poll() error {
	bl := p.bl
	latestBlock, err := bl.GetEVMBlockWithTxHashesByNumber(bl.ctx, "latest")
	if err == nil && latestBlock == nil {
		err = i18n.NewError(bl.ctx, msgs.MsgLatestBlockNotFound)
	}
	if err != nil {
		log.L(bl.ctx).Errorf("Failed to query latest block: %s", err)
		bl.incPollFailureMetric("eth_getBlockByNumber")
		return err
	}
	latest := latestBlock.ToBlockInfo(bl.IncludeLogsBloom)
	bl.setBlockHeightMetric(metricTargetBlockHeight, latest.Number.Uint64())
	log.L(bl.ctx).Debugf("Latest block %d / %s", latest.Number.Uint64(), latest.Hash)

	p.reconcileAndDispatch(slices.Values([]*ethrpc.BlockInfoJSONRPC{latest}))
	return nil
}
