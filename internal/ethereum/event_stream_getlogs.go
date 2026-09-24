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
	"bytes"
	"context"
	"time"

	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// State required when doing all management of polling position client-side
type getLogsPollState struct {
	fromBlock       int64                      // full mode: the next block to poll. light mode: the committed base of the re-scan window
	polledChain     []*ethrpc.BlockInfoJSONRPC // full mode only: sparse ascending (number, hash) records of polled blocks in the unstable window
	deliveredBlocks map[string]int64           // light mode only: blockHash->number for block-versions whose logs we have already delivered
}

// reset (re-)establishes the poll position, discarding any recorded hash continuity or
// delivered-block records (redelivery of the unstable window is de-duplicated downstream)
func (ps *getLogsPollState) reset(fromBlock int64) {
	ps.fromBlock = fromBlock
	ps.polledChain = nil
	ps.deliveredBlocks = nil
}

// filterDelivered strips logs from block-versions we already delivered on a previous sweep of the
// re-scan window (light mode). The block hash commits to the entire content of a block, so logs
// returned for a block hash we have seen are guaranteed identical to the ones we already
// processed - and a re-org replacing a block gives its logs a new block hash, so they pass
// through as new detections.
func (ps *getLogsPollState) filterDelivered(logs []*ethrpc.LogJSONRPC) []*ethrpc.LogJSONRPC {
	if len(ps.deliveredBlocks) == 0 {
		return logs
	}
	newLogs := make([]*ethrpc.LogJSONRPC, 0, len(logs))
	for _, l := range logs {
		if _, delivered := ps.deliveredBlocks[string(l.BlockHash)]; !delivered {
			newLogs = append(newLogs, l)
		}
	}
	return newLogs
}

// advanceRescan moves the light mode poll state forwards after successfully dispatching a page:
// the newly delivered block-versions are recorded for de-duplication on later sweeps, the
// committed window base moves to newBase (the page end, capped at the stable threshold), and
// records for blocks that have become stable are dropped (they are never re-scanned).
//
// Every page starts at the committed base, so a page that reaches the stable threshold is the
// last page of a sweep - the base holds at the threshold and the next cycle re-scans the whole
// unstable window [stableHead, head] from there. The catchup page size is at least
// checkpointBlockGap+1, so that window is always covered by a single page: a node that (against
// the requirement stated on ffcapi.ChainTrackingModeLight) silently truncated a page at its own
// head can only have lost events the next sweep re-scans, never events below a committed position.
func (ps *getLogsPollState) advanceRescan(newLogs []*ethrpc.LogJSONRPC, newBase int64) {
	for _, l := range newLogs {
		if ps.deliveredBlocks == nil {
			ps.deliveredBlocks = map[string]int64{}
		}
		ps.deliveredBlocks[string(l.BlockHash)] = trimUint64(l.BlockNumber.Uint64())
	}
	ps.fromBlock = newBase
	for h, n := range ps.deliveredBlocks {
		if n < newBase {
			delete(ps.deliveredBlocks, h)
		}
	}
}

// checkReorgRewind is used in full chain tracking mode only (light mode re-scans the unstable
// window and de-duplicates instead - see leadGroupSteadyStateGetLogs).
// It compares the canonical chain view recorded at the time we scanned each range, against the
// block listener's current canonical chain view. On a mismatch the view of the chain has changed
// behind our poll position (a re-org, or a stale view at scan time), so we rewind to the earliest
// diverging block to re-scan from there.
func (ps *getLogsPollState) checkReorgRewind(ctx context.Context, headChain []*ethrpc.BlockInfoJSONRPC) {
	if len(headChain) == 0 || len(ps.polledChain) == 0 {
		return
	}
	// Prune records that have aged out below the base of the monitored window - those blocks are
	// now considered stable, and we have nothing to compare them against
	baseBlock := blockNumberToInt64(headChain[0].Number.Uint64())
	firstInWindow := 0
	for firstInWindow < len(ps.polledChain) && blockNumberToInt64(ps.polledChain[firstInWindow].Number.Uint64()) < baseBlock {
		firstInWindow++
	}
	ps.polledChain = ps.polledChain[firstInWindow:]
	// Find the earliest block we polled whose hash is no longer canonical (lists are short and ordered, so linear scan is efficient)
	for i, polled := range ps.polledChain {
		polledNumber := blockNumberToInt64(polled.Number.Uint64())
		canonicalHash := blockHashInHeadChain(headChain, polledNumber)
		if canonicalHash == nil {
			continue // above the top of the current window - nothing to compare against
		}
		if !bytes.Equal(canonicalHash, polled.Hash) {
			log.L(ctx).Infof("Re-org detected at block %d (polled hash %s, now %s) - rewinding poll position from %d to %d", polledNumber, polled.Hash, canonicalHash, ps.fromBlock, polledNumber)
			ps.fromBlock = polledNumber
			ps.polledChain = ps.polledChain[:i] // records at/after the divergence are no longer valid
			return
		}
	}
}

// advance moves the poll position forwards after successfully processing blocks up to toBlock,
// recording the canonical hashes we hold for the polled range so a re-org behind the new position
// can be detected by checkReorgRewind on a later cycle.
//
// Note the hashes come from the headChain snapshot taken before the eth_getLogs query - if the
// chain re-organizes in between, the recorded hash and the queried logs can disagree, but the next
// cycle's continuity check then mismatches the new canonical view and rewinds us to re-poll.
func (ps *getLogsPollState) advance(headChain []*ethrpc.BlockInfoJSONRPC, toBlock int64) {
	for _, bi := range headChain {
		if n := blockNumberToInt64(bi.Number.Uint64()); n >= ps.fromBlock && n <= toBlock {
			ps.polledChain = append(ps.polledChain, bi)
		}
	}
	ps.fromBlock = toBlock + 1
}

// blockHashInHeadChain returns the hash of the given block number in the supplied canonical chain
// snapshot, or nil if that block number is not within the snapshot
func blockHashInHeadChain(headChain []*ethrpc.BlockInfoJSONRPC, blockNumber int64) ethtypes.HexBytes0xPrefix {
	for _, bi := range headChain {
		if blockNumberToInt64(bi.Number.Uint64()) == blockNumber {
			return bi.Hash
		}
	}
	return nil
}

// steadyStateScanCeiling returns the highest block the steady-state scan may poll to.
// Light mode always scans to chainHead. Full mode scans to min(chainHead, snapshotTop),
// but never above the stable threshold unless the block listener snapshot already covers that block -
// checkReorgRewind needs the hash from the snapshot taken before eth_getLogs.
// In normal operation the snapshot reaches the head (backfilled at startup), so this rarely binds.
// it only holds the scan back while the snapshot is still catching up, e.g. when startup backfill failed.
func steadyStateScanCeiling(lightMode bool, chainHead, stableHead int64, headChain []*ethrpc.BlockInfoJSONRPC) int64 {
	if lightMode {
		return chainHead
	}
	verifiableTo := stableHead
	if len(headChain) > 0 {
		if snapTop := blockNumberToInt64(headChain[len(headChain)-1].Number.Uint64()); snapTop > verifiableTo {
			verifiableTo = snapTop
		}
	}
	if chainHead < verifiableTo {
		return chainHead
	}
	return verifiableTo
}

// leadGroupSteadyStateGetLogs is the alternative steady state to leadGroupSteadyState, selected with
// events.filterPollingMode: client.
//
// Instead of establishing a node-side filter, we track our own in-memory poll position and page
// forwards with stateless eth_getLogs range queries.
//
// Detection in this function is decoupled from confirmation (in FFTM).
// The confirmation manager waits for its own configured number of confirmations after each
// event's block (immediate for an event that arrives already deep enough),
// delivers to the application, and waits for the ack.
//
// For light mode that is based just on a comparison of event vs. head block numbers.
// For full mode there is a client-side tracking of the full unstable head and the confirmation
// list is re-calculated client-side.
//
// Details on polling approach:
//
//   - FULL: We take a snapshot of the block listener's current view of the unstable head
//     of the chain before we poll for events. Each poll we check if a new fork is apparent
//     and re-poll from the position of the new fork in the chain.
//     While we cannot be certain the chain we poll is the same we had a client-side view of
//     before the poll, we know we will detect further divergence on subsequent poll cycles
//     (as long as it occurs within the checkpointBlockGap).
//
//   - LIGHT: Full blocks are not available to compare, so instead we re-scan the whole unstable
//     window (the checkpointBlockGap blocks below the head) on every sweep, de-duplicating what
//     we already delivered by block hash.
//
// Checkpoints come from two places:
//
//   - Ack-based: each delivered event carries its own checkpoint, persisted by FFTM as batches
//     are acknowledged. When events are flowing, the checkpoint moves forwards with delivery.
//
//   - Inactivity: when no events are flowing, FFTM periodically polls our high water mark (see
//     getHWM) recording how far we have scanned, held back checkpointBlockGap from the head.
//     The LastDetected floor in that response stops an inactivity checkpoint overtaking a
//     detected-but-unacknowledged event, so anything FFTM had not finished delivering is
//     re-detected after a crash, at any confirmation count.
//
// This means on restart, processing will either:
//
//   - Continue from the most recent confirmed acknowledged event, which could be the
//     confirmation count back from the head (configured to 6 or 10 etc.).
//   - Continue at least checkpointBlockGap behind wherever the head was before the restart,
//     possibly significantly longer based on when the inactivity checkpoint was captured.
func (es *eventStream) leadGroupSteadyStateGetLogs() bool {
	var ag *aggregatedListener
	lastUpdate := -1
	failCount := 0
	poll := &getLogsPollState{fromBlock: -1}
	for {
		if es.c.retry.DoFailureDelay(es.ctx, failCount) {
			log.L(es.ctx).Debugf("Stream loop exiting")
			return true
		}

		// Build the aggregated listener list if it has changed
		listenerChanged := es.buildReuseLeadGroupListener(&lastUpdate, &ag)

		caughtUpToHead := true

		// No need to poll for events, if we don't have any listeners
		if len(ag.signatureSet) > 0 {

			// The block listener maintains a view of the highest block. In light mode this is the
			// highest head observed across the (possibly load-balanced) nodes, and only goes down
			// for a chain reset. This call is just grabbing the current in-memory value.
			chainHeadBlock, ok := es.c.blockListener.GetHighestBlock(es.ctx)
			if !ok {
				log.L(es.ctx).Debugf("Stream loop exiting (closed checking block height)")
				return true
			}
			chainHead := blockNumberToInt64(chainHeadBlock)

			// (Re-)establish the poll position from the earliest listener HWM if we need to,
			// just as filter mode (re-)establishes the fromBlock of its filter
			if poll.fromBlock < 0 || listenerChanged {
				fromBlock := int64(-1)
				for _, l := range ag.listeners {
					if lHWM := l.getHWMBlock(); fromBlock < 0 || lHWM < fromBlock {
						fromBlock = lHWM
					}
				}
				poll.reset(fromBlock)
			}

			// The stable threshold is checkpointBlockGap behind the head, below which
			// re-orgs are not expected
			stableHead := chainHead - es.c.checkpointBlockGap
			if stableHead < 0 {
				stableHead = 0
			}

			// Check we're not outside of the steady state window, and need to fall back to catchup
			// mode. Catchup only polls up to the stable threshold, so we measure against the same
			// point - the two loops can never disagree and bounce control between each other.
			if (stableHead - poll.fromBlock) > es.c.catchupThreshold {
				log.L(es.ctx).Warnf("Block gap reached %d (above threshold of %d) - reverting to catchup mode", stableHead-poll.fromBlock, es.c.catchupThreshold)
				return false
			}

			// Both modes poll all the way to the head. What differs is how a re-org behind the
			// poll position is repaired:
			// In full chain tracking mode we check the blocks we already polled are still
			// canonical against the block listener's chain view, rewinding our position if not.
			// In light chain tracking mode there is no canonical chain view to check against, so
			// instead we re-scan the whole unstable window on every sweep, de-duplicating what we
			// already delivered by block hash - the committed position (poll.fromBlock) only ever
			// advances to the stable threshold, and each sweep scans from there to the head
			lightMode := es.c.chainTrackingMode == ffcapi.ChainTrackingModeLight
			var headChain []*ethrpc.BlockInfoJSONRPC
			if !lightMode {
				headChain = es.c.blockListener.SnapshotMonitoredHeadChain()
				poll.checkReorgRewind(es.ctx, headChain) // may rewind poll.fromBlock
			}
			scanFrom := poll.fromBlock

			// Poll the next page of blocks, if there are any we haven't polled yet.
			// Note if the scan ceiling holds us below the head, caughtUpToHead stays true:
			// we wait a poll interval for the view to extend, we don't spin.
			toBlock := steadyStateScanCeiling(lightMode, chainHead, stableHead, headChain)
			if maxToBlock := scanFrom + es.c.getCatchupPageSize() - 1; toBlock > maxToBlock {
				toBlock = maxToBlock
				caughtUpToHead = false // page again immediately, rather than waiting the polling interval
			}
			if toBlock >= scanFrom {
				ethLogs, err := es.getBlockRangeLogs(es.ctx, ag, scanFrom, toBlock)
				if err != nil {
					if es.rangeQueryFailed(es.ctx, scanFrom, toBlock, chainHead, toBlock > stableHead, err) {
						// An expected rejection of a page in the unstable window, from a node behind the
						// head we observed. Retry after the polling interval, with no failure backoff -
						// but never spin (a capped page cleared caughtUpToHead above)
						if es.waitPollingInterval() {
							return true
						}
						continue
					}
					failCount++
					continue
				}
				if lightMode {
					// Drop logs from block-versions already delivered on a previous sweep
					ethLogs = poll.filterDelivered(ethLogs)
				}
				events, err := es.filterEnrichSort(es.ctx, ag, ethLogs)
				if err != nil {
					log.L(es.ctx).Errorf("Failed to filter/enrich events fromBlock=%d toBlock=%d headBlock=%d: %s", scanFrom, toBlock, chainHead, err)
					failCount++
					continue
				}

				// High water mark for the restart checkpoint is min(scan position, stable threshold):
				// the scan runs to the head, but blocks in the unstable window can still change and the
				// re-org repair state (recorded hashes / delivered blocks) is in-memory only, so the
				// checkpoint holds at the horizon and a restart re-scans the window (redelivery is
				// de-duplicated downstream). A light mode head can still move backwards for a chain
				// reset, so there we additionally never move the committed base backwards.
				hwmBlock := toBlock + 1
				if stableHead < hwmBlock {
					hwmBlock = stableHead
				}
				if lightMode && hwmBlock < poll.fromBlock {
					hwmBlock = poll.fromBlock
				}

				// Dispatch the events
				if es.dispatchSetHWMCheckExit(ag, events, hwmBlock) {
					log.L(es.ctx).Debugf("Stream loop exiting")
					return true
				}

				// Update the head block to be the hwm block - a full-mode re-org rewind can
				// otherwise pull this backward, briefly stalling any other listener whose
				// individual catchup had already advanced past the old value (see catchupCeiling)
				es.storeHeadBlockForward(hwmBlock)

				if lightMode {
					// Record the block-versions we just delivered, and advance the committed window base
					poll.advanceRescan(ethLogs, hwmBlock)
				} else {
					// Advance our poll position, recording the hashes of the blocks we polled so we
					// can detect a re-org behind us on a later cycle
					poll.advance(headChain, toBlock)
				}
			}
		}

		// Reset failure count if we reach here
		failCount = 0

		// Sleep for the polling interval, unless we are paging through a backlog
		if caughtUpToHead && es.waitPollingInterval() {
			return true
		}
	}
}

// waitPollingInterval sleeps for the event polling interval, returning true if the stream stopped
func (es *eventStream) waitPollingInterval() bool {
	select {
	case <-time.After(es.c.eventFilterPollingInterval):
		return false
	case <-es.ctx.Done():
		log.L(es.ctx).Debugf("Stream loop stopping")
		return true
	}
}

// rangeQueryFailed handles a failed eth_getLogs range query from any of the polling loops. It returns
// true when the failure is an expected light chain tracking mode rejection that the caller should
// simply retry after the polling interval, and false when the caller should apply its failure backoff.
//
// In light mode a successful response is complete for the whole range, because the node rejects a
// range beyond its own head (the requirement stated on ffcapi.ChainTrackingModeLight). A rejected
// page above the stable threshold (checkpointBlockGap behind the highest head we have observed) is
// expected: a load-balanced node behind that head. A failure of a page at/below the threshold means
// a node is more than checkpointBlockGap behind - outside the drift the mode tolerates, and a
// deployment problem to surface - or the node is failing outright.
func (es *eventStream) rangeQueryFailed(ctx context.Context, fromBlock, toBlock, chainHead int64, aboveStableHead bool, err error) bool {
	if es.c.chainTrackingMode != ffcapi.ChainTrackingModeLight {
		log.L(ctx).Errorf("Failed to query block range fromBlock=%d toBlock=%d headBlock=%d: %s", fromBlock, toBlock, chainHead, err)
		return false
	}
	if aboveStableHead {
		log.L(ctx).Infof("Query of block range in the unstable window rejected (a node behind the observed head) - retrying after the polling interval fromBlock=%d toBlock=%d headBlock=%d: %s", fromBlock, toBlock, chainHead, err)
		es.c.blockListener.IncLightModeRangeAhead()
		return true
	}
	log.L(ctx).Warnf("Query of stable block range failed - a node is more than checkpointBlockGap=%d behind the observed head, or failing fromBlock=%d toBlock=%d headBlock=%d: %s", es.c.checkpointBlockGap, fromBlock, toBlock, chainHead, err)
	es.c.blockListener.IncLightModeDriftViolation()
	return false
}
